use anyhow::{bail, Result};

/// Very small SQL grammar:
///
/// ```text
/// query  := SELECT columns FROM ident [WHERE cond] [LIMIT number]
/// columns:= "*" | ident ("," ident)*
/// cond   := ident op value
/// op     := "=" | "!=" | "<" | "<=" | ">" | ">="
/// value  := number | string | ident
/// ```
#[derive(Debug)]
pub struct Query {
    pub table: String,
    pub columns: Vec<String>, // "*" for all
    pub limit: Option<usize>,
    pub filter: Option<(String, String, String)>, // column, op, value
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    Number(usize),
    StringLit(String),
    Star,
    Comma,
    Op(String),
    Eof,
}

struct Lexer<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> Lexer<'a> {
    fn new(s: &'a str) -> Self {
        Self {
            input: s.as_bytes(),
            pos: 0,
        }
    }

    fn next_byte(&self) -> Option<u8> {
        self.input.get(self.pos).copied()
    }

    fn bump(&mut self) {
        self.pos += 1;
    }

    fn skip_ws(&mut self) {
        while let Some(b) = self.next_byte() {
            if (b as char).is_whitespace() {
                self.bump();
            } else {
                break;
            }
        }
    }

    fn next_token(&mut self) -> Result<Token> {
        self.skip_ws();
        let b = match self.next_byte() {
            Some(b) => b,
            None => return Ok(Token::Eof),
        };
        let ch = b as char;

        // Ident or keyword or bare value
        if ch.is_ascii_alphabetic() || ch == '_' {
            let start = self.pos;
            self.bump();
            while let Some(nb) = self.next_byte() {
                let c = nb as char;
                if c.is_ascii_alphanumeric() || c == '_' || c == '.' {
                    self.bump();
                } else {
                    break;
                }
            }
            let s = std::str::from_utf8(&self.input[start..self.pos]).unwrap().to_string();
            return Ok(Token::Ident(s));
        }

        // Number
        if ch.is_ascii_digit() {
            let start = self.pos;
            self.bump();
            while let Some(nb) = self.next_byte() {
                let c = nb as char;
                if c.is_ascii_digit() {
                    self.bump();
                } else {
                    break;
                }
            }
            let s = std::str::from_utf8(&self.input[start..self.pos]).unwrap();
            let n: usize = s.parse()?;
            return Ok(Token::Number(n));
        }

        match ch {
            '*' => {
                self.bump();
                Ok(Token::Star)
            }
            ',' => {
                self.bump();
                Ok(Token::Comma)
            }
            // Simple string literal: '...'
            '\'' => {
                self.bump(); // skip opening quote
                let start = self.pos;
                while let Some(nb) = self.next_byte() {
                    self.bump();
                    if nb as char == '\'' {
                        let s =
                            std::str::from_utf8(&self.input[start..self.pos - 1]).unwrap().to_string();
                        return Ok(Token::StringLit(s));
                    }
                }
                bail!("Unterminated string literal");
            }
            // Operators: =, !=, <, <=, >, >=
            '=' | '!' | '<' | '>' => {
                let first = ch;
                self.bump();
                let mut op = first.to_string();
                if let Some(nb) = self.next_byte() {
                    let c = nb as char;
                    if c == '=' {
                        op.push('=');
                        self.bump();
                    }
                }
                Ok(Token::Op(op))
            }
            _ => bail!("Unexpected character in SQL: '{}'", ch),
        }
    }
}

struct Parser<'a> {
    lexer: Lexer<'a>,
    lookahead: Token,
}

impl<'a> Parser<'a> {
    fn new(sql: &'a str) -> Result<Self> {
        let mut lexer = Lexer::new(sql);
        let first = lexer.next_token()?;
        Ok(Self {
            lexer,
            lookahead: first,
        })
    }

    fn bump(&mut self) -> Result<()> {
        self.lookahead = self.lexer.next_token()?;
        Ok(())
    }

    fn expect_keyword(&mut self, kw: &str) -> Result<()> {
        if let Token::Ident(ref s) = self.lookahead {
            if s.eq_ignore_ascii_case(kw) {
                self.bump()?;
                return Ok(());
            }
        }
        bail!("Expected keyword '{}', found {:?}", kw, self.lookahead);
    }

    fn expect_ident(&mut self) -> Result<String> {
        if let Token::Ident(s) = std::mem::replace(&mut self.lookahead, Token::Eof) {
            self.lookahead = self.lexer.next_token()?;
            Ok(s)
        } else {
            bail!("Expected identifier, found {:?}", self.lookahead);
        }
    }

    fn parse_columns(&mut self) -> Result<Vec<String>> {
        match self.lookahead {
            Token::Star => {
                self.bump()?;
                Ok(vec!["*".to_string()])
            }
            _ => {
                let mut cols = Vec::new();
                cols.push(self.expect_ident()?);
                while let Token::Comma = self.lookahead {
                    self.bump()?; // consume comma
                    cols.push(self.expect_ident()?);
                }
                Ok(cols)
            }
        }
    }

    fn parse_filter(&mut self) -> Result<(String, String, String)> {
        // column
        let column = self.expect_ident()?;

        // operator
        let op = match std::mem::replace(&mut self.lookahead, Token::Eof) {
            Token::Op(s) => {
                self.lookahead = self.lexer.next_token()?;
                s
            }
            other => bail!("Expected operator, found {:?}", other),
        };

        // value: number, string, or ident
        let value = match std::mem::replace(&mut self.lookahead, Token::Eof) {
            Token::Number(n) => {
                self.lookahead = self.lexer.next_token()?;
                n.to_string()
            }
            Token::StringLit(s) => {
                self.lookahead = self.lexer.next_token()?;
                s
            }
            Token::Ident(s) => {
                self.lookahead = self.lexer.next_token()?;
                s
            }
            other => bail!("Expected literal value, found {:?}", other),
        };

        Ok((column, op, value))
    }

    fn parse_query(&mut self) -> Result<Query> {
        // SELECT
        self.expect_keyword("SELECT")?;
        let columns = self.parse_columns()?;

        // FROM <table>
        self.expect_keyword("FROM")?;
        let table = self.expect_ident()?;

        // Optional WHERE
        let mut filter = None;
        if let Token::Ident(ref s) = self.lookahead {
            if s.eq_ignore_ascii_case("WHERE") {
                self.bump()?;
                filter = Some(self.parse_filter()?);
            }
        }

        // Optional LIMIT
        let mut limit = None;
        if let Token::Ident(ref s) = self.lookahead {
            if s.eq_ignore_ascii_case("LIMIT") {
                self.bump()?;
                if let Token::Number(n) = std::mem::replace(&mut self.lookahead, Token::Eof) {
                    self.lookahead = self.lexer.next_token()?;
                    limit = Some(n);
                } else {
                    bail!("Expected numeric LIMIT value, found {:?}", self.lookahead);
                }
            }
        }

        // No trailing junk
        if self.lookahead != Token::Eof {
            bail!("Unexpected tokens after end of query: {:?}", self.lookahead);
        }

        Ok(Query {
            table,
            columns,
            limit,
            filter,
        })
    }
}

impl Query {
    pub fn parse(sql: &str) -> Result<Self> {
        let mut parser = Parser::new(sql)?;
        parser.parse_query()
    }
}
