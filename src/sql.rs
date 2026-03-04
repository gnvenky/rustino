use regex::Regex;
use anyhow::{Result, bail};

#[derive(Debug)]
pub struct Query {
    pub table: String,
    pub columns: Vec<String>, // "*" for all
    pub limit: Option<usize>,
    pub filter: Option<(String, String, String)>, // column, op, value
}

impl Query {
    pub fn parse(sql: &str) -> Result<Self> {
        // Basic regex for SELECT ... FROM ... [WHERE ...] [LIMIT ...]
        let re = Regex::new(
            r"(?i)^SELECT\s+(.+?)\s+FROM\s+(\S+)(?:\s+WHERE\s+(.+?))?(?:\s+LIMIT\s+(\d+))?$"
        ).unwrap();

        if let Some(caps) = re.captures(sql) {
            let cols_str = caps.get(1).unwrap().as_str();
            let columns = if cols_str.trim() == "*" {
                vec!["*".to_string()]
            } else {
                cols_str.split(',').map(|s| s.trim().to_string()).collect()
            };

            let table = caps.get(2).unwrap().as_str().to_string();

            let filter = caps.get(3).map(|f| {
                // Parse simple <column> <op> <value>
                let re_op = Regex::new(r"(?i)^(\S+)\s*(=|!=|<|<=|>|>=)\s*(.+)$").unwrap();
                let f_str = f.as_str().trim();
                if let Some(c) = re_op.captures(f_str) {
                    (c[1].to_string(), c[2].to_string(), c[3].to_string())
                } else {
                    ("".to_string(), "".to_string(), "".to_string())
                }
            });

            let limit = caps.get(4).map(|m| m.as_str().parse::<usize>().unwrap());

            Ok(Query { table, columns, limit, filter })
        } else {
            bail!("Query does not match minimal SQL grammar");
        }
    }
}