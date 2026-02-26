pub struct Snapshot {
    pub id: u64,
    pub timestamp: i64,
    pub manifest_list: Vec<String>,
}

pub struct Table {
    pub name: String,
    pub location: String,
    pub current_snapshot: Option<Snapshot>,
}

impl Table {
    pub fn new(name: &str, location: &str) -> Self {
        Table {
            name: name.to_string(),
            location: location.to_string(),
            current_snapshot: None,
        }
    }

    pub fn add_snapshot(&mut self, snapshot: Snapshot) {
        self.current_snapshot = Some(snapshot);
    }
}
