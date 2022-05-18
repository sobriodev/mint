//! Database metadata utilities.
//!
//! Metadata keeps all crucial information required to load, store and manipulate database
//! collections as well as the database itself.

use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};

/// A structure representing metadata of a database.
#[non_exhaustive]
#[derive(Serialize, Deserialize, Debug)]
pub struct Database {
    /// Name of a database.
    pub name: String,
    /// Database creation date.
    pub created: DateTime<Local>,
    /// Database last modification date.
    pub modified: DateTime<Local>,
}

impl Database {
    /// Return a database metadata structure, preinitialized for further processing.
    ///
    /// By default all fields are fully initialized with default values except for database's name
    /// which is set according to `name` parameter.
    #[must_use]
    pub fn new(name: &str) -> Self {
        let now = Local::now();
        Self {
            name: name.to_string(),
            created: now,
            modified: now,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn by_default_database_creation_date_equals_modification_date() {
        let database = Database::new("Database");
        assert_eq!(database.created, database.modified);
    }
}
