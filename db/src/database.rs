//! JSON database abstraction layer.
//!
//! Database consists of several `collections` - each one is an independent JSON notation based data
//! container (counterpart of a table in SQL based DBs) with an unique file inside the database
//! directory, synchronized whenever internal collection's structure has been altered.
//!
//! Database layer handles all I/O operations (loading and storing collections back to the
//! filesystem) as well as provides methods to manipulate existing collections and create new ones.
//!
//! On top of the database works a `Query Manager` which allows existing collections to be queried
//! to pass data to appropriate endpoints.

use crate::error::Result;
#[double]
use crate::io::Io;
use crate::metadata::Database as DbMeta;
use mockall_double::double;
use std::ffi::OsStr;

/// A structure representing a database.
#[non_exhaustive]
pub struct Database {
    #[allow(unused)] //TODO remove when the field is actually used
    io: Io,
    #[allow(unused)] //TODO remove when the field is actually used
    metadata: DbMeta,
}

impl Database {
    /// Create an empty database.
    ///
    /// The function creates and initializes database's internal structure prior to its first usage.
    /// After calling the function the database is ready to use.
    ///
    /// # Errors
    /// The function may produce an error in case I/O system call has failed or database
    /// could not be initialized due to internal error.
    pub fn create<P>(name: &str, path: P) -> Result<Self>
    where
        P: AsRef<OsStr> + 'static,
    {
        let metadata = DbMeta::new(name);
        let io = Io::create(path, &metadata)?;
        Ok(Self { io, metadata })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use more_asserts::*;

    #[test]
    fn database_is_successfully_created() {
        let name = "TestDatabase";
        let path = "/path/to/database";

        // Set up mock on Io::create which checks whether function arguments are valid
        let ctx = Io::create_context();
        ctx.expect()
            .times(1)
            .withf(move |path_arg: &&str, metadata_arg: &DbMeta| {
                *path_arg == path && metadata_arg.name == name
            })
            .returning(|_path: &str, _metadata: &DbMeta| Ok(Io::new()));

        let database = Database::create(name, path).unwrap();

        // Check whether internal metadata is filled with correct values
        let now = Utc::now();
        assert_eq!(name, database.metadata.name);
        assert_gt!(now, database.metadata.created);
        assert_eq!(database.metadata.created, database.metadata.modified);
    }
}
