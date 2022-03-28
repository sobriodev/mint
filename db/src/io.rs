//! Database filesystem abstraction layer.
//!
//! The internal structure of a database is hidden to an end-user. Thereby this module acts as
//! a middleware between database instance and OS filesystem.

use crate::error::{CustomKind, Error, Result};
use std::ffi::OsStr;
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};

/// A structure representing a filesystem abstraction layer.
#[derive(Debug)]
pub struct Io {
    #[allow(unused)]
    path: PathBuf,
}

impl Io {
    const METADATA_DIR: &'static str = ".metadata";
    const METADATA_FILE: &'static str = "metadata.json";

    #[inline]
    fn is_name_valid(filename: &str) -> bool {
        // Only alphanumeric characters + underscore supported at the moment
        !filename.is_empty() && filename.chars().all(|ch| ch.is_alphanumeric() || ch == '_')
    }

    /// Create a database filesystem structure.
    ///
    /// This function is typically called on a database creation.
    /// It initializes the filesystem before any further operation on a database can be performed.
    ///
    /// Currently `name` has restrictions and can only contain
    /// alphanumeric characters. An exception to this rule is underscore character.
    ///
    /// # Errors
    /// The function may return either an OS specific error in case system call has failed
    /// or a custom library error.
    pub fn new<P>(name: &str, path: P) -> Result<Self>
    where
        P: AsRef<OsStr>,
    {
        if !Self::is_name_valid(name) {
            return Err(Error::custom_err(
                CustomKind::InvalidArgument,
                "Database name contains forbidden characters",
            ));
        }

        // Create an empty metadata structure inside a specified directory
        let database_path = Path::new(&path).canonicalize()?.join(name);
        let metadata_path = database_path.join(Self::METADATA_DIR);
        fs::create_dir_all(&metadata_path)?;
        File::create(metadata_path.join(Self::METADATA_FILE))?;

        Ok(Self {
            path: database_path,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;

    const TEST_DATABASE_NAME: &'static str = "DB_UT";

    // Create database test directory
    fn create_temp_dir() -> TempDir {
        TempDir::new("").unwrap()
    }

    // Remove database test directory on test teardown.
    // It is done explicitly to catch possible errors
    fn remove_temp_dir(dir: TempDir) {
        dir.close().unwrap();
    }

    #[test]
    fn invalid_database_name_is_caught() {
        assert!(!Io::is_name_valid(""));
        assert!(!Io::is_name_valid("@"));
        assert!(!Io::is_name_valid("SomeName."));
        assert!(!Io::is_name_valid("<123+45>"));
        assert!(!Io::is_name_valid("&!@Name12"));
    }

    #[test]
    fn valid_database_name_does_not_pose_problems() {
        assert!(Io::is_name_valid("db"));
        assert!(Io::is_name_valid("ThisIsADatabase"));
        assert!(Io::is_name_valid("db_new"));
        assert!(Io::is_name_valid("_2022_database"));
        assert!(Io::is_name_valid("SomeDatabase_2022_backup"));
        assert!(Io::is_name_valid(TEST_DATABASE_NAME));
    }

    #[test]
    fn invalid_database_name_produces_error() {
        let temp_dir = create_temp_dir();

        let io = Io::new("!!InvalidName!!", temp_dir.path());
        let err = io.unwrap_err();
        assert_eq!(CustomKind::InvalidArgument, *err.get_custom_kind().unwrap());

        remove_temp_dir(temp_dir);
    }

    #[test]
    fn returned_database_path_is_absolute() {
        let temp_dir = create_temp_dir();

        let io = Io::new(TEST_DATABASE_NAME, temp_dir.path()).unwrap();
        assert!(io.path.is_absolute());

        remove_temp_dir(temp_dir);
    }

    #[test]
    fn expected_directory_structure_exists() {
        let temp_dir = create_temp_dir();

        let _ = Io::new(TEST_DATABASE_NAME, temp_dir.path()).unwrap();
        let database_dir = temp_dir.path().join(TEST_DATABASE_NAME);
        let metadata_dir = database_dir.join(Io::METADATA_DIR);
        let metadata_file = metadata_dir.join(Io::METADATA_FILE);
        assert!(database_dir.is_dir());
        assert!(metadata_dir.is_dir());
        assert!(metadata_file.is_file());
        // Metadata file shall exists and be empty at this point
        assert_eq!(
            0,
            File::open(metadata_file).unwrap().metadata().unwrap().len()
        );

        remove_temp_dir(temp_dir);
    }
}