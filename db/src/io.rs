//! Database filesystem abstraction layer.
//!
//! The internal structure of a database is hidden to an end-user. Thereby this module acts as
//! a middleware between database instance and OS filesystem.

#![cfg_attr(test, allow(dead_code))]
#[cfg(test)]
use mockall::automock;

use crate::error::{CustomKind, Error, Result};
use serde::Serialize;
use std::ffi::OsStr;
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};

/// A structure representing a filesystem abstraction layer.
#[non_exhaustive]
#[derive(Debug)]
pub struct Io {
    // TODO remove this label when the field is used internally
    #[allow(unused)]
    path: PathBuf,
}

#[cfg_attr(test, automock)]
impl Io {
    const METADATA_DIR: &'static str = ".metadata";
    const METADATA_FILE: &'static str = "metadata.json";

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
    pub fn create<P>(name: &str, path: P) -> Result<Self>
    where
        P: AsRef<OsStr> + 'static,
    {
        if !Self::is_name_valid(name) {
            return Err(Error::custom_err(
                CustomKind::InvalidArgument,
                "Database name contains forbidden characters",
            ));
        }
        // Create an empty metadata structure inside a specified directory
        let database_path = Path::new(&path).canonicalize()?.join(name);
        if database_path.exists() {
            return Err(Error::custom_err(
                CustomKind::DbIo,
                "Directory already exists",
            ));
        }

        let metadata_path = database_path.join(Self::METADATA_DIR);
        fs::create_dir_all(&metadata_path)?;
        File::create(metadata_path.join(Self::METADATA_FILE))?;

        Ok(Self {
            path: database_path,
        })
    }

    /// Open an existing database filesystem structure.
    ///
    /// This function may be called only after a specified database has been already created.
    ///
    /// # Errors
    /// The function may return a custom library error in case a database specified by `path`
    /// does not exists or has corrupted internal structure.
    pub fn open<P>(path: P) -> Result<Self>
    where
        P: AsRef<OsStr> + 'static,
    {
        let canonicalized_path_res = Path::new(&path).canonicalize();
        // Path::canonicalize returns an error in case specified directory does not exist.
        // Capture any IO error and generate custom one instead
        if canonicalized_path_res.is_err() {
            return Err(Error::custom_err(
                CustomKind::DbIo,
                "Database does not exists",
            ));
        }
        let canonicalized_path = canonicalized_path_res?;

        // Overall check if database's filesystem structure is valid
        let metadata_path = canonicalized_path.join(Self::METADATA_DIR);
        let metadata_file = metadata_path.join(Self::METADATA_FILE);
        if !metadata_path.exists() || !metadata_file.exists() || !metadata_file.is_file() {
            return Err(Error::custom_err(
                CustomKind::DbIo,
                "Corrupted database filesystem structure",
            ));
        }

        Ok(Self {
            path: canonicalized_path,
        })
    }

    // Open a file creating it optionally if needed
    fn open_file<P>(&self, path: P, create: bool) -> std::io::Result<File>
    where
        P: AsRef<Path> + 'static,
    {
        let file_path = self.path.join(&path);

        // Create directory structure in case file creation has been requested
        if create {
            // Obtain directories leading to the file. At least one parent directory is always
            // expected since the path is relative to a database's base directory
            let dirs = Path::new(&file_path).parent().unwrap();
            fs::create_dir_all(dirs)?;
        }

        fs::OpenOptions::new()
            .create_new(create)
            .write(true)
            .truncate(true)
            .open(file_path)
    }

    // Serialize a serializable object into a file
    fn do_serialize<S>(object: &S, file: File, pretty: bool) -> Result<()>
    where
        S: Serialize + 'static,
    {
        if pretty {
            serde_json::to_writer_pretty(file, &object)?;
        } else {
            serde_json::to_writer(file, &object)?;
        }

        Ok(())
    }

    /// Serialize an object into a file truncating old content.
    ///
    /// The path is relative to a database's base path and has to end with a file which has been
    /// created prior to call to this function. If an output file has not been created yet, then
    /// [`Io::serialize_new`] should be used.
    ///
    /// Depending on `pretty` flag the output may be a pretty JSON which retain formatting, thus
    /// providing better readability but the output file may be significantly larger.
    ///
    /// # Errors
    /// The function may return an IO, serde or a custom library error.
    pub fn serialize<S, P>(&self, object: &S, path: P, pretty: bool) -> Result<()>
    where
        S: Serialize + 'static,
        P: AsRef<Path> + 'static,
    {
        let file = self.open_file(path, false)?;
        Self::do_serialize(object, file, pretty)
    }

    /// Serialize an object into a new file.
    ///
    /// The function is basically the same as [`Io::serialize`] but it creates a new file in the
    /// filesystem rather than reusing an existing one. It fails when the file already exists.
    ///
    /// # Errors
    /// The function may return an IO, serde or a custom library error.
    pub fn serialize_new<S, P>(&self, object: &S, path: P, pretty: bool) -> Result<()>
    where
        S: Serialize + 'static,
        P: AsRef<Path> + 'static,
    {
        let file = self.open_file(path, true)?;
        Self::do_serialize(object, file, pretty)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use more_asserts::*;
    use rstest::*;
    use tempdir::TempDir;

    /* ----------------- */
    /* ---- Helpers ---- */
    /* ----------------- */

    type IoInstanceFixture = (Io, TempDir);
    const TEST_DATABASE_NAME: &'static str = "DB_UT";

    fn database_dir(dir: &TempDir) -> PathBuf {
        dir.path().join(TEST_DATABASE_NAME)
    }

    // Remove temporary directory manually to catch possible errors
    fn remove_temp_dir(dir: TempDir) {
        dir.close().unwrap();
    }

    fn test_database_metadata_file_path(dir: &TempDir) -> PathBuf {
        dir.path()
            .join(TEST_DATABASE_NAME)
            .join(Io::METADATA_DIR)
            .join(Io::METADATA_FILE)
    }

    fn file_len(path: &Path) -> u64 {
        File::open(path).unwrap().metadata().unwrap().len()
    }

    /* ------------------ */
    /* ---- Fixtures ---- */
    /* ------------------ */

    // Return temporary directory that will be removed automatically afterwards
    #[fixture]
    fn temp_dir() -> TempDir {
        TempDir::new("").unwrap()
    }

    // Create database IO instance
    #[fixture]
    fn io_created() -> IoInstanceFixture {
        let temp_dir = temp_dir();
        let io = Io::create(TEST_DATABASE_NAME, temp_dir.path().to_path_buf()).unwrap();
        (io, temp_dir)
    }

    // Create and open database IO instance
    #[fixture]
    fn io_opened() -> IoInstanceFixture {
        let (_, temp_dir) = io_created();
        let io = Io::open(temp_dir.path().join(TEST_DATABASE_NAME)).unwrap();
        (io, temp_dir)
    }

    /* -------------------------- */
    /* ---- Test definitions ---- */
    /* -------------------------- */

    #[rstest]
    #[case("")]
    #[case("@")]
    #[case("SomeName.")]
    #[case("<123+45>")]
    #[case("&!@Name12")]
    fn invalid_database_name_is_caught(#[case] name: &str) {
        assert!(!Io::is_name_valid(name));
    }

    #[rstest]
    #[case("db")]
    #[case("ThisIsADatabase")]
    #[case("db_new")]
    #[case("_2022_database")]
    #[case("SomeDatabase_2022_backup")]
    fn valid_database_name_does_not_pose_problems(#[case] name: &str) {
        assert!(Io::is_name_valid(name));
    }

    #[rstest]
    fn invalid_database_name_produces_error(temp_dir: TempDir) {
        let io = Io::create("!!InvalidName!!", temp_dir.path().to_path_buf());
        let err = io.unwrap_err();
        assert_eq!(CustomKind::InvalidArgument, *err.get_custom_kind().unwrap());

        remove_temp_dir(temp_dir);
    }

    #[rstest]
    fn existing_database_throws_error_when_creating_another_one_in_the_same_dir(
        io_created: IoInstanceFixture,
    ) {
        let (_io, temp_dir) = io_created;

        let result = Io::create(TEST_DATABASE_NAME, temp_dir.path().to_path_buf());
        let err = result.unwrap_err();
        assert_eq!(CustomKind::DbIo, *err.get_custom_kind().unwrap());

        remove_temp_dir(temp_dir);
    }

    #[rstest]
    fn returned_database_path_is_absolute_after_database_creation(io_created: IoInstanceFixture) {
        let (io, temp_dir) = io_created;
        assert!(io.path.is_absolute());

        remove_temp_dir(temp_dir);
    }

    #[rstest]
    fn expected_directory_structure_exists_after_database_creation(io_created: IoInstanceFixture) {
        let (_io, temp_dir) = io_created;

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

    #[rstest]
    fn invalid_path_generates_error_when_opening_database(temp_dir: TempDir) {
        // Temporary directory is empty at this point.
        // Append invalid trailing directory to the path and see if it produces and error
        let io = Io::open(temp_dir.path().join("InvalidDirectory"));
        let err = io.unwrap_err();
        assert_eq!(CustomKind::DbIo, *err.get_custom_kind().unwrap());

        remove_temp_dir(temp_dir);
    }

    #[rstest]
    fn missing_metadata_dir_produces_error_when_opening_database(temp_dir: TempDir) {
        // At this point temporary directory exists but contains nothing inside
        let io = Io::open(temp_dir.path().to_path_buf());
        let err = io.unwrap_err();
        assert_eq!(CustomKind::DbIo, *err.get_custom_kind().unwrap());

        remove_temp_dir(temp_dir);
    }

    #[rstest]
    fn missing_metadata_file_produces_error_when_opening_database(temp_dir: TempDir) {
        // Build partial database structure by creating metadata directory only
        fs::create_dir(temp_dir.path().join(Io::METADATA_DIR)).unwrap();

        let io = Io::open(temp_dir.path().to_path_buf());
        let err = io.unwrap_err();
        assert_eq!(CustomKind::DbIo, *err.get_custom_kind().unwrap());

        remove_temp_dir(temp_dir);
    }

    #[rstest]
    fn valid_directory_returns_io_instance_when_opening_database(io_opened: IoInstanceFixture) {
        let (io, temp_dir) = io_opened;
        // Internal path should always be absolute
        assert!(io.path.is_absolute());

        remove_temp_dir(temp_dir);
    }

    #[rstest]
    #[case::non_existing_file("/wrong/file.path")]
    #[case::absolute_path(Path::new("/").join(Io::METADATA_DIR).join(Io::METADATA_FILE))]
    #[case::path_to_directory(Path::new(Io::METADATA_DIR).to_path_buf())]
    #[case::empty_path("")]
    fn wrong_path_throws_error_when_serializing(
        #[case] path: PathBuf,
        io_opened: IoInstanceFixture,
    ) {
        let (io, temp_dir) = io_opened;
        let object = 0;

        let result = io.serialize(&object, path, true);
        let err = result.unwrap_err();
        // Expect IO error
        assert!(!err.is_custom());

        remove_temp_dir(temp_dir);
    }

    #[rstest]
    fn input_object_is_serialized(io_opened: IoInstanceFixture) {
        let (io, temp_dir) = io_opened;
        let metadata_file_path = test_database_metadata_file_path(&temp_dir);

        #[derive(Serialize, Default)]
        struct Object {
            field1: i32,
            field2: u32,
            field3: f64,
        }
        let object = Object::default();

        assert_eq!(0, file_len(&metadata_file_path));
        io.serialize(&object, metadata_file_path.clone(), true)
            .unwrap();
        // Metadata file shall have content updated
        assert_gt!(file_len(&metadata_file_path), 0);

        remove_temp_dir(temp_dir);
    }

    #[rstest]
    fn existing_file_throws_error_when_serializing_new(io_opened: IoInstanceFixture) {
        let (io, temp_dir) = io_opened;
        let path = test_database_metadata_file_path(&temp_dir);
        let object = 0;

        let result = io.serialize_new(&object, path, true);
        let err = result.unwrap_err();
        // Expect IO error
        assert!(!err.is_custom());

        remove_temp_dir(temp_dir);
    }

    #[rstest]
    #[case::absolute_path(Path::new("/").join(Io::METADATA_DIR).join(Io::METADATA_FILE))]
    #[case::path_to_directory(Path::new(Io::METADATA_DIR).to_path_buf())]
    #[case::empty_path(Path::new("").to_path_buf())]
    fn wrong_path_throws_error_when_serializing_new(
        #[case] path: PathBuf,
        io_opened: IoInstanceFixture,
    ) {
        let (io, temp_dir) = io_opened;
        let object = 0;

        let result = io.serialize(&object, path, true);
        let err = result.unwrap_err();
        // Expect IO error
        assert!(!err.is_custom());

        remove_temp_dir(temp_dir);
    }

    #[rstest]
    #[case::base_dir("serialized.json")]
    #[case::sub_dir("sub/serialized.json")]
    #[case::sub_sub_dir("sub/sub/serialized.json")]
    fn object_can_be_serialized_into_new_file(#[case] path: PathBuf, io_opened: IoInstanceFixture) {
        let (io, temp_dir) = io_opened;
        let full_path = database_dir(&temp_dir).join(&path);

        #[derive(Serialize, Default)]
        struct Object {
            field1: i32,
            field2: f32,
        }
        let object = Object::default();

        assert!(!full_path.exists());
        io.serialize_new(&object, path.clone(), true).unwrap();
        assert!(full_path.exists());
        assert_gt!(file_len(&full_path), 0);

        remove_temp_dir(temp_dir);
    }

    #[rstest]
    #[case::base_dir("serialized.json")]
    #[case::sub_dir("sub/serialized.json")]
    #[case::sub_sub_dir("sub/sub/serialized.json")]
    fn serialized_file_may_be_overwritten(#[case] path: PathBuf, io_opened: IoInstanceFixture) {
        let (io, temp_dir) = io_opened;
        let full_path = database_dir(&temp_dir).join(&path);
        let old_object: u8 = 100;
        let new_object: f64 = 12345678.12345678;

        // Serialize new object at first
        assert!(!full_path.exists());
        io.serialize_new(&old_object, path.clone(), true).unwrap();
        assert!(full_path.exists());
        let old_file_len = file_len(&full_path);
        assert_gt!(old_file_len, 0);

        // Overwrite the same object file
        io.serialize(&new_object, path.clone(), true).unwrap();
        assert_gt!(file_len(&full_path), old_file_len);

        remove_temp_dir(temp_dir);
    }
}
