//! Database filesystem abstraction layer.
//!
//! The internal structure of a database is hidden to an end-user. Thereby this module acts as
//! a middleware between database instance and OS filesystem.

#![cfg_attr(test, allow(dead_code))]
#[cfg(test)]
use mockall::automock;

use crate::error::{CustomKind, Error, Result};
use crate::metadata::Database as DbMeta;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::ffi::OsStr;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

/// A structure representing a filesystem abstraction layer.
#[non_exhaustive]
#[derive(Debug)]
pub struct Io {
    path: PathBuf,
}

// Possible file open modes when dealing with files
#[derive(Copy, Clone)]
enum FileOpenMode {
    Open,
    Write,
    WriteCreate,
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
    /// Currently metadata's `name` has restrictions and can only contain
    /// alphanumeric characters. An exception to this rule is underscore character.
    ///
    /// # Errors
    /// The function may return either an OS specific error in case system call has failed
    /// or a custom library error.
    pub fn create<P>(path: P, db_meta: &DbMeta) -> Result<Self>
    where
        P: AsRef<OsStr> + 'static,
    {
        if !Self::is_name_valid(&db_meta.name) {
            return Err(Error::custom_err(
                CustomKind::InvalidArgument,
                "Database name contains forbidden characters",
            ));
        }

        // Check if a directory already exist
        let database_path = Path::new(&path).canonicalize()?.join(&db_meta.name);
        if database_path.exists() {
            return Err(Error::custom_err(
                CustomKind::DbIo,
                "Directory already exists",
            ));
        }

        // Serialize metadata structure before returning IO object
        let metadata_file_path = database_path
            .join(Self::METADATA_DIR)
            .join(Self::METADATA_FILE);
        let io = Self {
            path: database_path,
        };
        io.serialize_new(db_meta, metadata_file_path, true)?;
        Ok(io)
    }

    /// Open an existing database filesystem structure.
    ///
    /// This function may be called only after a specified database has been already created.
    ///
    /// # Errors
    /// The function may return a custom library error in case a database specified by `path`
    /// does not exists or has corrupted internal structure.
    pub fn open<P>(path: P) -> Result<(Self, DbMeta)>
    where
        P: AsRef<OsStr> + 'static,
    {
        let canonicalized_path_res = Path::new(&path).canonicalize();
        // Path::canonicalize returns an error in case specified directory does not exist.
        // Capture any IO error and generate custom one instead
        if canonicalized_path_res.is_err() {
            return Err(Error::custom_err(
                CustomKind::DbIo,
                "Database does not exist",
            ));
        }
        let canonicalized_path = canonicalized_path_res?;

        let metadata_file_path = canonicalized_path
            .join(Self::METADATA_DIR)
            .join(Self::METADATA_FILE);
        let io = Self {
            path: canonicalized_path,
        };
        let metadata = io.deserialize(metadata_file_path)?;
        Ok((io, metadata))
    }

    // Open a file creating it optionally if needed
    fn open_file<P>(&self, path: P, mode: FileOpenMode) -> Result<File>
    where
        P: AsRef<Path> + 'static,
    {
        let file_path = self.path.join(&path);

        // Create directory structure in case file creation has been requested
        if matches!(mode, FileOpenMode::WriteCreate) {
            // Obtain directories leading to the file. At least one parent directory is always
            // expected since the path is relative to a database's base directory
            let dirs = Path::new(&file_path).parent().unwrap();
            fs::create_dir_all(dirs)?;
        }

        // Check if a path exists and is not a directory when open/write mode is selected.
        // This might be handled by `open` method below but the error would not contain
        // problematic path details. Debugging is much easier this way
        if !matches!(mode, FileOpenMode::WriteCreate) && (!file_path.exists() || file_path.is_dir())
        {
            return Err(Error::custom_err(
                CustomKind::InvalidArgument,
                &format!(
                    "Cannot serialize an object into invalid path: {}",
                    file_path.display()
                ),
            ));
        }

        // Set file options depending upon input mode
        let mut open_options = fs::OpenOptions::new();
        match mode {
            FileOpenMode::Open => {
                open_options.read(true);
            }
            FileOpenMode::Write => {
                open_options.write(true);
                open_options.truncate(true);
            }
            FileOpenMode::WriteCreate => {
                open_options.create_new(true);
                open_options.write(true);
            }
        }

        // Automatic result conversion cannot be handled - must be done manually
        let file_result = open_options.open(file_path);
        match file_result {
            Ok(file) => Ok(file),
            Err(err) => Err(Error::Io(err)),
        }
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
        let file = self.open_file(path, FileOpenMode::Write)?;
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
        let file = self.open_file(path, FileOpenMode::WriteCreate)?;
        Self::do_serialize(object, file, pretty)
    }

    /// Deserialize an object from an existing file.
    ///
    /// The file has to exists in the filesystem and contains a serialized instance of the same
    /// type. Either [`Io::serialize`] or [`Io::serialize_new`] is required prior to a call to this
    /// function.
    ///
    /// # Errors
    /// The function may return both custom library as well as IO and serde internal errors.
    pub fn deserialize<S, P>(&self, path: P) -> Result<S>
    where
        S: DeserializeOwned + 'static,
        P: AsRef<Path> + 'static,
    {
        let file = self.open_file(path, FileOpenMode::Open)?;
        let reader = BufReader::new(file);
        let object = serde_json::from_reader(reader)?;
        Ok(object)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use more_asserts::*;
    use rstest::*;
    use serde::Deserialize;
    use tempdir::TempDir;

    /* ----------------- */
    /* ---- Helpers ---- */
    /* ----------------- */

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

    type IoInstanceFixture = (Io, TempDir);

    // Return temporary directory that will be removed automatically afterwards
    #[fixture]
    fn temp_dir() -> TempDir {
        TempDir::new("").unwrap()
    }

    // Return a test database's metadata structure
    #[fixture]
    fn db_meta() -> DbMeta {
        DbMeta::new("DB_UT")
    }

    // Create database IO instance
    #[fixture]
    fn io_created() -> IoInstanceFixture {
        let temp_dir = temp_dir();
        let io = Io::create(temp_dir.path().to_path_buf(), &db_meta()).unwrap();
        (io, temp_dir)
    }

    // Create and open database IO instance
    #[fixture]
    fn io_opened() -> IoInstanceFixture {
        let (_, temp_dir) = io_created();
        let (io, _) = Io::open(temp_dir.path().join(TEST_DATABASE_NAME)).unwrap();
        (io, temp_dir)
    }

    #[derive(Serialize, Deserialize, Default, PartialEq, Debug)]
    struct Object {
        field1: i32,
        field2: f32,
    }

    // Return a serializable test object
    #[fixture]
    fn serializable_object() -> Object {
        Object::default()
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
        let metadata = DbMeta::new("!!InvalidName!!");
        let io = Io::create(temp_dir.path().to_path_buf(), &metadata);
        let err = io.unwrap_err();
        assert_eq!(CustomKind::InvalidArgument, *err.get_custom_kind().unwrap());

        remove_temp_dir(temp_dir);
    }

    #[rstest]
    fn existing_database_throws_error_when_creating_another_one_in_the_same_dir(
        io_created: IoInstanceFixture,
        db_meta: DbMeta,
    ) {
        let (_io, temp_dir) = io_created;

        let result = Io::create(temp_dir.path().to_path_buf(), &db_meta);
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
        // Metadata file shall exists and contain serialized structure
        assert_gt!(
            File::open(metadata_file).unwrap().metadata().unwrap().len(),
            0
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
        assert_eq!(CustomKind::InvalidArgument, *err.get_custom_kind().unwrap());

        remove_temp_dir(temp_dir);
    }

    #[rstest]
    fn missing_metadata_file_produces_error_when_opening_database(temp_dir: TempDir) {
        // Build partial database structure by creating metadata directory only
        fs::create_dir(temp_dir.path().join(Io::METADATA_DIR)).unwrap();

        let io = Io::open(temp_dir.path().to_path_buf());
        let err = io.unwrap_err();
        assert_eq!(CustomKind::InvalidArgument, *err.get_custom_kind().unwrap());

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
        serializable_object: Object,
    ) {
        let (io, temp_dir) = io_opened;

        let result = io.serialize(&serializable_object, path, true);
        let err = result.unwrap_err();
        assert_eq!(CustomKind::InvalidArgument, *err.get_custom_kind().unwrap());

        remove_temp_dir(temp_dir);
    }

    #[rstest]
    fn input_object_is_serialized(io_opened: IoInstanceFixture) {
        let (io, temp_dir) = io_opened;
        let metadata_file_path = test_database_metadata_file_path(&temp_dir);
        // Probably not the best metadata representation
        // but should be always represented by smaller number of bytes
        let new_object = 100;

        let len = file_len(&metadata_file_path);
        io.serialize(&new_object, metadata_file_path.clone(), true)
            .unwrap();
        // Metadata file shall have content updated
        assert_lt!(file_len(&metadata_file_path), len);

        remove_temp_dir(temp_dir);
    }

    #[rstest]
    fn existing_file_throws_error_when_serializing_new(
        io_opened: IoInstanceFixture,
        serializable_object: Object,
    ) {
        let (io, temp_dir) = io_opened;
        let path = test_database_metadata_file_path(&temp_dir);

        let result = io.serialize_new(&serializable_object, path, true);
        let err = result.unwrap_err();
        // Expect IO error
        assert!(!err.is_custom());

        remove_temp_dir(temp_dir);
    }

    #[rstest]
    #[case::absolute_path(Path::new("/").join(Io::METADATA_DIR).join(Io::METADATA_FILE))]
    #[case::path_to_directory(Path::new(Io::METADATA_DIR).to_path_buf())]
    #[case::empty_path("")]
    fn wrong_path_throws_error_when_serializing_new(
        #[case] path: PathBuf,
        io_opened: IoInstanceFixture,
        serializable_object: Object,
    ) {
        let (io, temp_dir) = io_opened;

        let result = io.serialize_new(&serializable_object, path, true);
        let err = result.unwrap_err();
        // Expect IO error
        assert!(!err.is_custom());

        remove_temp_dir(temp_dir);
    }

    #[rstest]
    #[case::base_dir("serialized.json")]
    #[case::sub_dir("sub/serialized.json")]
    #[case::sub_sub_dir("sub/sub/serialized.json")]
    fn object_can_be_serialized_into_new_file(
        #[case] path: PathBuf,
        io_opened: IoInstanceFixture,
        serializable_object: Object,
    ) {
        let (io, temp_dir) = io_opened;
        let full_path = database_dir(&temp_dir).join(&path);

        assert!(!full_path.exists());
        io.serialize_new(&serializable_object, path.clone(), true)
            .unwrap();
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

    #[rstest]
    fn existing_path_may_be_reused_when_serializing(
        io_opened: IoInstanceFixture,
        serializable_object: Object,
    ) {
        let (io, temp_dir) = io_opened;
        let base_path = Path::new("base_dir");
        let file1_path = base_path.join("file1.json");
        let file1_full_path = database_dir(&temp_dir).join(&file1_path);
        let file2_path = base_path.join("file2.json");
        let file2_full_path = database_dir(&temp_dir).join(&file2_path);

        // Serialize first object
        assert!(!file1_full_path.exists());
        io.serialize_new(&serializable_object, file1_path.clone(), true)
            .unwrap();
        assert!(file1_full_path.exists());
        assert_gt!(file_len(&file1_full_path), 0);

        // Serialize second object whose path already exists in the filesystem
        assert!(!file2_full_path.exists());
        io.serialize_new(&serializable_object, file2_path.clone(), true)
            .unwrap();
        assert!(file2_full_path.exists());
        assert_gt!(file_len(&file2_full_path), 0);

        remove_temp_dir(temp_dir);
    }

    #[rstest]
    #[case::file1("serialized.json")]
    fn non_pretty_json_implies_smaller_file_size_when_serializing(
        #[case] path: PathBuf,
        io_opened: IoInstanceFixture,
        serializable_object: Object,
    ) {
        let (io, temp_dir) = io_opened;
        let full_path = database_dir(&temp_dir).join(&path);

        // Write pretty JSON
        io.serialize_new(&serializable_object, path.clone(), true)
            .unwrap();
        let first_len = file_len(&full_path);
        assert_gt!(first_len, 0);

        // Write non-pretty JSON into the same file
        io.serialize(&serializable_object, path.clone(), false)
            .unwrap();
        assert_lt!(file_len(&full_path), first_len);

        remove_temp_dir(temp_dir);
    }

    #[rstest]
    #[case::absolute_path(Path::new("/").join(Io::METADATA_DIR).join(Io::METADATA_FILE))]
    #[case::path_to_directory(Path::new(Io::METADATA_DIR).to_path_buf())]
    #[case::empty_path("")]
    fn invalid_path_throws_error_when_deserializing(
        #[case] path: PathBuf,
        io_opened: IoInstanceFixture,
    ) {
        let (io, temp_dir) = io_opened;

        let result: Result<Object> = io.deserialize(path);
        let err = result.unwrap_err();
        assert_eq!(CustomKind::InvalidArgument, *err.get_custom_kind().unwrap());

        remove_temp_dir(temp_dir);
    }

    #[rstest]
    fn empty_file_throws_error_when_deserializing(io_opened: IoInstanceFixture) {
        let (io, temp_dir) = io_opened;
        let filename = "file.json";
        let path = database_dir(&temp_dir).join(filename);

        File::create(path.clone()).unwrap();
        let result: Result<Object> = io.deserialize(path);
        let err = result.unwrap_err();
        // Expect serde error
        assert!(!err.is_custom());

        remove_temp_dir(temp_dir);
    }

    #[rstest]
    fn deserializing_throws_error_when_different_object_was_written_into_file(
        io_opened: IoInstanceFixture,
        serializable_object: Object,
    ) {
        let (io, temp_dir) = io_opened;
        let path = "serialized.json";

        #[derive(Deserialize, Debug)]
        struct AnotherObject {
            some_field: i32,
            another_field: u8,
        }
        io.serialize_new(&serializable_object, path.clone(), true)
            .unwrap();
        let result: Result<AnotherObject> = io.deserialize(path);
        let err = result.unwrap_err();
        // Expect serde error
        assert!(!err.is_custom());

        remove_temp_dir(temp_dir);
    }

    #[rstest]
    #[case::base_dir("serialized.json")]
    #[case::sub_dir("sub/serialized.json")]
    #[case::sub_sub_dir("sub/sub/serialized.json")]
    fn object_may_be_deserialized(
        #[case] path: PathBuf,
        io_opened: IoInstanceFixture,
        serializable_object: Object,
    ) {
        let (io, temp_dir) = io_opened;

        io.serialize_new(&serializable_object, path.clone(), true)
            .unwrap();
        let deserialized = io.deserialize(path).unwrap();
        assert_eq!(serializable_object, deserialized);

        remove_temp_dir(temp_dir);
    }
}
