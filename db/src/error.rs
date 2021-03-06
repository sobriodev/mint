//! Error handling utilities.

use std::fmt::{Debug, Display, Formatter};

/// Custom error kinds produced by the library.
#[derive(Debug, PartialEq)]
pub enum CustomKind {
    /// Invalid argument
    InvalidArgument,
    /// Database IO layer error
    DbIo,
    /// JSON error
    Json,
}

/// Library error structure.
#[non_exhaustive]
#[derive(Debug)]
pub struct Custom {
    /// Error cause
    cause: String,
    /// Error kind
    kind: CustomKind,
}

/// Possible errors generated by the library.
/// This enum acts as a wrapper for error instances produced by dependencies on which this library
/// depends as well as custom ones specific to this library.
pub enum Error {
    /// I/O error (typically tied to filesystem errors)
    Io(std::io::Error),
    /// Serialization/deserialization error
    Serde(serde_json::Error),
    /// Custom error
    Custom(Custom),
}

/// Wrapper on Result type.
pub type Result<T> = std::result::Result<T, Error>;

impl Display for Custom {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.cause)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {}", e),
            Self::Serde(e) => write!(f, "Serde error: {}", e),
            Self::Custom(e) => write!(f, "Library error: {}", e),
        }
    }
}

impl Debug for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {:?}", e),
            Self::Serde(e) => write!(f, "Serde error: {:?}", e),
            Self::Custom(e) => write!(f, "I/O error: {:?}", e),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Self::Serde(e)
    }
}

impl std::error::Error for Error {}

impl Error {
    /// Generate a custom library error.
    ///
    /// # Examples
    /// ```
    /// use db::error::{Error, CustomKind};
    /// let error = Error::custom_err(CustomKind::InvalidArgument, "Invalid argument");
    /// ```
    #[must_use]
    pub fn custom_err(kind: CustomKind, cause: &str) -> Self {
        Self::Custom(Custom {
            cause: cause.to_string(),
            kind,
        })
    }

    /// Check if an error instance holds a custom error inside.
    ///
    /// # Examples
    /// ```
    /// use db::error::{Error, CustomKind};
    /// let error = Error::custom_err(CustomKind::InvalidArgument, "Invalid argument");
    /// assert!(error.is_custom());
    /// ```
    ///
    #[inline]
    #[must_use]
    pub fn is_custom(&self) -> bool {
        matches!(self, Self::Custom(_))
    }

    /// Get custom error kind if any or none otherwise.
    ///
    /// # Examples
    /// ```
    /// use db::error::{Error, CustomKind};
    /// let error = Error::custom_err(CustomKind::InvalidArgument, "Invalid argument");
    /// assert_eq!(CustomKind::InvalidArgument, *error.get_custom_kind().unwrap());
    /// ```
    #[inline]
    #[must_use]
    pub fn get_custom_kind(&self) -> Option<&CustomKind> {
        match self {
            Self::Custom(custom) => Some(&custom.kind),
            _ => None,
        }
    }
}
