
use clap::Args;
use db::database::Database;
use db::error::Result;
use serde::Serialize;
use std::env;
use std::path::{Path, PathBuf};

/// List of arguments supported by the command.
#[derive(Args)]
pub struct Params {
    #[clap(short, long, help = "Database name")]
    name: String,
    #[clap(short, long, help = "Database directory")]
    directory: Option<String>,
    #[clap(short, long, help = "JSON output format")]
    pub json_output: bool,
}

/// Output of the command
#[derive(Serialize)]
pub struct Output {
    path: PathBuf,
}

/// Print command's text output
pub fn print_text_output(outcome: &Output) {
    println!("Created an empty database inside: {}", outcome.path.display());
}

/// Main entry of the command
pub fn execute(params: &Params) -> Result<Output> {
    // Initialize an empty database structure inside a specified directory
    // or inside the current directory in case no argument was provided
    let path = match &params.directory {
        Some(directory) => Path::new(directory).to_path_buf(),
        None => env::current_dir()?,
    };
    Database::create(&params.name,path.clone())?;

    // Database path consists of a base path joined with a database name
    Ok(Output { path: path.join(&params.name) })
}