//! Database Command Line Interface.
//!
//! CLI provides a set of commands which may be used either to create or interact with an existing
//! database instance.
//!
//! Each command resides inside an own file. The output may be a plain text or a JSON format which
//! enables simpler processing by external tools. A command is composed of at least following public
//! members:
//! - `Params` which is a struct supplying a list of supported parameters
//! - `Output` which describes what is returned as the result of running a command
//! - `execute` which is used to invoke a command and return an output to the caller
//! - `print_text_output` which prints out result(data) of a command in text mode
//! The function for packing an output in json format is common across all commands, therefore
//! it is not needed to implement the function for each command respectively. The only restriction
//! is that `Output` must implement `Serialize` trait.

#![deny(warnings)]
#![deny(missing_docs, rustdoc::missing_crate_level_docs)]

use clap::{Parser, Subcommand};
use db::error::Result;
use serde::Serialize;
use std::process;

mod create;

#[derive(Parser)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    #[clap(about = "Create an empty database")]
    Create(create::Params),
}

#[derive(Serialize)]
struct CommandError {
    cause: String,
}

#[derive(Serialize)]
struct CommandOutput<O>
where
    O: Serialize,
{
    status: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<O>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<CommandError>,
}

fn print_json_output<O>(output: &CommandOutput<O>)
where
    O: Serialize,
{
    println!("{}", serde_json::to_string_pretty(&output).unwrap());
}

fn do_execute<Params, Output>(
    exec: fn(Params) -> Result<Output>,
    params: Params,
    json: bool,
    print_text_output_fn: fn(&Output),
) where
    Output: Serialize,
{
    const STATUS_OK: i32 = 0;
    const STATUS_FAILURE: i32 = -1;
    const EXIT_FAILURE: i32 = 1;

    // Call a specific command's executor
    let result = exec(params);
    let error = result.is_err();
    let output = match result {
        Ok(outcome) => CommandOutput {
            status: STATUS_OK,
            data: Some(outcome),
            error: None,
        },

        Err(err) => CommandOutput {
            status: STATUS_FAILURE,
            data: None,
            error: Some(CommandError {
                cause: err.to_string(),
            }),
        },
    };

    // Print output
    if json {
        print_json_output(&output);
    } else {
        match output.status {
            STATUS_OK => print_text_output_fn(&output.data.unwrap()),
            _ => println!("{}", &output.error.unwrap().cause),
        }
    }

    // Notify the caller the command has failed
    if error {
        process::exit(EXIT_FAILURE);
    }
}

fn main() {
    // Parse and execute a specific command depending on the user input
    let cli = Cli::parse();
    match &cli.command {
        Commands::Create(params) => do_execute(
            create::execute,
            params,
            params.json,
            create::print_text_output,
        ),
    };
}
