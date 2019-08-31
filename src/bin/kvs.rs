extern crate clap;
use clap::{crate_version, App, AppSettings, Arg, SubCommand};
use failure;
use kvs;
use kvs::KvStore;
use std::env;

fn main() -> kvs::Result<()> {
    if let Err(e) = run_kvs() {
        // Print the Display message for any error.
        // Simply returning the error will print the Debug version, which is not as nice.
        println!("{}", e);
        std::process::exit(1)
    }
    Ok(())
}

fn run_kvs() -> kvs::Result<()> {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(crate_version!())
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .settings(&[
            AppSettings::VersionlessSubcommands,
            AppSettings::SubcommandRequiredElseHelp,
        ])
        .subcommand(
            SubCommand::with_name("get")
                .about("Get the string value of a given string key")
                .arg(
                    Arg::with_name("key")
                        .takes_value(true)
                        .value_name("KEY")
                        .required(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("set")
                .about("Set the value of a string key to a string")
                .arg(
                    Arg::with_name("key")
                        .takes_value(true)
                        .value_name("KEY")
                        .required(true),
                )
                .arg(
                    Arg::with_name("value")
                        .takes_value(true)
                        .value_name("VALUE")
                        .required(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("rm").about("Remove a given key").arg(
                Arg::with_name("key")
                    .takes_value(true)
                    .value_name("KEY")
                    .required(true),
            ),
        )
        .get_matches();

    let curr_dir = env::current_dir()?;

    let mut kvstore = KvStore::open(&curr_dir)?;

    match matches.subcommand() {
        ("get", Some(command_matches)) => match command_matches.value_of("key") {
            Some(key) => {
                match kvstore.get(key.into())? {
                    Some(value) => println!("{}", value),
                    None => println!("Key not found"),
                }
                Ok(())
            }
            _ => Err(KvsCliError::UnexpectedArgs {})?,
        },
        ("set", Some(command_matches)) => match (
            command_matches.value_of("key"),
            command_matches.value_of("value"),
        ) {
            (Some(key), Some(value)) => kvstore.set(key.into(), value.into()),
            _ => Err(KvsCliError::UnexpectedArgs {})?,
        },
        ("rm", Some(command_matches)) => match command_matches.value_of("key") {
            Some(key) => kvstore.remove(key.into()),
            _ => Err(KvsCliError::UnexpectedArgs {})?,
        },
        (cmd, _) => Err(KvsCliError::UnknownCommand {
            command: cmd.to_string(),
        })?,
    }
}

#[derive(Debug, failure::Fail)]
enum KvsCliError {
    #[fail(display = "Unknown command: {}", command)]
    UnknownCommand { command: String },

    #[fail(display = "Unexpected CLI arguments")]
    UnexpectedArgs {},
}
