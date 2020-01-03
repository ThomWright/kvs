extern crate clap;
use clap::{crate_version, App, AppSettings, Arg, SubCommand};
use failure;
use kvs;
use kvs::KvsClient;
use std::env;

fn main() -> kvs::Result<()> {
    if let Err(e) = run_kvs() {
        // Print the Display message for any error.
        // Simply returning the error will print the Debug version, which is not as nice.
        eprintln!("{}", e);
        std::process::exit(1)
    }
    Ok(())
}

fn run_kvs() -> kvs::Result<()> {
    let version = env!("CARGO_PKG_VERSION");

    let addr_arg = Arg::with_name("addr")
        .help("An IP address, either v4 or v6, and a port number, with the format IP:PORT")
        .long("addr")
        .takes_value(true)
        .value_name("ADDR")
        .default_value("127.0.0.1:4000");
    let key_arg = Arg::with_name("key")
        .takes_value(true)
        .value_name("KEY")
        .required(true);

    let matches = App::new(&[env!("CARGO_PKG_NAME"), "-client"].concat())
        .version(crate_version!())
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .version(version)
        .settings(&[
            AppSettings::VersionlessSubcommands,
            AppSettings::SubcommandRequiredElseHelp,
        ])
        .subcommand(
            SubCommand::with_name("get")
                .about("Get the string value of a given string key")
                .arg(&key_arg)
                .arg(&addr_arg),
        )
        .subcommand(
            SubCommand::with_name("set")
                .about("Set the value of a string key to a string")
                .arg(&key_arg)
                .arg(
                    Arg::with_name("value")
                        .takes_value(true)
                        .value_name("VALUE")
                        .required(true),
                )
                .arg(&addr_arg),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("Remove a given key")
                .arg(&key_arg)
                .arg(&addr_arg),
        )
        .get_matches();

    match matches.subcommand() {
        ("get", Some(command_matches)) => match command_matches.value_of("key") {
            Some(key) => {
                let address = command_matches.value_of("addr").unwrap();
                let client = KvsClient::connect(address)?;
                match client.get(key.to_string())? {
                    None => println!("Key not found"),
                    Some(value) => println!("{}", value),
                }
                Ok(())
            }
            _ => Err(KvsClientCliError::UnexpectedArgs.into()),
        },
        ("set", Some(command_matches)) => match (
            command_matches.value_of("key"),
            command_matches.value_of("value"),
        ) {
            (Some(key), Some(value)) => {
                let address = command_matches.value_of("addr").unwrap();
                let client = KvsClient::connect(address)?;
                client.set(key.to_string(), value.to_string())
            }
            _ => Err(KvsClientCliError::UnexpectedArgs.into()),
        },
        ("rm", Some(command_matches)) => match command_matches.value_of("key") {
            Some(key) => {
                let address = command_matches.value_of("addr").unwrap();
                let client = KvsClient::connect(address)?;
                client.remove(key.to_string())
            }
            _ => Err(KvsClientCliError::UnexpectedArgs.into()),
        },
        (cmd, _) => Err(KvsClientCliError::UnknownCommand {
            command: cmd.to_string(),
        }
        .into()),
    }
}

#[derive(Debug, failure::Fail)]
enum KvsClientCliError {
    #[fail(display = "Unknown command: {}", command)]
    UnknownCommand { command: String },

    #[fail(display = "Unexpected CLI arguments")]
    UnexpectedArgs,
}
