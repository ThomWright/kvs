extern crate clap;
use clap::{crate_version, App, AppSettings, Arg, SubCommand};

use kvs::KvStore;

fn main() {
    let matches = App::new("kvs")
        .settings(&[
            AppSettings::VersionlessSubcommands,
            AppSettings::SubcommandRequiredElseHelp,
        ])
        .version(crate_version!())
        .author("Thom Wright <dev@thomwright.co.uk>")
        .about("A key-value store")
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

    let mut kvstore = KvStore::new();

    match matches.subcommand() {
        ("get", Some(command_matches)) => {
            if let Some(key) = command_matches.value_of("key") {
                kvstore.get(key.into());
            }
        }
        ("set", Some(command_matches)) => {
            if let (Some(key), Some(value)) = (
                command_matches.value_of("key"),
                command_matches.value_of("value"),
            ) {
                kvstore.set(key.into(), value.into());
            }
        }
        ("rm", Some(command_matches)) => {
            if let Some(key) = command_matches.value_of("key") {
                kvstore.remove(key.into());
            }
        }
        _ => panic!("Unknown command"),
    }
}
