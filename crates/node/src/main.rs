use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

use node::config::Config;

fn main() -> ExitCode {
    let Some(path) = env::args_os().nth(1).map(PathBuf::from) else {
        eprintln!("usage: vakthund <config.toml>");
        return ExitCode::FAILURE;
    };
    match Config::load(&path) {
        Ok(config) => {
            println!("{config:#?}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
