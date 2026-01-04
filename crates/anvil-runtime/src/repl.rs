use std::io::Write;

use anyhow::{anyhow, Result};

use anvil_parse::ASTBuilder;
use crate::{run, run_stmt, Executor, Planner};

pub async fn run_repl() -> Result<()>
{
    let mut builder  = ASTBuilder::new();
    let mut planner  = Planner::default();
    let mut executor = Executor::default();

    loop {
        let input = readline()?;
        let line = input.trim();
        if line.is_empty() {
            continue;
        }

        match parse_command(line) {
            Err(e)  => { println!("{e}"); continue; },
            Ok(cmd) => match cmd {
                Some(Cmd::Run(script)) => {
                    let source = std::fs::read_to_string(script)?;
                    if let Err(e) = run(&source).await {
                        println!("{e}");
                    }
                    continue;
                }
                Some(Cmd::Help(tool)) => {
                    println!("<help for {tool}>");
                    continue;
                }
                Some(Cmd::Reset) => {
                    executor.reset();
                    continue;
                }
                Some(Cmd::Exit)  => return Ok(()),
                None => {}
            }
        }

        let stmt = if line.ends_with(';') {
            line.to_string()
        } else {
            format!("{line};")
        };
        match run_stmt(&mut builder, &mut planner, &mut executor, &stmt).await {
            Ok(_)  => {}
            Err(e) => println!("{e}"),
        }
    }
}

enum Cmd {
    Run(String),
    Help(String),
    Exit,
    Reset,
}

fn readline() -> Result<String>
{
    write!(std::io::stdout(), "anvil> ")?;
    std::io::stdout().flush()?;

    let mut buffer = String::new();
    std::io::stdin().read_line(&mut buffer)?;

    Ok(buffer)
}

fn parse_command(line: &str) -> Result<Option<Cmd>>
{
    let mut iter = line.splitn(2, ' ');
    let cmd = match iter.next() {
        Some("run") => {
            if let Some(script) = iter.next() {
                Some(Cmd::Run(script.to_string()))
            } else {
                return Err(anyhow!("run command requires path to script file"))
            }
        }
        Some("help") => {
            if let Some(tool) = iter.next() {
                Some(Cmd::Help(tool.to_string()))
            } else {
                Some(Cmd::Help("".into()))
            }
        }
        Some("reset") =>  Some(Cmd::Reset),
        Some("exit") | Some("quit") => Some(Cmd::Exit),
        _ => None,
    };

    Ok(cmd)
}