use std::io::Write;

use anyhow::{anyhow, Result};

use anvil_parse::{ASTBuilder, parse_program, parse_statement};
use crate::Interpreter;

pub async fn run_repl() -> Result<()>
{
    let mut builder = ASTBuilder::new();
    let mut interpreter = Interpreter::default();

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
                    if let Err(e) = run_program(&mut interpreter, &script).await {
                        println!("{e}");
                    }
                    continue;
                }
                Some(Cmd::Help(tool)) => {
                    println!("<help for {tool}>");
                    continue;
                }
                Some(Cmd::Reset) => interpreter.reset(),
                Some(Cmd::Exit)  => return Ok(()),
                None => {}
            }
        }

        let stmt = if line.ends_with(';') {
            line.to_string()
        } else {
            format!("{line};")
        };
        match parse_statement(&mut builder, &stmt) {
            Ok(stmt) => {
                if let Err(e) = interpreter.eval_statement(stmt).await {
                    println!("{e}");
                }
            }
            Err(e)  => println!("{e}"),
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
    write!(std::io::stdout(), "> ")?;
    std::io::stdout().flush()?;

    let mut buffer = String::new();
    std::io::stdin().read_line(&mut buffer)?;

    Ok(buffer)
}

async fn run_program(interpreter: &mut Interpreter, script: &str) -> Result<()>
{
    let source = std::fs::read_to_string(script)?;
    let program = parse_program(&source)?;
    interpreter.eval(program).await?;

    Ok(())
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