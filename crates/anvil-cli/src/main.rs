use anyhow::Result;
use clap::Parser;
use anvil_runtime::Interpreter;


#[derive(Parser)]
struct Cli {
    /// Path to the Anvil script to run
    script: String,
}

#[tokio::main]
async fn main() -> Result<()>
{
    let cli = Cli::parse();
    let source = std::fs::read_to_string(&cli.script)?;
    let program = anvil_parse::parser::parse_program(&source)?;

    let mut interpreter = Interpreter::default();
    interpreter.eval(program).await?;

    Ok(())
}
