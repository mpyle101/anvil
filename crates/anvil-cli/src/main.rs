use anyhow::Result;
use clap::Parser;

use anvil_runtime::eval_program;


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
    eval_program(&source).await?;

    Ok(())
}
