use anyhow::Result;
use clap::Parser;

use anvil_runtime::{eval_program, run_repl};


#[derive(Parser)]
struct Cli {
    /// Path to the Anvil script to run
    script: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()>
{
    let cli = Cli::parse();
    if let Some(script) = cli.script {
        let source = std::fs::read_to_string(&script)?;
        eval_program(&source).await?;
    } else {
        run_repl().await?;
    }

    Ok(())
}
