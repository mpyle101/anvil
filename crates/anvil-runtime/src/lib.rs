use anyhow::Result;

mod executor;
mod expression;
//mod interpreter;
mod planner;
mod repl;
mod tools;

use executor::Executor;

pub use expression::eval_expression;
//pub use interpreter::{Interpreter, eval_program};
pub use planner::{ExecutionPlan, ExecEdge, ExecNode, Planner};
pub use repl::run_repl;

pub async fn run(input: &str) -> Result<()>
{
    let program = anvil_parse::build_program(input)?;

    let mut planner = Planner::default();
    let plan = planner.build(program)?;

    let mut executor = Executor::default();
    executor.run(plan).await?;

    Ok(())
}
