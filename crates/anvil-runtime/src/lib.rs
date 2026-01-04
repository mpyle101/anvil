use anyhow::Result;

mod executor;
mod expression;
mod planner;
mod repl;
mod tools;

use executor::Executor;
use anvil_parse::ASTBuilder;

pub use expression::eval_expression;
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

pub async fn run_stmt(
    builder: &mut ASTBuilder,
    planner: &mut Planner,
    executor: &mut Executor,
    input: &str
) -> Result<()>
{
    let stmt = anvil_parse::build_statement(builder, input)?;
    let plan = planner.build_statement(stmt)?;
    executor.run(plan).await
}