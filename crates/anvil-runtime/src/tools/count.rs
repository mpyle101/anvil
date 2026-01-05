use anyhow::{anyhow, Result};
use datafusion::prelude::*;
use datafusion::execution::context::SessionContext;

use anvil_context::intern;
use crate::tools::{ToolArgs, ToolRef, Values};

pub async fn run(args: &CountArgs, inputs: Values, ctx: &SessionContext) -> Result<Values>
{
    let df = inputs.get_one()
        .ok_or_else(|| anyhow!("count tool requires input"))?;

    let n = df.clone().count().await? as i64;
    let df = ctx.read_empty()?
        .with_column(&args.col, lit(n))?;

    Ok(Values::new(df))
}

#[derive(Debug)]
pub struct CountArgs {
    col: String,
}

impl TryFrom<&ToolRef> for CountArgs {
    type Error = anyhow::Error;

    fn try_from(tr: &ToolRef) -> Result<Self>
    {
        let args = ToolArgs::new(&tr.args)?;
        args.check_named_args(&[intern("col")])?;

        let col = args.optional_positional_string(0, "col")?.unwrap_or("count".into());

        Ok(CountArgs { col })
    }
}