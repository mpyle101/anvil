use anyhow::{anyhow, Result};
use datafusion::prelude::*;
use datafusion::execution::context::SessionContext;

use crate::tools::{Data, ToolArg, ToolArgs, Value};

pub struct CountTool;

impl CountTool {
    pub async fn run(input: Value, args: &[ToolArg], ctx: &SessionContext) -> Result<Value>
    {
        let Data { df, .. } = match input {
            Value::Single(data) => data,
            _ => return Err(anyhow!("count tool requires single input")),
        };

        let args: CountArgs = args.try_into()?;
        let n = df.clone().count().await? as i64;
        let df = ctx.read_empty()?
            .with_column(&args.col, lit(n))?;

        Ok(Value::Single(Data { df, src: "count".into() }))
    }
}

struct CountArgs {
    col: String,
}

impl TryFrom<&[ToolArg]> for CountArgs {
    type Error = anyhow::Error;

    fn try_from(args: &[ToolArg]) -> Result<Self>
    {
        let args = ToolArgs::new(args)?;
        args.check_named_args(&["col"])?;

        let col = args.optional_positional_string(0, "col")?.unwrap_or("count".into());

        Ok(CountArgs { col })
    }
}