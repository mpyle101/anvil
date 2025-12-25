use anyhow::{anyhow, Result};

use crate::tools::{ToolArg, ToolArgs, Value};

pub struct PrintTool;

impl PrintTool {
    pub async fn run(input: Value, args: &[ToolArg]) -> Result<Value>
    {
        println!("ARGS: {args:?}");
        let data = match input {
            Value::Single(data) => data,
            _ => return Err(anyhow!("print tool requires single input")),
        };

        println!("Source: {}", data.src);
        let args: PrintArgs = args.try_into()?;
        if let Some(limit) = args.limit {
            data.df.clone().show_limit(limit as usize).await?;
        } else {
            data.df.clone().show().await?;
        }

        Ok(Value::Single(data))
    }
}

struct PrintArgs {
    limit: Option<i64>,
}

impl TryFrom<&[ToolArg]> for PrintArgs {
    type Error = anyhow::Error;

    fn try_from(args: &[ToolArg]) -> Result<Self>
    {
        let args = ToolArgs::new(args)?;
        args.check_named_args(&["limit"])?;

        let limit = args.optional_positional_integer(0, "limit")?;

        Ok(PrintArgs { limit })
    }
}