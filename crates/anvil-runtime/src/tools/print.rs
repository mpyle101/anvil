use anyhow::{anyhow, Result};

use crate::tools::{ToolArg, ToolArgs, ToolRef, Value};

pub async fn run(tr: &ToolRef, input: Value) -> Result<Value>
{
    let data = match input {
        Value::Single(data) => data,
        _ => return Err(anyhow!("print tool requires single input")),
    };

    println!("Source: {}", data.src);
    let args: PrintArgs = tr.args.as_slice().try_into()?;
    if let Some(limit) = args.limit {
        data.df.clone().show_limit(limit as usize).await?;
    } else {
        data.df.clone().show().await?;
    }

    Ok(Value::Single(data))
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