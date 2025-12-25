use anyhow::{anyhow, Result};

use crate::tools::{Data, ToolArg, ToolArgs, Value};

pub async fn run(input: Value, args: &[ToolArg]) -> Result<Value>
{
    let Data { df, src } = match input {
        Value::Single(data) => data,
        _ => return Err(anyhow!("drop requires single input")),
    };

    let args: DropArgs = args.try_into()?;
    let cols = args.cols.split(',').collect::<Vec<_>>();
    let df = df.drop_columns(&cols)?;

    Ok(Value::Single(Data { df, src }))
}

struct DropArgs {
    cols: String,
}

impl TryFrom<&[ToolArg]> for DropArgs {
    type Error = anyhow::Error;

    fn try_from(args: &[ToolArg]) -> Result<Self>
    {
        let args = ToolArgs::new(args)?;
        args.check_named_args(&[])?;

        let cols = args.require_positional_string(0, "cols")?;

        Ok(DropArgs { cols })
    }
}