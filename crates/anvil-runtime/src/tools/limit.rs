use anyhow::{anyhow, Result};

use crate::tools::{Data, ToolArg, ToolArgs, Value};

pub async fn run(input: Value, args: &[ToolArg]) -> Result<Value>
{
    let Data { df, src } = match input {
        Value::Single(data) => data,
        _ => return Err(anyhow!("limit requires single input")),
    };

    let args: LimitArgs = args.try_into()?;
    let df = df.limit(args.skip, Some(args.count))?;

    Ok(Value::Single(Data { df, src }))
}

struct LimitArgs {
    count: usize,
    skip: usize,
}

impl TryFrom<&[ToolArg]> for LimitArgs {
    type Error = anyhow::Error;

    fn try_from(args: &[ToolArg]) -> Result<Self>
    {
        let args = ToolArgs::new(args)?;
        args.check_named_args(&["count"])?;

        let count = args.require_positional_integer(0, "count")? as usize;
        let skip  = args.optional_integer("skip")?.unwrap_or(0) as usize;

        Ok(LimitArgs { count, skip })
    }
}