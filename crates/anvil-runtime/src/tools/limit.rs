use anyhow::{anyhow, Result};

use crate::tools::{Data, ToolArg, ToolArgs, Value};

pub async fn run(input: Value, args: &[ToolArg]) -> Result<Value>
{
    let Data { df, src } = match input {
        Value::Single(data) => data,
        _ => return Err(anyhow!("limit requires single input")),
    };

    let args: LimitArgs = args.try_into()?;
    let df = df.limit(args.limit, args.count)?;

    Ok(Value::Single(Data { df, src }))
}

struct LimitArgs {
    limit: usize,
    count: Option<usize>,
}

impl TryFrom<&[ToolArg]> for LimitArgs {
    type Error = anyhow::Error;

    fn try_from(args: &[ToolArg]) -> Result<Self>
    {
        let args = ToolArgs::new(args)?;
        args.check_named_args(&["count"])?;

        let limit = args.require_positional_integer(0, "path")? as usize;
        let count = args.optional_integer("count")?.map(|n| n as usize);

        Ok(LimitArgs { limit, count })
    }
}