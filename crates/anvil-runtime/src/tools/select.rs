use anyhow::{anyhow, Result};
use datafusion::prelude::col;

use crate::tools::{Data, ToolArg, ToolArgs, ToolRef, Value};

pub async fn run(tr: &ToolRef, input: Value) -> Result<Value>
{
    let Data { df, src } = match input {
        Value::Single(data) => data,
        _ => return Err(anyhow!("select requires single input")),
    };

    let args: SelectArgs = tr.args.as_slice().try_into()?;
    let cols = args.cols.split(',')
        .map(|s| {
            match s.split_once(':') {
                Some((s1, s2)) => col(s1).alias(s2),
                None => col(s)
            }
        })
        .collect::<Vec<_>>();
    let df = df.select(cols)?;

    Ok(Value::Single(Data { df, src }))
}

struct SelectArgs {
    cols: String,
}

impl TryFrom<&[ToolArg]> for SelectArgs {
    type Error = anyhow::Error;

    fn try_from(args: &[ToolArg]) -> Result<Self>
    {
        let args = ToolArgs::new(args)?;
        args.check_named_args(&[])?;

        let cols = args.require_positional_string(0, "cols")?;

        Ok(SelectArgs { cols })
    }
}