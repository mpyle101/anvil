use anyhow::{anyhow, Result};
use datafusion::prelude::col;

use crate::tools::{Data, ToolArg, ToolArgs, Value};

pub async fn run(input: Value, args: &[ToolArg]) -> Result<Value>
{
    let Data { df, src } = match input {
        Value::Single(data) => data,
        _ => return Err(anyhow!("sort requires single input")),
    };

    let args: SortArgs = args.try_into()?;
    let expr = args.cols.split(',')
        .map(|s| {
            let parts = s.splitn(3, ':').collect::<Vec<_>>();
            if parts.is_empty() || parts[0].is_empty() {
                return Err(anyhow!("sort requires non-empty expressions"))
            }
            let expr = col(format!(r#""{}""#, parts[0]));
            let sort = match parts.len() {
                1 => expr.sort(false, false),
                2 => expr.sort(parts[1] == true.to_string(), false),
                _ => {
                    expr.sort(
                        parts[1] == true.to_string(),
                        parts[2] == true.to_string(),
                    )
                }
            };
            Ok(sort)
        })
        .collect::<Result<Vec<_>>>()?;
    let df = df.sort(expr)?;

    Ok(Value::Single(Data { df, src }))
}

struct SortArgs {
    cols: String,
}

impl TryFrom<&[ToolArg]> for SortArgs {
    type Error = anyhow::Error;

    fn try_from(args: &[ToolArg]) -> Result<Self>
    {
        let args = ToolArgs::new(args)?;
        args.check_named_args(&[])?;

        let cols = args.require_positional_string(0, "cols")?;

        Ok(SortArgs { cols })
    }
}