use anyhow::{anyhow, Result};
use datafusion::scalar::ScalarValue;

use crate::tools::{Data, ToolArg, ToolArgs, ToolRef, Value};

pub async fn run(tr: &ToolRef, input: Value) -> Result<Value>
{
    let Data { df, src } = match input {
        Value::Single(data) => data,
        _ => return Err(anyhow!("drop requires single input")),
    };

    let args: FillArgs = tr.args.as_slice().try_into()?;
    let cols = args.cols.unwrap_or("".into())
        .split(',')
        .map(|s| s.to_owned())
        .collect::<Vec<_>>();
    let df = df.fill_null(ScalarValue::from(args.value), cols)?;

    Ok(Value::Single(Data { df, src }))
}

struct FillArgs {
    value: i64,
    cols: Option<String>,
}

impl TryFrom<&[ToolArg]> for FillArgs {
    type Error = anyhow::Error;

    fn try_from(args: &[ToolArg]) -> Result<Self>
    {
        let args = ToolArgs::new(args)?;
        args.check_named_args(&[])?;

        let value = args.require_positional_integer(0, "value")?;
        let cols = args.optional_positional_string(1, "cols")?;

        Ok(FillArgs { value, cols })
    }
}