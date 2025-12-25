use anyhow::{anyhow, Result};

use crate::tools::{Data, ToolArg, Value};

pub async fn run(input: Value, _args: &[ToolArg]) -> Result<Value>
{
    let Data { df, .. } = match input {
        Value::Single(data) => data,
        _ => return Err(anyhow!("describe requires single input")),
    };

    let df = df.describe().await?;

    Ok(Value::Single(Data { df, src: "describe".into() }))
}
