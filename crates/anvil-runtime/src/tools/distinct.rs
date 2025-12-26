use anyhow::{anyhow, Result};

use crate::tools::{Data, ToolRef, Value};

pub async fn run(_tr: &ToolRef, input: Value) -> Result<Value>
{
    let Data { df, src } = match input {
        Value::Single(data) => data,
        _ => return Err(anyhow!("distinct tool requires single input")),
    };

    let df = df.distinct()?;

    Ok(Value::Single(Data { df, src }))
}
