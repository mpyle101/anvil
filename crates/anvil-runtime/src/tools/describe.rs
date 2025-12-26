use anyhow::{anyhow, Result};

use crate::tools::{Data, ToolRef, Value};

pub async fn run(tr: &ToolRef, input: Value) -> Result<Value>
{
    let Data { df, .. } = match input {
        Value::Single(data) => data,
        _ => return Err(anyhow!("describe requires single input")),
    };

    let df = df.describe().await?;

    Ok(Value::Single(Data { df, src: format!("describe ({})", tr.id) }))
}
