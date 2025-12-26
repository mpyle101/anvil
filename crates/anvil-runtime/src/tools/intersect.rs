use anyhow::{anyhow, Result};

use crate::tools::{Data, ToolRef, Value};

pub async fn run(tr: &ToolRef, input: Value) -> Result<Value>
{
    let data = match input {
        Value::Multiple(data) => data,
        _ => return Err(anyhow!("intersect requires multiple inputs")),
    };
    if data.len() != 2 {
        return Err(anyhow!("intersect requires two data sets: (left, right)"))
    }
    let df_left  = data[0].df.clone();
    let df_right = data[1].df.clone();

    let df = df_left.intersect(df_right)?;

    Ok(Value::Single(Data { df, src: format!("intersect ({})", tr.id) }))
}
