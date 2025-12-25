use anyhow::{anyhow, Result};

use crate::tools::{Data, ToolArg, Value};

pub async fn run(input: Value, _args: &[ToolArg]) -> Result<Value>
{
    let data = match input {
        Value::Multiple(data) => data,
        _ => return Err(anyhow!("intersect requires multiple input")),
    };
    if data.len() != 2 {
        return Err(anyhow!("intersect requires two data sets: (left, right)"))
    }
    let df_left  = data[0].df.clone();
    let df_right = data[1].df.clone();

    let df = df_left.intersect(df_right)?;

    Ok(Value::Single(Data { df, src: "intersect".into() }))
}
