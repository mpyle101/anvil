use anyhow::{anyhow, Result};

use crate::tools::{Data, ToolArg, Value};

pub struct DistinctTool;

impl DistinctTool {
    pub async fn run(input: Value, _args: &[ToolArg]) -> Result<Value>
    {
        let Data { df, src } = match input {
            Value::Single(data) => data,
            _ => return Err(anyhow!("distinct tool requires single input")),
        };

        let df = df.distinct()?;

        Ok(Value::Single(Data { df, src }))
    }
}
