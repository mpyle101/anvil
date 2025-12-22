use anyhow::{anyhow, Result};

use crate::Value;


pub struct ShowTool;

impl ShowTool {
    pub async fn run(input: Value) -> Result<Value>
    {
        let df = match input {
            Value::Single(df) => df,
            _ => return Err(anyhow!("filter requires single input")),
        };

        df.clone().show().await?;

        Ok(Value::Single(df))
    }
}
