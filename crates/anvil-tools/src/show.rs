use anyhow::{anyhow, Result};

use crate::Value;


pub struct ShowTool;

impl ShowTool {
    pub async fn run(input: Value) -> Result<Value>
    {
        let data = match input {
            Value::Single(data) => data,
            _ => return Err(anyhow!("filter requires single input")),
        };

        println!("Source: {}", data.src);
        data.df.clone().show().await?;

        Ok(Value::Single(data))
    }
}
