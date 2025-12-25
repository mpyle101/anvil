use anyhow::{anyhow, Result};

use crate::Value;


pub struct PrintTool;

impl PrintTool {
    pub async fn run(input: Value) -> Result<Value>
    {
        let data = match input {
            Value::Single(data) => data,
            _ => return Err(anyhow!("print tool requires single input")),
        };

        println!("Source: {}", data.src);
        data.df.clone().show().await?;

        Ok(Value::Single(data))
    }
}
