use anyhow::{anyhow, Result};

use anvil_parse::ast::{Literal, ToolArg};
use crate::value::Value;


pub struct FilterTool;

impl FilterTool {
    pub async fn run(
        input: Value,
        args: &[ToolArg]
    ) -> Result<Value>
    {
        let df = match input {
            Value::Single(df) => df,
            _ => return Err(anyhow!("filter requires input")),
        };

        // Expect exactly one positional string arg
        let predicate = match args {
            [ToolArg::Positional(Literal::String(s))] => s,
            _ => return Err(anyhow!("filter requires a predicate string")),
        };

        let expr = df.parse_sql_expr(predicate)?;
        let df_true  = df.clone().filter(expr.clone())?;
        let df_false = df.filter(expr.is_false())?;

        Ok(Value::Multiple(vec![df_true, df_false]))
    }
}
