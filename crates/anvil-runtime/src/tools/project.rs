use anyhow::{anyhow, Result};
use datafusion::execution::context::SessionContext;

use crate::eval_expression;
use crate::tools::{parse_expression, Data, ToolArg, ToolRef, Value};

pub async fn run(tr: &ToolRef, input: Value, ctx: &SessionContext) -> Result<Value>
{
    let df = match input {
        Value::Single(data) => data.df,
        Value::None => ctx.read_empty()?,
        _ => return Err(anyhow!("projection requires single or no input")),
    };

    let mut exprs = Vec::new();
    for arg in &tr.args {
        match arg {
            ToolArg::Positional(_) => {
                return Err(anyhow!("projection tool only accepts keyword arguments"))
            }
            ToolArg::Keyword { key, value } => {
                let expr  = parse_expression(value.to_string().as_str())?;
                let right = eval_expression(&expr)?;
                exprs.push(right.alias(key));
            }
        }
    }

    let df = df.select(exprs)?;

    Ok(Value::Single(Data { df, src: format!("project ({})", tr.id) }))
}
