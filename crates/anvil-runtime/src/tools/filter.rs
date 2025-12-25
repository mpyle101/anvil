use anyhow::{anyhow, Result};

use crate::eval_expression;
use crate::tools::{parse_expression, Data, ToolArg, ToolArgs, Value};

pub async fn run(input: Value, args: &[ToolArg]) -> Result<Value>
{
    let Data { df, src } = match input {
        Value::Single(data) => data,
        _ => return Err(anyhow!("filter requires single input")),
    };

    let args: FilterArgs = args.try_into()?;
    let ast  = parse_expression(args.predicate.as_str())?;
    let expr = eval_expression(&ast)?;

    let df_true  = df.clone().filter(expr.clone())?;
    let df_false = df.filter(expr.is_not_true())?;

    Ok(Value::Multiple(vec![
        Data { df: df_true, src: src.clone() }, 
        Data { df: df_false, src },
    ]))
}

struct FilterArgs {
    predicate: String,
}

impl TryFrom<&[ToolArg]> for FilterArgs {
    type Error = anyhow::Error;

    fn try_from(args: &[ToolArg]) -> Result<Self>
    {
        let args = ToolArgs::new(args)?;
        args.check_named_args(&[])?;

        let predicate = args.require_positional_string(0, "path")?;

        Ok(FilterArgs { predicate })
    }
}