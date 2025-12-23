use std::convert::TryFrom;

use anyhow::{anyhow, Result};

use anvil_parse::ast::ToolArg;
use crate::{Data, ToolArgs, Value};


pub struct FilterTool;

impl FilterTool {
    pub async fn run(
        input: Value,
        args: &[ToolArg]
    ) -> Result<Value>
    {
        let Data { df, src } = match input {
            Value::Single(data) => data,
            _ => return Err(anyhow!("filter requires single input")),
        };

        let args: FilterArgs = args.try_into()?;
        let expr = df.parse_sql_expr(args.predicate.as_str())?;

        let df_true  = df.clone().filter(expr.clone())?;
        let df_false = df.filter(expr.is_false())?;

        Ok(Value::Multiple(vec![
            Data { df: df_true, src: src.clone() }, 
            Data { df: df_false, src },
        ]))
    }
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