use std::convert::TryFrom;

use anyhow::{anyhow, Result};
use datafusion::prelude::{DataFrame, Expr};

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

        let args: FilterArgs = (args, &df).try_into()?;
        let df_true  = df.clone().filter(args.predicate.clone())?;
        let df_false = df.filter(args.predicate.is_false())?;

        Ok(Value::Multiple(vec![
            Data { df: df_true, src: src.clone() }, 
            Data { df: df_false, src },
        ]))
    }
}

struct FilterArgs {
    predicate: Expr,
}

impl TryFrom<(&[ToolArg], &DataFrame)> for FilterArgs {
    type Error = anyhow::Error;

    fn try_from((args, df): (&[ToolArg], &DataFrame)) -> Result<Self>
    {
        let args = ToolArgs::new(args)?;
        args.check_named_args(&[])?;

        let predicate = args.require_positional_string(0, "path")?;
        let expr = df.parse_sql_expr(predicate.as_str())?;

        Ok(FilterArgs { predicate: expr })
    }
}