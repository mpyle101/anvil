use std::convert::TryFrom;

use anyhow::{anyhow, Result};
use datafusion::prelude::{DataFrame, Expr};

use anvil_parse::ast::ToolArg;
use crate::{ToolArgs, Value};


pub struct FilterTool;

impl FilterTool {
    pub async fn run(
        input: Value,
        args: &[ToolArg]
    ) -> Result<Value>
    {
        let df = match input {
            Value::Single(df) => df,
            _ => return Err(anyhow!("filter requires single input")),
        };

        let args = FilterArgs::try_from((args, &df))?;
        let df_true  = df.clone().filter(args.predicate.clone())?;
        let df_false = df.filter(args.predicate.is_false())?;

        Ok(Value::Multiple(vec![df_true, df_false]))
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