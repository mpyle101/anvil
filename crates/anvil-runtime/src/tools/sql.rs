use anyhow::{anyhow, Result};
use datafusion::prelude::SessionContext;

use crate::tools::{ArgValue, ToolArg, ToolRef, Values};

pub async fn run(args: &SqlArgs, inputs: Values, ctx: &SessionContext) -> Result<Values>
{
    let df = if let Some(sql) = &args.sql {
        ctx.sql(sql).await?
    } else if let Some(df) = inputs.get_one() {
        let mut exprs = vec![];
        for (ident, sql) in &args.exprs {
            let expr = df.parse_sql_expr(sql)?;
            exprs.push(expr.alias(ident));
        }
        df.clone().select(exprs)?
    } else {
        return Err(anyhow!("SQL string not found"))
    };

    Ok(Values::new(df))
}

#[derive(Debug)]
pub struct SqlArgs {
    sql: Option<String>,
    exprs: Vec<(String, String)>
}

impl TryFrom<&ToolRef> for SqlArgs {
    type Error = anyhow::Error;

    fn try_from(tr: &ToolRef) -> Result<Self>
    {
        let mut sql   = None;
        let mut exprs = Vec::new();
        for arg in &tr.args {
            match arg {
                ToolArg::Positional(value) => {
                    match value {
                        ArgValue::String(s) => sql = Some(s.clone()),
                        _ => return Err(anyhow!("sql tool SQL must be string"))
                    }
                }
                ToolArg::Keyword { ident, value } => {
                    match value {
                        ArgValue::String(s) => exprs.push((ident.clone(), s.clone())),
                        _ => return Err(anyhow!("sql tool expression must be a string {value:?}"))
                    }
                }
            }
        }

        Ok(SqlArgs { sql, exprs })
    }
}