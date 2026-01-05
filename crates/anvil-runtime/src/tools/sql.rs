use anyhow::{anyhow, Result};
use datafusion::prelude::SessionContext;

use anvil_context::{resolve, Symbol};
use crate::tools::{ArgValue, ToolArg, ToolId, ToolRef, Values};

pub async fn run(args: &SqlArgs, inputs: Option<Values>, ctx: &SessionContext) -> Result<Values>
{
    let df = if let Some(sql) = &args.sql {
        ctx.sql(sql).await?
    } else if let Some(v) = inputs {
        let df = v.get_one().unwrap();
        let mut exprs = vec![];
        for (ident, sql) in &args.exprs {
            let expr = df.parse_sql_expr(sql)?;
            exprs.push(expr.alias(resolve(*ident)));
        }
        df.clone().select(exprs)?
    } else {
        return Err(anyhow!("sql tool ({}) requires SQL string or input", args.id))
    };

    Ok(Values::new(df))
}

#[derive(Debug)]
pub struct SqlArgs {
    pub id: ToolId,
    sql: Option<String>,
    exprs: Vec<(Symbol, String)>
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
                        ArgValue::String(s) => exprs.push((*ident, s.clone())),
                        _ => return Err(anyhow!("sql tool expression must be a string {value:?}"))
                    }
                }
            }
        }

        Ok(SqlArgs { id: tr.id, sql, exprs })
    }
}