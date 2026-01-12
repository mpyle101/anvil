use anyhow::{anyhow, Result};
use datafusion::prelude::col;
use datafusion::logical_expr::SortExpr;

use crate::tools::{ArgValue, ToolArgs, ToolId, ToolRef, Values};
use anvil_context::resolve;

pub async fn run(id: &ToolId, args: &SortArgs, inputs: Values) -> Result<Values>
{
    let df = inputs.get_one().cloned()
        .ok_or_else(|| anyhow!("sort tool ({id}) requires input"))?;
    let df = df.sort(args.exprs.clone())?;

    Ok(Values::new(df))
}

#[derive(Debug)]
pub struct SortArgs {
    exprs: Vec<SortExpr>,
}

impl TryFrom<&ToolRef> for SortArgs {
    type Error = anyhow::Error;

    fn try_from(tr: &ToolRef) -> Result<Self>
    {
        let args = ToolArgs::new(&tr.args)?;

        let mut exprs = vec![];
        for v in args.positional {
            match v {
                ArgValue::Ident(s) | ArgValue::String(s) => {
                    let column = col(format!(r#""{s}""#));
                    exprs.push(column.sort(true, false))
                }
                _ => return Err(anyhow!("sort columns must be a string or identifier: {v:?}"))
            }
        }

        for (sym, (v, _)) in args.keyword {
            let column = col(format!(r#""{}""#, resolve(sym)));
            match v {
                ArgValue::Ident(s) | ArgValue::String(s) => {
                    if s == "desc" {
                        exprs.push(column.sort(false, false))
                    } else if s == "asc" {
                        exprs.push(column.sort(true, false))
                    } else {
                        return Err(anyhow!("sort direction must be 'asc' or 'desc': {s}"))
                    }
                }
                _ => return Err(anyhow!("sort direction must be a string or identifier: {v:?}"))
            }
        }

        Ok(SortArgs { exprs })
    }
}