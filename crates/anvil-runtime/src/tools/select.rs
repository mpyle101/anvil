use anyhow::{anyhow, Result};
use datafusion::prelude::{col, try_cast, Expr};

use crate::tools::{ArgValue, ToolArgs, ToolId, ToolRef, Values};

use anvil_context::resolve;

pub async fn run(id: &ToolId, args: &SelectArgs, inputs: Values) -> Result<Values>
{
    let df = inputs.get_one().cloned()
        .ok_or_else(|| anyhow!("select tool ({id}) requires input"))?;
    let df = df.select(args.exprs.clone())?;

    Ok(Values::new(df))
}

#[derive(Debug)]
pub struct SelectArgs {
    exprs: Vec<Expr>,
}

impl TryFrom<&ToolRef> for SelectArgs {
    type Error = anyhow::Error;

    fn try_from(tr: &ToolRef) -> Result<Self>
    {
        let args = ToolArgs::new(&tr.args)?;

        let mut exprs = vec![];
        for v in args.positional {
            match v {
                ArgValue::Ident(s)  => exprs.push(col(s)),
                ArgValue::String(s) => exprs.push(col(s)),
                _ => return Err(anyhow!("select columns must be a string or identifier: {v:?}"))
            }
        }

        for (sym, (v, dt)) in args.keyword {
            match v {
                ArgValue::Ident(s) | ArgValue::String(s) => {
                    let alias  = resolve(sym);
                    let column = col(format!(r#""{s}""#));  // preserve case
                    if let Some(dtype) = dt {
                        exprs.push(try_cast(column.alias(alias), dtype))
                    } else {
                        exprs.push(column.alias(alias))
                    }
                }
                _ => return Err(anyhow!("select columns must be a string or identifier: {v:?}"))
            }
        }

        Ok(SelectArgs { exprs })
    }
}