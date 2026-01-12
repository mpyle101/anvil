use anyhow::{anyhow, Result};
use datafusion::prelude::{try_cast, Expr, SessionContext};

use anvil_context::resolve;
use crate::eval_expression;
use crate::tools::{parse_expression, ArgValue, ToolArg, ToolId, ToolRef, Values};

pub async fn run(_id: &ToolId, args: &ProjectArgs, inputs: Values, ctx: &SessionContext) -> Result<Values>
{
    let df = if let Some(df) = inputs.get_one() {
        df.clone()
    } else {
        ctx.read_empty()?
    };
    let df = df.select(args.exprs.clone())?;

    Ok(Values::new(df))
}

#[derive(Debug)]
pub struct ProjectArgs {
    exprs: Vec<Expr>,
}

impl TryFrom<&ToolRef> for ProjectArgs {
    type Error = anyhow::Error;

    fn try_from(tr: &ToolRef) -> Result<Self>
    {
        let mut exprs = Vec::new();
        for arg in &tr.args {
            match arg {
                ToolArg::Positional(_) => {
                    return Err(anyhow!("projection tool only accepts keyword arguments"))
                }
                ToolArg::Keyword { ident, value, dtype } => {
                    match value {
                        ArgValue::String(s) => {
                            let expr  = parse_expression(s)?;
                            let right = eval_expression(&expr)?;
                            let alias = resolve(*ident);
                            if let Some(dt) = dtype {
                                exprs.push(try_cast(right.alias(alias), dt.clone()))
                            } else {
                                exprs.push(right.alias(alias));
                            }
                        }
                        _ => return Err(anyhow!("projection tool expressions must be a string {value:?}"))
                    }
                }
            }
        }

        Ok(ProjectArgs { exprs })
    }
}