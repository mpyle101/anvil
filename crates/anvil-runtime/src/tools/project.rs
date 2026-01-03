use anyhow::{anyhow, Result};
use datafusion::prelude::{Expr, SessionContext};

use crate::eval_expression;
use crate::tools::{parse_expression, ArgValue, ToolArg, ToolRef, Values};

pub async fn run(args: &ProjectArgs, inputs: Values, ctx: &SessionContext) -> Result<Values>
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
                ToolArg::Keyword { ident, value } => {
                    match value {
                        ArgValue::String(s) => {
                            let expr  = parse_expression(s)?;
                            let right = eval_expression(&expr)?;
                            exprs.push(right.alias(ident));
                        }
                        _ => return Err(anyhow!("projection tool expression must be a string {value:?}"))
                    }
                }
            }
        }

        Ok(ProjectArgs { exprs })
    }
}