use anyhow::{anyhow, Result};
use datafusion::prelude::col;
use datafusion::logical_expr::SortExpr;

use crate::tools::{ToolArgs, ToolId, ToolRef, Values};

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
        args.check_named_args(&[])?;

        let cols = args.required_positional_string(0, "cols")?;
        let exprs = cols.split(',')
            .map(|s| {
                let parts = s.splitn(3, ':').collect::<Vec<_>>();
                if parts.is_empty() || parts[0].is_empty() {
                    return Err(anyhow!("sort requires non-empty expressions"))
                }
                let expr = col(format!(r#""{}""#, parts[0]));
                let sort = match parts.len() {
                    1 => expr.sort(false, false),
                    2 => expr.sort(parts[1] == true.to_string(), false),
                    _ => {
                        expr.sort(
                            parts[1] == true.to_string(),
                            parts[2] == true.to_string(),
                        )
                    }
                };
                Ok(sort)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(SortArgs { exprs })
    }
}