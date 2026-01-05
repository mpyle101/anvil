use anyhow::{anyhow, Result};
use datafusion::prelude::{col, Expr};

use crate::tools::{ToolArgs, ToolId, ToolRef, Values};

pub async fn run(args: &SelectArgs, inputs: Values) -> Result<Values>
{
    let df = inputs.get_one().cloned()
        .ok_or_else(|| anyhow!("select tool ({}) requires input", args.id))?;
    let df = df.select(args.exprs.clone())?;

    Ok(Values::new(df))
}

#[derive(Debug)]
pub struct SelectArgs {
    pub id: ToolId,
    exprs: Vec<Expr>,
}

impl TryFrom<&ToolRef> for SelectArgs {
    type Error = anyhow::Error;

    fn try_from(tr: &ToolRef) -> Result<Self>
    {
        let args = ToolArgs::new(&tr.args)?;
        args.check_named_args(&[])?;

        let cols  = args.required_positional_string(0, "cols")?;
        let exprs = cols.split(',')
            .map(|s| {
                match s.split_once(':') {
                    Some((s1, s2)) => col(s1).alias(s2),
                    None => col(s)
                }
            })
            .collect::<Vec<_>>();

        Ok(SelectArgs { id: tr.id, exprs })
    }
}