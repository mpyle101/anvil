use anyhow::{anyhow, Result};

use crate::tools::{ToolArgs, ToolId, ToolRef, Values};

pub async fn run(args: &DropArgs, inputs: Values) -> Result<Values>
{
    let df = inputs.get_one().cloned()
        .ok_or_else(|| anyhow!("drop tool ({}) requires input", args.id))?;

    let cols = args.cols.split(',').collect::<Vec<_>>();
    let df = df.drop_columns(&cols)?;

    Ok(Values::new(df))
}

#[derive(Debug)]
pub struct DropArgs {
    pub id: ToolId,
    cols: String,
}

impl TryFrom<&ToolRef> for DropArgs {
    type Error = anyhow::Error;

    fn try_from(tr: &ToolRef) -> Result<Self>
    {
        let args = ToolArgs::new(&tr.args)?;
        args.check_named_args(&[])?;

        let cols = args.required_positional_string(0, "cols")?;

        Ok(DropArgs { id: tr.id, cols })
    }
}