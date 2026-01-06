use anyhow::{anyhow, Result};

use crate::tools::{ToolArgs, ToolId, ToolRef, Values};

pub async fn run(id: &ToolId, args: &DropArgs, inputs: Values) -> Result<Values>
{
    let df = inputs.get_one().cloned()
        .ok_or_else(|| anyhow!("drop tool ({id}) requires input"))?;

    let cols = args.cols.split(',').collect::<Vec<_>>();
    let df = df.drop_columns(&cols)?;

    Ok(Values::new(df))
}

#[derive(Debug)]
pub struct DropArgs {
    cols: String,
}

impl TryFrom<&ToolRef> for DropArgs {
    type Error = anyhow::Error;

    fn try_from(tr: &ToolRef) -> Result<Self>
    {
        let args = ToolArgs::new(&tr.args)?;
        args.check_named_args(&[])?;

        let cols = args.required_positional_string(0, "cols")?;

        Ok(DropArgs { cols })
    }
}