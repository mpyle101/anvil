use anyhow::{anyhow, Result};

use crate::tools::{ToolId, ToolRef, Values};

pub async fn run(args: &DistinctArgs, inputs: Values) -> Result<Values>
{
    let df = inputs.get_one().cloned()
        .ok_or_else(|| anyhow!("distinct tool ({}) requires input", args.id))?;

    let df = df.distinct()?;

    Ok(Values::new(df))
}

#[derive(Debug)]
pub struct DistinctArgs {
    pub id: ToolId,
}

impl TryFrom<&ToolRef> for DistinctArgs {
    type Error = anyhow::Error;

    fn try_from(tr: &ToolRef) -> Result<Self>
    {
        Ok(DistinctArgs { id: tr.id, })
    }
}