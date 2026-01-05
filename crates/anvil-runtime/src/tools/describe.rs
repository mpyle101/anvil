use anyhow::{anyhow, Result};

use crate::tools::{ToolId, ToolRef, Values};

pub async fn run(args: &DescribeArgs, inputs: Values) -> Result<Values>
{
    let df = inputs.get_one().cloned()
        .ok_or_else(|| anyhow!("describe tool ({}) requires input", args.id))?;

    let df = df.describe().await?;

    Ok(Values::new(df))
}

#[derive(Debug)]
pub struct DescribeArgs {
    pub id: ToolId,
}

impl TryFrom<&ToolRef> for DescribeArgs {
    type Error = anyhow::Error;

    fn try_from(tr: &ToolRef) -> Result<Self>
    {
        Ok(DescribeArgs { id: tr.id, })
    }
}