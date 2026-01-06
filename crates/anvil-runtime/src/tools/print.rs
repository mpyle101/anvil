use anyhow::{anyhow, Result};

use crate::tools::{ToolArgs, ToolId, ToolRef, Values};

pub async fn run(id: &ToolId, args: &PrintArgs, inputs: Values) -> Result<Values>
{
    let df = inputs.get_one().cloned()
        .ok_or_else(|| anyhow!("print tool ({id}) requires input"))?;

    if let Some(limit) = args.limit {
        df.clone().show_limit(limit as usize).await?;
    } else {
        df.clone().show().await?;
    }

    Ok(inputs)
}

#[derive(Debug)]
pub struct PrintArgs {
    limit: Option<i64>,
}

impl TryFrom<&ToolRef> for PrintArgs {
    type Error = anyhow::Error;

    fn try_from(tr: &ToolRef) -> Result<Self>
    {
        let args  = ToolArgs::new(&tr.args)?;
        let limit = args.optional_positional_integer(0, "limit")?;

        Ok(PrintArgs { limit })
    }
}