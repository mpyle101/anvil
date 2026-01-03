use anyhow::{anyhow, Result};

use crate::tools::{ToolArgs, ToolRef, Values};

pub async fn run(args: &PrintArgs, inputs: Values) -> Result<Values>
{
    let df = inputs.get_one().cloned()
        .ok_or_else(|| anyhow!("print tool requires input"))?;

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
        let args = ToolArgs::new(&tr.args)?;
        args.check_named_args(&["limit"])?;

        let limit = args.optional_positional_integer(0, "limit")?;

        Ok(PrintArgs { limit })
    }
}