use anyhow::{anyhow, Result};

use crate::tools::{ToolId, Values};

pub async fn run(id: &ToolId, inputs: Values) -> Result<Values>
{
    let df = inputs.get_one().cloned()
        .ok_or_else(|| anyhow!("describe tool ({id}) requires input"))?;

    let df = df.describe().await?;

    Ok(Values::new(df))
}
