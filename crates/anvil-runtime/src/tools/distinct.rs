use anyhow::{anyhow, Result};

use crate::tools::{ToolId, Values};

pub async fn run(id: &ToolId, inputs: Values) -> Result<Values>
{
    let df = inputs.get_one().cloned()
        .ok_or_else(|| anyhow!("distinct tool ({id}) requires input"))?;

    let df = df.distinct()?;

    Ok(Values::new(df))
}
