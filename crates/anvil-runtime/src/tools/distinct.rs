use anyhow::{anyhow, Result};

use crate::tools::Values;

pub async fn run(inputs: Values) -> Result<Values>
{
    let df = inputs.get_one().cloned()
        .ok_or_else(|| anyhow!("distinct tool requires input"))?;

    let df = df.distinct()?;

    Ok(Values::new(df))
}
