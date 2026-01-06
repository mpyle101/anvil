use anyhow::{anyhow, Result};
use datafusion::scalar::ScalarValue;

use crate::tools::{ToolArgs, ToolId, ToolRef, Values};

pub async fn run(id: &ToolId, args: &FillArgs, inputs: Values) -> Result<Values>
{
    let df = inputs.get_one().cloned()
        .ok_or_else(|| anyhow!("fill tool ({id}) requires input"))?;

    let cols = args.cols.as_ref()
        .map(|s| s.split(',').map(|c| c.to_owned()).collect())
        .unwrap_or_default();
    let df = df.fill_null(ScalarValue::from(args.value), cols)?;

    Ok(Values::new(df))
}

#[derive(Debug)]
pub struct FillArgs {
    value: i64,
    cols: Option<String>,
}

impl TryFrom<&ToolRef> for FillArgs {
    type Error = anyhow::Error;

    fn try_from(tr: &ToolRef) -> Result<Self>
    {
        let args = ToolArgs::new(&tr.args)?;
        args.check_named_args(&[])?;

        let value = args.required_positional_integer(0, "value")?;
        let cols  = args.optional_positional_string(1, "cols")?;

        Ok(FillArgs { value, cols })
    }
}