use anyhow::{anyhow, Result};

use anvil_context::intern;
use crate::tools::{ToolArgs, ToolId, ToolRef, Values};

pub async fn run(args: &LimitArgs, inputs: Values) -> Result<Values>
{
    let df = inputs.get_one().cloned()
        .ok_or_else(|| anyhow!("limit tool ({}) requires input", args.id))?;
    let df = df.limit(args.skip, Some(args.count))?;

    Ok(Values::new(df))
}

#[derive(Debug)]
pub struct LimitArgs {
    pub id: ToolId,
    count: usize,
    skip: usize,
}

impl TryFrom<&ToolRef> for LimitArgs {
    type Error = anyhow::Error;

    fn try_from(tr: &ToolRef) -> Result<Self>
    {
        let args = ToolArgs::new(&tr.args)?;
        args.check_named_args(&[intern("skip")])?;

        let count = args.required_positional_integer(0, "count")? as usize;
        let skip  = args.optional_integer(intern("skip"))?.unwrap_or(0) as usize;

        Ok(LimitArgs { id: tr.id, count, skip })
    }
}