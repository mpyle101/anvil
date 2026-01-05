use anyhow::{anyhow, Result};

use anvil_context::syms;
use crate::tools::{Flow, FlowRef, ToolArgs, ToolId, ToolRef, Values};

pub async fn run(args: &IntersectArgs, inputs: Values) -> Result<Values>
{
    let df_lt = inputs.dfs.get(&syms().left).cloned()
        .ok_or_else(|| anyhow!("intersect tool ({}) requires left port", args.id))?;
    let df_rt = inputs.dfs.get(&syms().right).cloned()
        .ok_or_else(|| anyhow!("intersect tool ({}) requires right port", args.id))?;

    let df = df_lt.intersect(df_rt)?;

    Ok(Values::new(df))
}

pub fn flows(args: &IntersectArgs) -> Vec<FlowRef>
{
    vec![
        FlowRef { port: syms().left,  flow: args.flow_lt.clone() },
        FlowRef { port: syms().right, flow: args.flow_rt.clone() }
    ]
}

#[derive(Debug)]
pub struct IntersectArgs {
    pub id: ToolId,
    flow_lt: Flow,
    flow_rt: Flow,
}

impl TryFrom<&ToolRef> for IntersectArgs {
    type Error = anyhow::Error;

    fn try_from(tr: &ToolRef) -> Result<Self>
    {
        let args = ToolArgs::new(&tr.args)?;
        let flow_lt = args.required_positional_flow(0, "left")?;
        let flow_rt = args.required_positional_flow(1, "right")?;

        Ok(IntersectArgs { id: tr.id, flow_lt, flow_rt })
    }
}