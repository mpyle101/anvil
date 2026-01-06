use anyhow::{anyhow, Result};

use anvil_context::syms;
use crate::tools::{Flow, FlowRef, ToolArgs, ToolId, ToolRef, Values};

pub async fn run(id: &ToolId, inputs: Values) -> Result<Values>
{
    let df_lt = inputs.dfs.get(&syms().left).cloned()
        .ok_or_else(|| anyhow!("union tool ({id}) requires left port"))?;
    let df_rt = inputs.dfs.get(&syms().right).cloned()
        .ok_or_else(|| anyhow!("union tool ({id}) requires right port"))?;

    let df = df_lt.union(df_rt)?;

    Ok(Values::new(df))
}

pub fn flows(args: &UnionArgs) -> Vec<FlowRef>
{
    vec![
        FlowRef { port: syms().left,  flow: args.flow_lt.clone() },
        FlowRef { port: syms().right, flow: args.flow_rt.clone() }
    ]
}

#[derive(Debug)]
pub struct UnionArgs {
    flow_lt: Flow,
    flow_rt: Flow,
}

impl TryFrom<&ToolRef> for UnionArgs {
    type Error = anyhow::Error;

    fn try_from(tr: &ToolRef) -> Result<Self>
    {
        let args = ToolArgs::new(&tr.args)?;
        let flow_lt = args.required_positional_flow(0, "left")?;
        let flow_rt = args.required_positional_flow(1, "right")?;

        Ok(UnionArgs { flow_lt, flow_rt })
    }
}