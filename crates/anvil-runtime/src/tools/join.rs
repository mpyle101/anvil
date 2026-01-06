use anyhow::{anyhow, Result};
use datafusion::prelude::JoinType;

use anvil_context::syms;
use crate::tools::{Flow, FlowRef, ToolArgs, ToolId, ToolRef, Values};

pub async fn run(id: &ToolId, args: &JoinArgs, inputs: Values) -> Result<Values>
{
    let df_lt = inputs.dfs.get(&syms().left).cloned()
        .ok_or_else(|| anyhow!("join tool ({id}) requires left port"))?;
    let df_rt = inputs.dfs.get(&syms().right).cloned()
        .ok_or_else(|| anyhow!("join tool ({id}) requires right port"))?;

    let cols_lt = args.cols_lt.split(',').collect::<Vec<_>>();
    let cols_rt = args.cols_rt.split(',').collect::<Vec<_>>();
    let df = df_lt.join(df_rt, args.join_type, &cols_lt, &cols_rt, None)?;

    Ok(Values::new(df))
}

pub fn flows(args: &JoinArgs) -> Vec<FlowRef>
{
    vec![
        FlowRef { port: syms().left,  flow: args.flow_lt.clone() },
        FlowRef { port: syms().right, flow: args.flow_rt.clone() }
    ]
}

#[derive(Debug)]
pub struct JoinArgs {
    cols_lt: String,
    cols_rt: String,
    flow_lt: Flow,
    flow_rt: Flow,
    join_type: JoinType,
}

impl TryFrom<&ToolRef> for JoinArgs {
    type Error = anyhow::Error;

    fn try_from(tr: &ToolRef) -> Result<Self>
    {
        let args = ToolArgs::new(&tr.args)?;
        args.check_named_args(&[syms().join_type, syms().cols_lt, syms().cols_rt])?;

        let flow_lt = args.required_positional_flow(0, "left")?;
        let flow_rt = args.required_positional_flow(1, "right")?;

        let cols_lt = args.optional_string(syms().cols_lt)?.ok_or_else(
            || anyhow!("join 'cols_lt' argument does not exist")
        )?;
        let cols_rt = args.optional_string(syms().cols_rt)?.ok_or_else(
            || anyhow!("join 'cols_tr' argument does not exist")
        )?;

        let join_type = args.optional_string(syms().join_type)?;
        let join_type = join_type.unwrap_or("inner".into());
        let join_type = match join_type.as_str() {
            "inner" => JoinType::Inner,
            "outer" => JoinType::Full,
            "left"  => JoinType::Left,
            "right" => JoinType::Right,
            _ => return Err(anyhow!("uknown join type '{join_type}")),
        };


        Ok(JoinArgs {
            cols_lt,
            cols_rt,
            flow_lt,
            flow_rt,
            join_type
        })
    }
}