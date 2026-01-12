use anyhow::{anyhow, Result};

use crate::tools::{ArgValue, ToolArgs, ToolId, ToolRef, Values};

pub async fn run(id: &ToolId, args: &DropArgs, inputs: Values) -> Result<Values>
{
    let df = inputs.get_one().cloned()
        .ok_or_else(|| anyhow!("drop tool ({id}) requires input"))?;

    let cols = args.cols.iter()
        .map(|s| s.as_str())
        .collect::<Vec<_>>();
    let df = df.drop_columns(&cols)?;

    Ok(Values::new(df))
}

#[derive(Debug)]
pub struct DropArgs {
    cols: Vec<String>,
}

impl TryFrom<&ToolRef> for DropArgs {
    type Error = anyhow::Error;

    fn try_from(tr: &ToolRef) -> Result<Self>
    {
        let args = ToolArgs::new(&tr.args)?;
        args.check_named_args(&[])?;

        let mut cols = vec![];
        for v in args.positional {
            match v {
                ArgValue::Ident(s)  => cols.push(s),
                ArgValue::String(s) => cols.push(s),
                _ => return Err(anyhow!("drop columns must be a string or identifier: {v:?}"))
            }
        }

        Ok(DropArgs { cols })
    }
}