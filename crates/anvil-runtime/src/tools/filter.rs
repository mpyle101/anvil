use anyhow::{anyhow, Result};

use crate::eval_expression;
use crate::tools::{parse_expression, ToolArgs, ToolRef, Values};

pub async fn run(args: &FilterArgs, inputs: Values) -> Result<Values>
{
    let df = inputs.get_one()
        .cloned()
        .ok_or_else(|| anyhow!("filter tool requires input"))?;

    let ast  = parse_expression(args.predicate.as_str())?;
    let expr = eval_expression(&ast)?;

    let df_true  = df.clone().filter(expr.clone())?;
    let df_false = df.filter(expr.is_not_true())?;

    let mut values = Values::default();
    values.set(df_true, "true");
    values.set(df_false, "false");

    Ok(values)
}

#[derive(Debug)]
pub struct FilterArgs {
    predicate: String,
}

impl TryFrom<&ToolRef> for FilterArgs {
    type Error = anyhow::Error;

    fn try_from(tr: &ToolRef) -> Result<Self>
    {
        let args = ToolArgs::new(&tr.args)?;
        args.check_named_args(&[])?;

        let predicate = args.required_positional_string(0, "path")?;

        Ok(FilterArgs { predicate })
    }
}