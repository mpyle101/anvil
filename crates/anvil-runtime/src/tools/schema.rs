use std::sync::Arc;

use anyhow::{anyhow, Result};
use datafusion::prelude::*;
use datafusion::common::arrow::array::{BooleanArray, UInt64Array, StringArray};

use crate::tools::{ToolId, ToolRef, Values};

pub async fn run(args: &SchemaArgs, inputs: Values) -> Result<Values>
{
    let df = inputs.get_one().cloned()
        .ok_or_else(|| anyhow!("schema tool ({}) requires input", args.id))?;

    let mut names = vec![];
    let mut sizes = vec![];
    let mut types = vec![];
    let mut nulls = vec![];

    for field in df.schema().fields() {
        names.push(field.name().as_str());
        sizes.push(field.size() as u64);
        types.push(field.data_type().to_string());
        nulls.push(field.is_nullable());
    }

    let df = DataFrame::from_columns(vec![
        ("name", Arc::new(StringArray::from(names))),
        ("size", Arc::new(UInt64Array::from(sizes))),
        ("type", Arc::new(StringArray::from(types))),
        ("nullable", Arc::new(BooleanArray::from(nulls))),
    ])?;

    Ok(Values::new(df))
}

#[derive(Debug)]
pub struct SchemaArgs {
    pub id: ToolId,
}

impl TryFrom<&ToolRef> for SchemaArgs {
    type Error = anyhow::Error;

    fn try_from(tr: &ToolRef) -> Result<Self>
    {
        Ok(SchemaArgs { id: tr.id, })
    }
}