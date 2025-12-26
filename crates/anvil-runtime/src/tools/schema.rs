use std::sync::Arc;

use anyhow::{anyhow, Result};
use datafusion::prelude::*;
use datafusion::common::arrow::array::{BooleanArray, UInt64Array, StringArray};

use crate::tools::{Data, ToolRef, Value};

pub async fn run(tr: &ToolRef, input: Value) -> Result<Value>
{
    let Data { df, .. } = match input {
        Value::Single(data) => data,
        _ => return Err(anyhow!("schema requires single input")),
    };

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

    Ok(Value::Single(Data { df, src: format!("schema ({})", tr.id) }))
}
