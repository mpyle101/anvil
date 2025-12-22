use std::path::Path;

use anyhow::anyhow;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::{CsvReadOptions, NdJsonReadOptions, ParquetReadOptions};

use anvil_parse::ast::ToolArg;
use crate::{ToolArgs, Value};

pub struct InputTool;

impl InputTool {
    pub async fn run(
        input: Value,
        args: &[ToolArg],
        ctx: &SessionContext,
    ) -> anyhow::Result<Value>
    {
        if let Value::Single(_) = input {
            return Err(anyhow!("input tool does not take input"));
        }

        let args = ToolArgs::new(args)?;
        args.check_named_args(&["format"])?;

        let path = args.require_positional_string(0, "path")?;
        if !Path::new(&path).exists() {
            return Err(anyhow!("File not found: {path}"));
        }

        let format = args.optional_string("format")?;
        let format = format.unwrap_or_else(|| {
            if path.ends_with(".csv") { "csv" }
            else if path.ends_with(".json") { "json" }
            else if path.ends_with(".avro") { "avro" }
            else { "parquet" }
            .to_string()
        });

        let df = match format.as_str() {
            "csv"     => ctx.read_csv(&path, CsvReadOptions::new()).await?,
            "json"    => ctx.read_json(&path, NdJsonReadOptions::default()).await?,
            "parquet" => ctx.read_parquet(&path, ParquetReadOptions::default()).await?,
            _ => return Err(anyhow!("unsupported input format '{format}'")),
        };

        Ok(Value::Single(df))
    }
}
