use std::path::Path;

use anyhow::{anyhow, Result};
use datafusion::execution::context::SessionContext;
use datafusion::prelude::{CsvReadOptions, NdJsonReadOptions, ParquetReadOptions};

use crate::{Data, ToolArg, ToolArgs, Value};


pub struct InputTool;

impl InputTool {
    pub async fn run(
        input: Value,
        args: &[ToolArg],
        ctx: &SessionContext,
    ) -> Result<Value>
    {
        use InputFormat::*;

        match input {
            Value::Single(_) | Value::Multiple(_) => {
                return Err(anyhow!("input tool does not take input"));
            }
            Value::None => ()
        };

        let args: InputArgs = args.try_into()?;
        let df = match args.format {
            csv     => ctx.read_csv(&args.path, CsvReadOptions::default()).await?,
            json    => ctx.read_json(&args.path, NdJsonReadOptions::default()).await?,
            parquet => ctx.read_parquet(&args.path, ParquetReadOptions::default()).await?,
        };

        Ok(Value::Single(Data { df, src: args.path }))
    }
}

#[allow(non_camel_case_types)]
enum InputFormat {
    csv,
    json,
    parquet
}

struct InputArgs {
    format: InputFormat,
    path: String,
}

impl TryFrom<&[ToolArg]> for InputArgs {
    type Error = anyhow::Error;

    fn try_from(args: &[ToolArg]) -> Result<Self>
    {
        let args = ToolArgs::new(args)?;
        args.check_named_args(&["format"])?;

        let path = args.require_positional_string(0, "input: path")?;
        let fpath = Path::new(&path);
        if !fpath.exists() {
            return Err(anyhow!("input file not found: {}", fpath.display()));
        }

        let format = args.optional_string("format")?;
        let format = match format {
            Some(s) => {
                match s.as_str() {
                    "csv"     => InputFormat::csv,
                    "json"    => InputFormat::json,
                    "parquet" => InputFormat::parquet,
                    _ => {
                        return Err(anyhow!("input file format unsupported {s}"))
                    }
                }
            }
            None => {
                if let Some(s) = fpath.extension() {
                    match s.to_str() {
                        Some("csv")  => InputFormat::csv,
                        Some("json") => InputFormat::json,
                        _            => InputFormat::parquet
                    }
                } else {
                    InputFormat::parquet
                }
            }
        };

        Ok(InputArgs { format, path })
    }
}