use std::path::Path;

use anyhow::{anyhow, Result};
use datafusion::execution::context::SessionContext;
use datafusion::execution::options::ArrowReadOptions;
use datafusion::prelude::{AvroReadOptions, CsvReadOptions, NdJsonReadOptions, ParquetReadOptions};

use crate::tools::{Data, ToolArg, ToolArgs, ToolRef, Value};

pub async fn run(tr: &ToolRef, input: Value, ctx: &SessionContext) -> Result<Value>
{
    use InputFormat::*;

    match input {
        Value::Single(_) | Value::Multiple(_) => {
            return Err(anyhow!("input tool does not take input"));
        }
        Value::None => ()
    };

    let args: InputArgs = tr.args.as_slice().try_into()?;
    let (path, table) = (&args.path, &args.table);
    match args.format {
        csv     => ctx.register_csv(table, path, CsvReadOptions::default()).await?,
        avro    => ctx.register_avro(table, path, AvroReadOptions::default()).await?,
        json    => ctx.register_json(table, path, NdJsonReadOptions::default()).await?,
        arrow   => ctx.register_arrow(table, path, ArrowReadOptions::default()).await?,
        parquet => ctx.register_parquet(table, path, ParquetReadOptions::default()).await?,
    };

    let df = ctx.table(table).await?;

    Ok(Value::Single(Data { df, src: args.path }))
}

#[allow(non_camel_case_types)]
enum InputFormat {
    csv,
    avro,
    json,
    arrow,
    parquet
}

struct InputArgs {
    format: InputFormat,
    path: String,
    table: String,
}

impl TryFrom<&[ToolArg]> for InputArgs {
    type Error = anyhow::Error;

    fn try_from(args: &[ToolArg]) -> Result<Self>
    {
        let args = ToolArgs::new(args)?;
        args.check_named_args(&["format", "table"])?;

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
                    "avro"    => InputFormat::avro,
                    "json"    => InputFormat::json,
                    "arrow"   => InputFormat::arrow,
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

        let table = args.optional_string("table")?.unwrap_or("tbl".into());

        Ok(InputArgs { format, path, table })
    }
}