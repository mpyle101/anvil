use std::path::Path;

use anyhow::{anyhow, Result};
use datafusion::execution::context::SessionContext;
use datafusion::execution::options::ArrowReadOptions;
use datafusion::prelude::{AvroReadOptions, CsvReadOptions, NdJsonReadOptions, ParquetReadOptions};

use anvil_context::intern;
use crate::tools::{ToolArgs, ToolId, ToolRef, Values};

pub async fn run(args: &InputArgs, ctx: &SessionContext) -> Result<Values>
{
    use InputFormat::*;

    let df = match args.format {
        csv     => ctx.read_csv(&args.path, CsvReadOptions::default()).await?,
        avro    => ctx.read_avro(&args.path, AvroReadOptions::default()).await?,
        json    => ctx.read_json(&args.path, NdJsonReadOptions::default()).await?,
        arrow   => ctx.read_arrow(&args.path, ArrowReadOptions::default()).await?,
        parquet => ctx.read_parquet(&args.path, ParquetReadOptions::default()).await?,
    };

    Ok(Values::new(df))
}

#[derive(Debug)]
#[allow(non_camel_case_types)]
enum InputFormat {
    csv,
    avro,
    json,
    arrow,
    parquet
}

#[derive(Debug)]
pub struct InputArgs {
    pub id: ToolId,
    format: InputFormat,
    path: String,
}

impl TryFrom<&ToolRef> for InputArgs {
    type Error = anyhow::Error;

    fn try_from(tr: &ToolRef) -> Result<Self>
    {
        let args = ToolArgs::new(&tr.args)?;
        args.check_named_args(&[intern("format")])?;

        let path = args.required_positional_string(0, "input: path")?;
        let fpath = Path::new(&path);
        if !fpath.exists() {
            return Err(anyhow!("input file not found: {}", fpath.display()));
        }

        let format = args.optional_string(intern("format"))?;
        let format = match format {
            Some(s) => {
                match s.as_str() {
                    "csv"     => InputFormat::csv,
                    "avro"    => InputFormat::avro,
                    "json"    => InputFormat::json,
                    "arrow"   => InputFormat::arrow,
                    "parquet" => InputFormat::parquet,
                    _ => {
                        return Err(anyhow!("unsupported input file format {s}"))
                    }
                }
            }
            None => {
                if let Some(s) = fpath.extension() {
                    match s.to_str() {
                        Some("csv")  => InputFormat::csv,
                        Some("avro") => InputFormat::avro,
                        Some("json") => InputFormat::json,
                        _            => InputFormat::parquet
                    }
                } else {
                    InputFormat::parquet
                }
            }
        };

        Ok(InputArgs { id: tr.id, format, path })
    }
}