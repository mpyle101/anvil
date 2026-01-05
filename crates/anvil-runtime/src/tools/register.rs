use std::path::Path;

use anyhow::{anyhow, Result};
use datafusion::execution::context::SessionContext;
use datafusion::execution::options::ArrowReadOptions;
use datafusion::prelude::{AvroReadOptions, CsvReadOptions, NdJsonReadOptions, ParquetReadOptions};

use anvil_context::intern;
use crate::tools::{ToolArgs, ToolRef, Values};

pub async fn run(args: &RegisterArgs, ctx: &SessionContext) -> Result<Values>
{
    use Format::*;

    let (path, table) = (&args.path, &args.table);
    match args.format {
        csv     => ctx.register_csv(table, path, CsvReadOptions::default()).await?,
        avro    => ctx.register_avro(table, path, AvroReadOptions::default()).await?,
        json    => ctx.register_json(table, path, NdJsonReadOptions::default()).await?,
        arrow   => ctx.register_arrow(table, path, ArrowReadOptions::default()).await?,
        parquet => ctx.register_parquet(table, path, ParquetReadOptions::default()).await?,
    };

    let df = ctx.table(table).await?;

    Ok(Values::new(df))
}

#[derive(Debug)]
#[allow(non_camel_case_types)]
enum Format {
    csv,
    avro,
    json,
    arrow,
    parquet
}

#[derive(Debug)]
pub struct RegisterArgs {
    format: Format,
    path: String,
    table: String,
}

impl TryFrom<&ToolRef> for RegisterArgs {
    type Error = anyhow::Error;

    fn try_from(tr: &ToolRef) -> Result<Self>
    {
        let args = ToolArgs::new(&tr.args)?;
        args.check_named_args(&[intern("format"), intern("table")])?;

        let path = args.required_positional_string(0, "input: path")?;
        let fpath = Path::new(&path);
        if !fpath.exists() {
            return Err(anyhow!("input file not found: {}", fpath.display()));
        }

        let format = args.optional_string(intern("format"))?;
        let format = match format {
            Some(s) => {
                match s.as_str() {
                    "csv"     => Format::csv,
                    "avro"    => Format::avro,
                    "json"    => Format::json,
                    "arrow"   => Format::arrow,
                    "parquet" => Format::parquet,
                    _ => {
                        return Err(anyhow!("input file format unsupported {s}"))
                    }
                }
            }
            None => {
                if let Some(s) = fpath.extension() {
                    match s.to_str() {
                        Some("csv")  => Format::csv,
                        Some("json") => Format::json,
                        _            => Format::parquet
                    }
                } else {
                    Format::parquet
                }
            }
        };

        let table = args.optional_string(intern("table"))?
            .unwrap_or("tbl".into());

        Ok(RegisterArgs { format, path, table })
    }
}