use std::path::Path;

use anyhow::{anyhow, Result};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::logical_expr::logical_plan::dml::InsertOp;

use anvil_context::intern;
use crate::tools::{ToolArgs, ToolId, ToolRef, Values};

pub async fn run(id: &ToolId, args: &OutputArgs, inputs: Values) -> Result<Values>
{
    use OutputFormat::*;

    let df = inputs.get_one().cloned()
        .ok_or_else(|| anyhow!("output tool ({id}) requires input"))?;

    let options = DataFrameWriteOptions::new()
        .with_insert_operation(args.mode)
        .with_single_file_output(args.single);

    match args.format {
        csv     => df.write_csv(&args.path, options, None).await?,
        json    => df.write_json(&args.path, options, None).await?,
        parquet => df.write_parquet(&args.path, options, None).await?,
    };

    Ok(Values::default())
}


#[allow(non_camel_case_types)]
#[derive(Debug)]
enum OutputFormat {
    csv,
    json,
    parquet
}

#[derive(Debug)]
pub struct OutputArgs {
    format: OutputFormat,
    mode: InsertOp,
    path: String,
    single: bool,
}

impl TryFrom<&ToolRef> for OutputArgs {
    type Error = anyhow::Error;

    fn try_from(tr: &ToolRef) -> Result<Self>
    {
        let args = ToolArgs::new(&tr.args)?;
        args.check_named_args(&[intern("format")])?;

        let path   = args.required_positional_string(0, "path")?;
        let fpath  = Path::new(&path);
        let single = args.optional_bool(intern("single"))?.unwrap_or(true);

        let format = args.optional_string(intern("format"))?;
        let format = match format {
            Some(s) => {
                match s.as_str() {
                    "csv"     => OutputFormat::csv,
                    "json"    => OutputFormat::json,
                    "parquet" => OutputFormat::parquet,
                    _ => {
                        return Err(anyhow!("input file format unsupported {s}"))
                    }
                }
            }
            None => {
                if let Some(s) = fpath.extension() {
                    match s.to_str() {
                        Some("csv")  => OutputFormat::csv,
                        Some("json") => OutputFormat::json,
                        _            => OutputFormat::parquet
                    }
                } else {
                    OutputFormat::parquet
                }
            }
        };

        let mode = args.optional_string(intern("mode"))?.unwrap_or_else(|| "append".into());
        let mode = match mode.as_str() {
            "append"    => InsertOp::Append,
            "overwrite" => InsertOp::Overwrite,
            "replace"   => InsertOp::Replace,
            _ => {
                return Err(anyhow!("mode must be 'append', 'overwrite' or 'replace': {mode}"))
            }
        };

        Ok(OutputArgs { format, mode, path, single })
    }
}