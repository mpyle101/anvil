use std::path::Path;

use anyhow::{anyhow, Result};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::logical_expr::logical_plan::dml::InsertOp;

use crate::{Data, ToolArg, ToolArgs, Value};


pub struct OutputTool;

impl OutputTool {
    pub async fn run(
        input: Value,
        args: &[ToolArg],
    ) -> Result<Value>
    {
        use OutputFormat::*;

        let df = match input {
            Value::Single(Data {df, ..}) => df,
            Value::None => {
                return Err(anyhow!("previous tool does not produce output"))
            }
            Value::Multiple(_) => {
                return Err(anyhow!("output does not accept multiple values"))
            }
        };

        let args: OutputArgs = args.try_into()?;
        let options = DataFrameWriteOptions::new()
            .with_insert_operation(args.mode)
            .with_single_file_output(args.single);

        match args.format {
            csv     => df.write_csv(&args.path, options, None).await?,
            json    => df.write_json(&args.path, options, None).await?,
            parquet => df.write_parquet(&args.path, options, None).await?,
        };

        Ok(Value::None)
    }
}


#[allow(non_camel_case_types)]
enum OutputFormat {
    csv,
    json,
    parquet
}

struct OutputArgs {
    format: OutputFormat,
    mode: InsertOp,
    path: String,
    single: bool,
}

impl TryFrom<&[ToolArg]> for OutputArgs {
    type Error = anyhow::Error;

    fn try_from(args: &[ToolArg]) -> Result<Self>
    {
        let args = ToolArgs::new(args)?;
        args.check_named_args(&["format"])?;

        let path   = args.require_positional_string(0, "path")?;
        let fpath  = Path::new(&path);
        let single = args.optional_bool("single")?.unwrap_or(true);

        let format = args.optional_string("format")?;
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

        let mode = args.optional_string("mode")?.unwrap_or_else(|| "append".into());
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