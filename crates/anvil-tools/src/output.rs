use anyhow::anyhow;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::logical_expr::logical_plan::dml::InsertOp;

use anvil_parse::ast::ToolArg;
use crate::{ToolArgs, Value};


pub struct OutputTool;

impl OutputTool {
    pub async fn run(
        input: Value,
        args: &[ToolArg],
    ) -> anyhow::Result<Value>
    {
        let df = match input {
            Value::Single(df) => df,
            Value::None => {
                return Err(anyhow!("previous tool does not produce output"))
            }
            Value::Multiple(_) => {
                return Err(anyhow!("output does not accept multiple values"))
            }
        };

        let args = ToolArgs::new(args)?;
        args.check_named_args(&["format", "mode", "single"])?;

        let path = args.require_positional_string(0, "path")?;

        let format = args.optional_string("format")?;
        let format = format.unwrap_or_else(|| {
            if path.ends_with(".csv") { "csv" }
            else if path.ends_with(".json") { "json" }
            else if path.ends_with(".avro") { "avro" }
            else { "parquet" }
            .to_string()
        });

        let mode = args.optional_string("mode")?.unwrap_or_else(|| "append".into());
        let options = DataFrameWriteOptions::new();
        let options = match mode.as_str() {
            "append"    => options.with_insert_operation(InsertOp::Append),
            "overwrite" => options.with_insert_operation(InsertOp::Overwrite),
            "replace"   => options.with_insert_operation(InsertOp::Replace),
            _ => {
                return Err(anyhow!("mode must be 'append', 'overwrite' or 'replace': {mode}"))
            }
        };

        let single = args.optional_bool("single")?.unwrap_or(true);
        let options = options.with_single_file_output(single);

        match format.as_str() {
            "csv"     => df.write_csv(&path, options, None).await?,
            "json"    => df.write_json(&path, options, None).await?,
            "parquet" => df.write_parquet(&path, options, None).await?,
            _ => return Err(anyhow!("unsupported output format '{}'", format)),
        };

        Ok(Value::None)
    }
}
