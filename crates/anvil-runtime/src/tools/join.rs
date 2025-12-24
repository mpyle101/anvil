use anyhow::{anyhow, Result};
use datafusion::prelude::JoinType;

use crate::tools::{Data, ToolArg, ToolArgs, Value};


pub struct JoinTool;

impl JoinTool {
    pub async fn run(
        input: Value,
        args: &[ToolArg]
    ) -> Result<Value>
    {
        let data = match input {
            Value::Multiple(data) => data,
            _ => return Err(anyhow!("join requires single input")),
        };
        if data.len() != 2 {
            return Err(anyhow!("join requires two data sets: (left, right)"))
        }
        let df_left  = data[0].df.clone();
        let df_right = data[1].df.clone();

        let args: JoinArgs = args.try_into()?;
        let cols_left  = args.left.split(',').collect::<Vec<_>>();
        let cols_right = args.right.split(',').collect::<Vec<_>>();

        let df = df_left.join(df_right, args.join_type, &cols_left, &cols_right, None)?;

        Ok(Value::Single(Data { df, src: "join tool".into() }))
    }
}

struct JoinArgs {
    left: String,
    right: String,
    join_type: JoinType,
}

impl TryFrom<&[ToolArg]> for JoinArgs {
    type Error = anyhow::Error;

    fn try_from(args: &[ToolArg]) -> Result<Self>
    {
        let args = ToolArgs::new(args)?;
        args.check_named_args(&["type", "left", "right"])?;

        let left = args.optional_string("left")?.ok_or(
            anyhow!("join 'left' columns argument does not exist")
        )?;
        let right = args.optional_string("right")?.ok_or(
            anyhow!("join 'left' columns argument does not exist")
        )?;

        let join_type = args.optional_string("type")?;
        let join_type = join_type.unwrap_or("inner".into());
        let join_type = match join_type.as_str() {
            "inner" => JoinType::Inner,
            "outer" => JoinType::Full,
            "left"  => JoinType::Left,
            "right" => JoinType::Right,
            _ => {
                return Err(anyhow!("uknown join type '{join_type}"))
            }
        };


        Ok(JoinArgs { left, right, join_type })
    }
}