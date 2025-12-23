use std::collections::HashMap;
use std::convert::TryFrom;

use anyhow::{anyhow, Result};
use datafusion::prelude::JoinType;

use anvil_parse::ast::ToolArg;
use crate::{Data, ToolArgs, Value};

type Variables = HashMap<String, Value>;

pub struct JoinTool;

impl JoinTool {
    pub async fn run(
        input: Value,
        args: &[ToolArg],
        vars: &Variables
    ) -> Result<Value>
    {
        let Data { df, .. } = match input {
            Value::Single(data) => data,
            _ => return Err(anyhow!("join requires single input")),
        };
        let df_left = df;

        let args: JoinArgs = args.try_into()?;

        let var = vars.get(&args.df).ok_or_else(
            || anyhow!("right join variable does not exist '{}'", args.df)
        )?.clone();

        let Data { df:df_right, .. } = match var {
            Value::Single(data) => data,
            Value::Multiple(_) => {
                return Err(anyhow!("join only takes a single right side input"))
            }
            Value::None => {
                return Err(anyhow!("join require right side input"))
            }
        } ;
        let cols_left  = args.left.split(',').collect::<Vec<_>>();
        let cols_right = args.right.split(',').collect::<Vec<_>>();

        let df = df_left.join(df_right, args.join_type, &cols_left, &cols_right, None)?;

        Ok(Value::Single(Data { df, src: "join tool".into() }))
    }
}

struct JoinArgs {
    df: String,
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

        let df = args.require_positional_string(0, "df")?;

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


        Ok(JoinArgs { df, left, right, join_type })
    }
}