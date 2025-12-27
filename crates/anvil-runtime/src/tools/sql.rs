use anyhow::{anyhow, Result};
use datafusion::prelude::{DataFrame, Expr, SessionContext};

use crate::tools::{Data, ToolArg, ToolArgs, ToolRef, Value};

pub async fn run(tr: &ToolRef, input: Value, ctx: &SessionContext) -> Result<Value>
{
    let df = match input {
        Value::Single(data) => {
            let exprs = collect_exprs(&data.df, tr)?;
            data.df.select(exprs)?
        },
        Value::None => {
            let args: SqlArgs = tr.args.as_slice().try_into()?;
            ctx.sql(&args.sql).await?
        }
        _ => return Err(anyhow!("sql tool requires single or no inputs")),
    };

    Ok(Value::Single(Data { df, src: format!("sql ({})", tr.id) }))
}

fn collect_exprs(df: &DataFrame, tr: &ToolRef) -> Result<Vec<Expr>>
{
    let mut exprs = Vec::new();
    for arg in &tr.args {
        match arg {
            ToolArg::Positional(_) => {}
            ToolArg::Keyword { key, value } => {
                let expr  = df.parse_sql_expr(value.to_string().as_str())?;
                exprs.push(expr.alias(key));
            }
        }
    }

    Ok(exprs)
}

struct SqlArgs {
    sql: String,
}

impl TryFrom<&[ToolArg]> for SqlArgs {
    type Error = anyhow::Error;

    fn try_from(args: &[ToolArg]) -> Result<Self>
    {
        let args = ToolArgs::new(args)?;
        args.check_named_args(&[])?;

        let sql = args.require_positional_string(0, "sql")?;

        Ok(SqlArgs { sql })
    }
}