#![allow(dead_code)]

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use datafusion::execution::context::SessionContext;

use crate::tools::*;

pub async fn run(
    name: &str,
    input: Value,
    args: &[ToolArg],
    ctx: &SessionContext,
) -> anyhow::Result<Value>
{
    match name {
        "count"     => count::run(input, args, ctx).await,
        "describe"  => describe::run(input, args).await,
        "distinct"  => distinct::run(input, args).await,
        "drop"      => drop::run(input, args).await,
        "fill"      => fill::run(input, args).await,
        "filter"    => filter::run(input, args).await,
        "input"     => input::run(input, args, ctx).await,
        "intersect" => intersect::run(input, args).await,
        "join"      => join::run(input, args).await,
        "limit"     => limit::run(input, args).await,
        "output"    => output::run(input, args).await,
        "print"     => print::run(input, args).await,
        "schema"    => schema::run(input, args).await,
        "select"    => select::run(input, args).await,
        "sort"      => sort::run(input, args).await,
        "union"     => union::run(input, args).await,
        _ => Err(anyhow!("Unknown tool encountered: {name}"))
    }
}


pub struct ToolArgs {
    positional: Vec<Literal>,
    keyword: HashMap<String, Literal>,
}

impl ToolArgs {
    pub fn new(args: &[ToolArg]) -> Result<Self>
    {
        let mut positional = Vec::new();
        let mut keyword = HashMap::new();

        for arg in args {
            match arg {
                ToolArg::Positional(lit) => positional.push(lit.clone()),
                ToolArg::Keyword { key, value } => {
                    if keyword.insert(key.clone(), value.clone()).is_some() {
                        return Err(anyhow!("duplicate argument '{}'", key));
                    }
                }
            }
        }

        Ok(Self { positional, keyword })
    }

    pub fn require_positional_string(&self, index: usize, name: &str) -> Result<String>
    {
        match self.positional.get(index) {
            Some(Literal::String(s)) => Ok(s.clone()),
            Some(_) => Err(anyhow!("'{name}' must be a string")),
            None => Err(anyhow!("missing required positional argument '{name}'")),
        }
    }

    pub fn require_positional_integer(&self, index: usize, name: &str) -> Result<i64>
    {
        match self.positional.get(index) {
            Some(Literal::Integer(n)) => Ok(*n),
            Some(_) => Err(anyhow!("'{name}' must be a integer")),
            None => Err(anyhow!("missing required positional argument '{name}'")),
        }
    }

    pub fn optional_positional_string(&self, index: usize, name: &str) -> Result<Option<String>>
    {
        match self.positional.get(index) {
            Some(Literal::String(s)) => Ok(Some(s.clone())),
            Some(_) => Err(anyhow!("'{name}' must be a string")),
            None => Ok(None),
        }
    }

    pub fn optional_positional_integer(&self, index: usize, name: &str) -> Result<Option<i64>>
    {
        match self.positional.get(index) {
            Some(Literal::Integer(n)) => Ok(Some(*n)),
            Some(_) => Err(anyhow!("'{name}' must be a integer")),
            None => Ok(None),
        }
    }

    pub fn optional_string(&self, key: &str) -> Result<Option<String>>
    {
        match self.keyword.get(key) {
            Some(Literal::String(s)) => Ok(Some(s.clone())),
            Some(_) => Err(anyhow!("{key} must be a string")),
            None => Ok(None),
        }
    }

    pub fn optional_integer(&self, key: &str) -> Result<Option<i64>>
    {
        match self.keyword.get(key) {
            Some(Literal::Integer(n)) => Ok(Some(*n)),
            Some(_) => Err(anyhow!("{key} must be an integer")),
            None => Ok(None),
        }
    }

    pub fn optional_bool(&self, key: &str) -> Result<Option<bool>>
    {
        match self.keyword.get(key) {
            Some(Literal::Boolean(b)) => Ok(Some(*b)),
            Some(_) => Err(anyhow!("{key} must be a boolean")),
            None => Ok(None),
        }
    }

    pub fn check_named_args(&self, allowed: &[&str]) -> Result<()> {
        for key in self.keyword.keys() {
            if !allowed.contains(&key.as_str()) {
                return Err(anyhow!("unexpected named argument '{key}'"));
            }
        }
        Ok(())
    }

}