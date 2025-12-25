#![allow(dead_code)]

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use datafusion::execution::context::SessionContext;

use crate::tools::{Literal, ToolArg};

use crate::tools::*;

pub enum Tool {
    Distinct,
    Filter,
    Input,
    Intersect,
    Join,
    Limit,
    Output,
    Print,
    Union,
}

impl Tool {
    pub fn dispatch(name: &str) -> Result<Tool>
    {
        let tool = match name {
            "distinct"  => Tool::Distinct,
            "filter"    => Tool::Filter,
            "input"     => Tool::Input,
            "intersect" => Tool::Intersect,
            "join"      => Tool::Join,
            "limit"     => Tool::Limit,
            "output"    => Tool::Output,
            "print"     => Tool::Print,
            _ => return Err(anyhow!("Unknown tool encountered: {name}"))
        };

        Ok(tool)
    }

    pub async fn run(
        &self,
        input: Value,
        args: &[ToolArg],
        ctx: &SessionContext,
        _vars: &HashMap<String, Value>
    ) -> anyhow::Result<Value>
    {
        match self {
            Tool::Distinct  => DistinctTool::run(input, args).await,
            Tool::Filter    => FilterTool::run(input, args).await,
            Tool::Input     => InputTool::run(input, args, ctx).await,
            Tool::Intersect => IntersectTool::run(input, args).await,
            Tool::Join      => JoinTool::run(input, args).await,
            Tool::Limit     => LimitTool::run(input, args).await,
            Tool::Output    => OutputTool::run(input, args).await,
            Tool::Print     => PrintTool::run(input, args).await,
            Tool::Union     => UnionTool::run(input, args).await,
        }
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