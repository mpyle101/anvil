use std::collections::HashMap;

use anyhow::{anyhow, Result};
use datafusion::execution::context::SessionContext;

use crate::{Literal, ToolArg};

use crate::{
    InputTool,
    OutputTool,
    FilterTool,
    JoinTool,
    ShowTool,
    Value
};

pub enum Tool {
    Input,
    Output,
    Filter,
    Join,
    Show,
}

impl Tool {
    pub fn dispatch(name: &str) -> Result<Tool>
    {
        let tool = match name {
            "input"  => Tool::Input,
            "output" => Tool::Output,
            "filter" => Tool::Filter,
            "join"   => Tool::Join,
            "show"   => Tool::Show,
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
            Tool::Input  => InputTool::run(input, args, ctx).await,
            Tool::Output => OutputTool::run(input, args).await,
            Tool::Filter => FilterTool::run(input, args).await,
            Tool::Join   => JoinTool::run(input, args).await,
            Tool::Show   => ShowTool::run(input).await,
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

    pub fn optional_string(&self, key: &str) -> Result<Option<String>>
    {
        match self.keyword.get(key) {
            Some(Literal::String(s)) => Ok(Some(s.clone())),
            Some(_) => Err(anyhow!("{key} must be a string")),
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