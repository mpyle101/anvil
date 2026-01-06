use std::collections::HashMap;

use anyhow::{anyhow, Result};

use anvil_context::{intern, resolve, Symbol};
use crate::tools::*;


#[derive(Debug)]
pub struct ToolArgs {
    positional: Vec<ArgValue>,
    keyword: HashMap<Symbol, ArgValue>,
}

impl ToolArgs {
    pub fn new(args: &[ToolArg]) -> Result<Self>
    {
        let mut keyword = HashMap::new();
        let mut positional = Vec::new();

        for arg in args {
            match arg {
                ToolArg::Keyword { ident, value } => {
                    if keyword.insert(*ident, value.clone()).is_some() {
                        return Err(anyhow!("duplicate named argument '{}'", resolve(*ident)));
                    }
                }
                ToolArg::Positional(v) => positional.push(v.clone()),
            }
        }

        Ok(Self { positional, keyword })
    }

    pub fn required_positional_string(&self, index: usize, name: &str) -> Result<String>
    {
        match self.positional.get(index) {
            Some(ArgValue::String(s)) => Ok(s.clone()),
            Some(_) => Err(anyhow!("'{name}' must be a string")),
            None => Err(anyhow!("missing required positional argument '{name}'")),
        }
    }

    pub fn required_positional_integer(&self, index: usize, name: &str) -> Result<i64>
    {
        match self.positional.get(index) {
            Some(ArgValue::Integer(n)) => Ok(*n),
            Some(_) => Err(anyhow!("'{name}' must be a integer")),
            None => Err(anyhow!("missing required positional argument '{name}'")),
        }
    }

    pub fn required_positional_flow(&self, index: usize, name: &str) -> Result<Flow>
    {
        match self.positional.get(index) {
            Some(av) => {
                match av {
                    ArgValue::Flow(f)   => Ok(f.clone()),
                    ArgValue::Ident(s)  => Ok(Flow { items: vec![FlowItem::Variable(intern(s))]}),
                    ArgValue::String(s) => Ok(Flow { items: vec![FlowItem::Variable(intern(s))]}),
                    _ => Err(anyhow!("'{name}' must be flow, identifier or string"))
                }
            }
            None => Err(anyhow!("missing required positional argument '{name}'")),
        }
    }

    pub fn optional_positional_string(&self, index: usize, name: &str) -> Result<Option<String>>
    {
        match self.positional.get(index) {
            Some(ArgValue::String(s)) => Ok(Some(s.clone())),
            Some(_) => Err(anyhow!("'{name}' must be a string")),
            None => Ok(None),
        }
    }

    pub fn optional_positional_integer(&self, index: usize, name: &str) -> Result<Option<i64>>
    {
        match self.positional.get(index) {
            Some(ArgValue::Integer(n)) => Ok(Some(*n)),
            Some(_) => Err(anyhow!("'{name}' must be a integer")),
            None => Ok(None),
        }
    }

    pub fn optional_string(&self, key: Symbol) -> Result<Option<String>>
    {
        match self.keyword.get(&key) {
            Some(ArgValue::String(s)) => Ok(Some(s.clone())),
            Some(_) => Err(anyhow!("{} must be a string", resolve(key))),
            None => Ok(None),
        }
    }

    pub fn optional_integer(&self, key: Symbol) -> Result<Option<i64>>
    {
        match self.keyword.get(&key) {
            Some(ArgValue::Integer(n)) => Ok(Some(*n)),
            Some(_) => Err(anyhow!("{} must be an integer", resolve(key))),
            None => Ok(None),
        }
    }

    pub fn optional_bool(&self, key: Symbol) -> Result<Option<bool>>
    {
        match self.keyword.get(&key) {
            Some(ArgValue::Boolean(b)) => Ok(Some(*b)),
            Some(_) => Err(anyhow!("{} must be a boolean", resolve(key))),
            None => Ok(None),
        }
    }

    pub fn check_named_args(&self, allowed: &[Symbol]) -> Result<()>
    {
        for key in self.keyword.keys() {
            if !allowed.contains(key) {
                return Err(anyhow!("unexpected named argument '{}'", resolve(*key)));
            }
        }

        Ok(())
    }

}

