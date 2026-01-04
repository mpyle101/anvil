use std::collections::HashMap;

use anyhow::{anyhow, Result};
use datafusion::prelude::{DataFrame, SessionContext};

use crate::tools::*;


#[derive(Debug)]
pub enum Tool {
    Count(count::CountArgs),
    Describe,
    Distinct,
    Drop(drop::DropArgs),
    Fill(fill::FillArgs),
    Filter(filter::FilterArgs),
    Input(input::InputArgs),
    Intersect(intersect::IntersectArgs),
    Join(join::JoinArgs),
    Limit(limit::LimitArgs),
    Output(output::OutputArgs),
    Print(print::PrintArgs),
    Project(project::ProjectArgs),
    Register(register::RegisterArgs),
    Schema,
    Select(select::SelectArgs),
    Sort(sort::SortArgs),
    Sql(sql::SqlArgs),
    Union(union::UnionArgs),
}

impl TryFrom<&ToolRef> for Tool {
    type Error = anyhow::Error;

    fn try_from(tr: &ToolRef) -> Result<Self>
    {
        let name = tr.name.as_str();
        let tool = match name {
            "count"     => Tool::Count(tr.try_into()?),
            "describe"  => Tool::Describe,
            "distinct"  => Tool::Distinct,
            "drop"      => Tool::Drop(tr.try_into()?),
            "fill"      => Tool::Fill(tr.try_into()?),
            "filter"    => Tool::Filter(tr.try_into()?),
            "input"     => Tool::Input(tr.try_into()?),
            "intersect" => Tool::Intersect(tr.try_into()?),
            "join"      => Tool::Join(tr.try_into()?),
            "limit"     => Tool::Limit(tr.try_into()?),
            "output"    => Tool::Output(tr.try_into()?),
            "print"     => Tool::Print(tr.try_into()?),
            "project"   => Tool::Project(tr.try_into()?),
            "register"  => Tool::Register(tr.try_into()?),
            "schema"    => Tool::Schema,
            "select"    => Tool::Select(tr.try_into()?),
            "sort"      => Tool::Sort(tr.try_into()?),
            "sql"       => Tool::Sql(tr.try_into()?),
            "union"     => Tool::Union(tr.try_into()?),
            _ => return Err(anyhow!("unknown tool: {name}"))
        };

        Ok(tool)
    }
}

impl Tool {
    pub async fn run(&self, inputs: Option<Values>, ctx: &SessionContext) -> Result<Values>
    {
        let outputs = if let Tool::Sql(args) = self {
            sql::run(args, inputs, ctx).await?
        } else if self.is_source() {
            if inputs.is_some() {
                return Err(anyhow!("{} tool does not take input", self.name()))
            }

            match self {
                Tool::Input(args)    => input::run(args, ctx).await?,
                Tool::Register(args) => register::run(args, ctx).await?,
                _ => unreachable!("{} is not a source tool", self.name())
            }
        } else {
            if inputs.is_none() {
                return Err(anyhow!("{} tool requires input(s)", self.name()))
            }

            let inputs = inputs.unwrap();
            match self {
                Tool::Count(args)    => count::run(args, inputs, ctx).await?,
                Tool::Describe       => describe::run(inputs).await?,
                Tool::Distinct       => distinct::run(inputs).await?,
                Tool::Drop(args)     => drop::run(args, inputs).await?,
                Tool::Fill(args)     => fill::run(args, inputs).await?,
                Tool::Filter(args)   => filter::run(args, inputs).await?,
                Tool::Intersect(_)   => intersect::run(inputs).await?,
                Tool::Join(args)     => join::run(args, inputs).await?,
                Tool::Limit(args)    => limit::run(args, inputs).await?,
                Tool::Output(args)   => output::run(args, inputs).await?,
                Tool::Print(args)    => print::run(args, inputs).await?,
                Tool::Project(args)  => project::run(args, inputs, ctx).await?,
                Tool::Schema         => schema::run(inputs).await?,
                Tool::Select(args)   => select::run(args, inputs).await?,
                Tool::Sort(args)     => sort::run(args, inputs).await?,
                Tool::Union(_)       => union::run(inputs).await?,
                _ => unreachable!("{} is not a sink tool", self.name())
            }
        };

        Ok(outputs)
    }

    pub fn name(&self) -> &str
    {
        match self {
            Tool::Count(_)     => "count",
            Tool::Describe     => "describe",
            Tool::Distinct     => "distinct",
            Tool::Drop(_)      => "drop",
            Tool::Fill(_)      => "fill",
            Tool::Filter(_)    => "filter",
            Tool::Input(_)     => "input",
            Tool::Intersect(_) => "intersect",
            Tool::Join(_)      => "join",
            Tool::Limit(_)     => "limit",
            Tool::Output(_)    => "output",
            Tool::Print(_)     => "print",
            Tool::Project(_)   => "project",
            Tool::Register(_)  => "register",
            Tool::Schema       => "schema",
            Tool::Select(_)    => "select",
            Tool::Sort(_)      => "sort",
            Tool::Sql(_)       => "sql",
            Tool::Union(_)     => "union",
        }
    }

    pub fn expand(&self) -> Vec<FlowRef>
    {
        match self {
            Tool::Join(args)      => join::flows(args),
            Tool::Intersect(args) => intersect::flows(args),
            Tool::Union(args)     => union::flows(args),
            _ => vec![],
        }
    }

    pub fn outputs(&self) -> Vec<&str>
    {
        if let Tool::Filter(_) = self {
            filter::outputs()
        } else {
            vec!["*"]
        }
    }

    pub fn is_source(&self) -> bool
    {
        matches!(self, Tool::Input(_) | Tool::Register(_))
    }
}

#[derive(Debug)]
pub struct ToolArgs {
    positional: Vec<ArgValue>,
    keyword: HashMap<String, ArgValue>,
}

impl ToolArgs {
    pub fn new(args: &[ToolArg]) -> Result<Self>
    {
        let mut keyword = HashMap::new();
        let mut positional = Vec::new();

        for arg in args {
            match arg {
                ToolArg::Keyword { ident, value } => {
                    if keyword.insert(ident.clone(), value.clone()).is_some() {
                        return Err(anyhow!("duplicate named argument '{ident}'"));
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
                    ArgValue::Ident(s)  => Ok(Flow { items: vec![FlowItem::Variable(s.clone())]}),
                    ArgValue::String(s) => Ok(Flow { items: vec![FlowItem::Variable(s.clone())]}),
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

    pub fn optional_string(&self, key: &str) -> Result<Option<String>>
    {
        match self.keyword.get(key) {
            Some(ArgValue::String(s)) => Ok(Some(s.clone())),
            Some(_) => Err(anyhow!("{key} must be a string")),
            None => Ok(None),
        }
    }

    pub fn optional_integer(&self, key: &str) -> Result<Option<i64>>
    {
        match self.keyword.get(key) {
            Some(ArgValue::Integer(n)) => Ok(Some(*n)),
            Some(_) => Err(anyhow!("{key} must be an integer")),
            None => Ok(None),
        }
    }

    pub fn optional_bool(&self, key: &str) -> Result<Option<bool>>
    {
        match self.keyword.get(key) {
            Some(ArgValue::Boolean(b)) => Ok(Some(*b)),
            Some(_) => Err(anyhow!("{key} must be a boolean")),
            None => Ok(None),
        }
    }

    pub fn check_named_args(&self, allowed: &[&str]) -> Result<()>
    {
        for key in self.keyword.keys() {
            if !allowed.contains(&key.as_str()) {
                return Err(anyhow!("unexpected named argument '{key}'"));
            }
        }

        Ok(())
    }

}

#[derive(Debug)]
pub struct FlowRef {
    pub port: String,
    pub flow: Flow,
}

#[derive(Clone, Debug, Default)]
pub struct Values {
    pub dfs: HashMap<String, DataFrame>,
}

impl Values {
    pub fn new(df: DataFrame) -> Self
    {
        Values { dfs: HashMap::from([("*".into(), df)]) }
    }

    pub fn get_one(&self) -> Option<&DataFrame>
    {
        self.dfs.values().next()
    }
    
    pub fn set(&mut self, port: &str, df: DataFrame)
    {
        self.dfs.insert(port.into(), df);
    }
}
