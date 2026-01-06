use anyhow::{anyhow, Result};
use datafusion::prelude::SessionContext;

use anvil_context::{resolve, tool_types, Symbol, ToolType};
use crate::tools::*;


#[derive(Debug)]
pub struct FlowRef {
    pub port: Symbol,
    pub flow: Flow,
}

#[derive(Debug)]
pub enum Tool {
    Count((ToolId, count::CountArgs)),
    Describe(ToolId),
    Distinct(ToolId),
    Drop((ToolId, drop::DropArgs)),
    Fill((ToolId, fill::FillArgs)),
    Filter((ToolId, filter::FilterArgs)),
    Input((ToolId, input::InputArgs)),
    Intersect((ToolId, intersect::IntersectArgs)),
    Join((ToolId, join::JoinArgs)),
    Limit((ToolId, limit::LimitArgs)),
    Output((ToolId, output::OutputArgs)),
    Print((ToolId, print::PrintArgs)),
    Project((ToolId, project::ProjectArgs)),
    Register((ToolId, register::RegisterArgs)),
    Schema(ToolId),
    Select((ToolId, select::SelectArgs)),
    Sort((ToolId, sort::SortArgs)),
    Sql((ToolId, sql::SqlArgs)),
    Union((ToolId, union::UnionArgs)),
}

impl TryFrom<&ToolRef> for Tool {
    type Error = anyhow::Error;

    fn try_from(tr: &ToolRef) -> Result<Self>
    {
        use ToolType::*;

        let name = tr.name;
        let tool = match tool_types().get(&tr.name) {
            Some(Count)     => Tool::Count((tr.id, tr.try_into()?)),
            Some(Describe)  => Tool::Describe(tr.id),
            Some(Distinct)  => Tool::Distinct(tr.id),
            Some(Drop)      => Tool::Drop((tr.id, tr.try_into()?)),
            Some(Fill)      => Tool::Fill((tr.id, tr.try_into()?)),
            Some(Filter)    => Tool::Filter((tr.id, tr.try_into()?)),
            Some(Input)     => Tool::Input((tr.id, tr.try_into()?)),
            Some(Intersect) => Tool::Intersect((tr.id, tr.try_into()?)),
            Some(Join)      => Tool::Join((tr.id, tr.try_into()?)),
            Some(Limit)     => Tool::Limit((tr.id, tr.try_into()?)),
            Some(Output)    => Tool::Output((tr.id, tr.try_into()?)),
            Some(Print)     => Tool::Print((tr.id, tr.try_into()?)),
            Some(Project)   => Tool::Project((tr.id, tr.try_into()?)),
            Some(Register)  => Tool::Register((tr.id, tr.try_into()?)),
            Some(Schema)    => Tool::Schema(tr.id),
            Some(Select)    => Tool::Select((tr.id, tr.try_into()?)),
            Some(Sort)      => Tool::Sort((tr.id, tr.try_into()?)),
            Some(Sql)       => Tool::Sql((tr.id, tr.try_into()?)),
            Some(Union)     => Tool::Union((tr.id, tr.try_into()?)),
            _ => return Err(anyhow!("unknown tool: {}", resolve(name)))
        };

        Ok(tool)
    }
}

impl Tool {
    pub async fn run(&self, inputs: Option<Values>, ctx: &SessionContext) -> Result<Values>
    {
        let outputs = if let Tool::Sql((id, args)) = self {
            sql::run(id, args, inputs, ctx).await?
        } else if self.is_source() {
            if inputs.is_some() {
                return Err(anyhow!("{} tool does not take input", self.name()))
            }

            match self {
                Tool::Input((id, args))    => input::run(id, args, ctx).await?,
                Tool::Register((id, args)) => register::run(id, args, ctx).await?,
                _ => unreachable!("{} is not a source tool", self.name())
            }
        } else {
            if inputs.is_none() {
                return Err(anyhow!("{} tool requires input(s)", self.name()))
            }

            let inputs = inputs.unwrap();
            match self {
                Tool::Count((id, args))   => count::run(id, args, inputs, ctx).await?,
                Tool::Describe(id)        => describe::run(id, inputs).await?,
                Tool::Distinct(id)        => distinct::run(id, inputs).await?,
                Tool::Drop((id, args))    => drop::run(id, args, inputs).await?,
                Tool::Fill((id, args))    => fill::run(id, args, inputs).await?,
                Tool::Filter((id, args))  => filter::run(id, args, inputs).await?,
                Tool::Intersect((id, _))  => intersect::run(id, inputs).await?,
                Tool::Join((id, args))    => join::run(id, args, inputs).await?,
                Tool::Limit((id, args))   => limit::run(id, args, inputs).await?,
                Tool::Output((id, args))  => output::run(id, args, inputs).await?,
                Tool::Print((id, args))   => print::run(id, args, inputs).await?,
                Tool::Project((id, args)) => project::run(id, args, inputs, ctx).await?,
                Tool::Schema(id)          => schema::run(id, inputs).await?,
                Tool::Select((id, args))  => select::run(id, args, inputs).await?,
                Tool::Sort((id, args))    => sort::run(id, args, inputs).await?,
                Tool::Union((id, _))      => union::run(id, inputs).await?,
                _ => unreachable!("{} is not a sink tool", self.name())
            }
        };

        Ok(outputs)
    }

    pub fn name(&self) -> &str
    {
        match self {
            Tool::Count(_)     => "count",
            Tool::Describe(_)  => "describe",
            Tool::Distinct(_)  => "distinct",
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
            Tool::Schema(_)    => "schema",
            Tool::Select(_)    => "select",
            Tool::Sort(_)      => "sort",
            Tool::Sql(_)       => "sql",
            Tool::Union(_)     => "union",
        }
    }

    pub fn id(&self) -> ToolId
    {
        match self {
            Tool::Count((id, _))     => *id,
            Tool::Describe(id)       => *id,
            Tool::Distinct(id)       => *id,
            Tool::Drop((id, _))      => *id,
            Tool::Fill((id, _))      => *id,
            Tool::Filter((id, _))    => *id,
            Tool::Input((id, _))     => *id,
            Tool::Intersect((id, _)) => *id,
            Tool::Join((id, _))      => *id,
            Tool::Limit((id, _))     => *id,
            Tool::Output((id, _))    => *id,
            Tool::Print((id, _))     => *id,
            Tool::Project((id, _))   => *id,
            Tool::Register((id, _))  => *id,
            Tool::Schema(id)         => *id,
            Tool::Select((id, _))    => *id,
            Tool::Sort((id, _))      => *id,
            Tool::Sql((id, _))       => *id,
            Tool::Union((id, _))     => *id,
        }
    }

    pub fn expand(&self) -> Vec<FlowRef>
    {
        match self {
            Tool::Join((_, args))      => join::flows(args),
            Tool::Intersect((_, args)) => intersect::flows(args),
            Tool::Union((_, args))     => union::flows(args),
            _ => vec![],
        }
    }

    pub fn is_source(&self) -> bool
    {
        matches!(self, Tool::Input(_) | Tool::Register(_))
    }
}
