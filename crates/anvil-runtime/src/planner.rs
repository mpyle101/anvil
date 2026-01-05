use core::default::Default;
use std::collections::HashMap;
use std::fmt;

use anyhow::{anyhow, Result};
use petgraph::graph::{Graph, NodeIndex};

use anvil_context::{intern, resolve, syms, Symbol};
use anvil_parse::anvil::ast::*;
use crate::tools::Tool;

pub type ExecutionPlan = Graph<ExecNode, ExecEdge>;


#[derive(Default)]
pub struct Planner {
    plan: ExecutionPlan,
    vars: HashMap<Symbol, NodeIndex>,
    tools: HashMap<ToolId, NodeIndex>,
}

impl Planner {
    pub fn build(&mut self, program: Program) -> Result<&ExecutionPlan>
    {
        for stmt in program.statements {
            self.build_statement(stmt)?;
        }

        Ok(&self.plan)
    }

    pub fn build_statement(&mut self, stmt: Statement) -> Result<&ExecutionPlan>
    {
        let ix = self.build_flow(&stmt.flow, syms().default, None)?;

        if let Some(name) = &stmt.variable {
            let vx = self.add_var_node(name)?;
            self.plan.try_add_edge(ix, vx, ExecEdge::default())?;
        }

        if let Some(branches) = &stmt.branches {
            for branch in branches {
                self.build_branch(branch, ix)?;
            }
        }

        Ok(&self.plan)
    }

    fn build_flow(
        &mut self,
        flow: &Flow,
        port: Symbol,
        input: Option<NodeIndex>,
    ) -> Result<NodeIndex>
    {
        let mut current = input.map(|ix| (port, ix));

        for item in &flow.items {
            current = match item {
                FlowItem::Tool(tr) => {
                    let tool: Tool = tr.try_into()?;

                    let mut fr = vec![];
                    for f in tool.expand() {
                        let ix = self.build_flow(&f.flow, syms().default, None)?;
                        fr.push((f.port, ix));
                    }

                    let ix = self.add_tool_node(&tr.id, tool)?;

                    if let Some((p, src)) = current {
                        self.plan.try_add_edge(src, ix, ExecEdge::new(p))?;
                    }
                    for (p, src) in fr {
                        self.plan.try_add_edge(src, ix, ExecEdge::new(p))?;
                    }
                    Some((syms().default, ix))
                }
                FlowItem::Variable(name) => {
                    let ix = self.vars.get(name)
                        .cloned()
                        .ok_or_else(|| anyhow!("undefined variable '{}'", resolve(*name)))?;
                    if let Some((p, src)) = current {
                        self.plan.try_add_edge(src, ix, ExecEdge::new(p))?;
                    }
                    Some((syms().default, ix))
                }
            }
        }

        Ok(current.unwrap().1)
    }

    fn build_branch(&mut self, branch: &Branch, input: NodeIndex) -> Result<()>
    {
        match &branch.target {
            Target::Variable(name) => {
                let ix = self.add_var_node(name)?;
                self.plan.try_add_edge(input, ix, ExecEdge::new(branch.name))?;
            }
            Target::Flow { flow, variable } => {
                let ix = self.build_flow(flow, branch.name, Some(input))?;
                if let Some(name) = variable {
                    let vx = self.add_var_node(name)?;
                    self.plan.try_add_edge(ix, vx, ExecEdge::default())?;
                }
            }
        }

        Ok(())
    }

    fn add_tool_node(&mut self, id: &ToolId, tool: Tool) -> Result<NodeIndex>
    {
        let ix = if let Some(ix) = self.tools.get(id) {
            *ix
        } else {
            let ix = self.plan.try_add_node(ExecNode::Tool(tool))?;
            self.tools.insert(*id, ix);
            ix
        };

        Ok(ix)
    }

    fn add_var_node(&mut self, name: &Symbol) -> Result<NodeIndex>
    {
        let ix = if let Some(ix) = self.vars.get(name) {
            *ix
        } else {
            let ix = self.plan.try_add_node(ExecNode::Variable(*name))?;
            self.vars.insert(*name, ix);
            ix
        };

        Ok(ix)
    }
}

#[derive(Debug)]
pub enum ExecNode {
    Tool(Tool),
    Variable(Symbol),
}

impl fmt::Display for ExecNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result
    {
        write!(f, "{}", self.name())
    }
}

impl ExecNode {
    pub fn is_source(&self) -> bool
    {
        if let ExecNode::Tool(tool) = self {
            tool.is_source()
        } else {
            false
        }
    }

    pub fn name(&self) -> &str
    {
        match self {
            ExecNode::Tool(tool)    => tool.name(),
            ExecNode::Variable(sym) => resolve(*sym),
        }
    }
}

#[derive(Debug)]
pub struct ExecEdge {
    pub port: Symbol,
}

impl fmt::Display for ExecEdge {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result
    {
        write!(f, "{}", resolve(self.port))
    }
}

impl ExecEdge {
    fn new(port: Symbol) -> Self
    {
        ExecEdge { port }
    }
}

impl Default for ExecEdge {
    fn default() -> Self
    {
        ExecEdge { port: intern("*") }
    }
}
