use std::collections::HashMap;

use anyhow::{anyhow, Result};
use petgraph::graph::{Graph, NodeIndex};

use anvil_parse::anvil::ast::*;

type ExecutionPlan = Graph<Node, String>;

#[derive(Debug)]
pub enum Node {
    Tool(ToolRef),
    Variable(String),
}

#[derive(Default)]
pub struct Planner {
    plan: ExecutionPlan,
    vars: HashMap<String, NodeIndex>,
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

    fn build_statement(&mut self, stmt: Statement) -> Result<()>
    {
        let ix = self.build_flow(&stmt.flow, "default", None)?;

        if let Some(name) = &stmt.variable {
            let vx = self.add_var_node(name)?;
            self.plan.try_add_edge(ix, vx, "default".into())?;
        }

        if let Some(block) = &stmt.branch {
            for branch in &block.branches {
                self.build_branch(branch, ix)?;
            }
        }

        Ok(())
    }

    fn build_flow(
        &mut self,
        flow: &Flow,
        port: &str,
        input: Option<NodeIndex>,
    ) -> Result<NodeIndex>
    {
        let mut current = match input {
            None => vec![],
            Some(ix) => vec![(port, ix)]
        };

        for item in &flow.items {
            current = match item {
                FlowItem::Tool(tool) => {
                    let ix = self.add_tool_node(tool)?;
                    for (p, i) in current {
                        self.plan.try_add_edge(i, ix, p.into())?;
                    }
                    vec![("default", ix)]
                }
                FlowItem::Variable(name) => {
                    let ix = self.vars.get(name)
                        .cloned()
                        .ok_or_else(|| anyhow!("undefined variable '{name}'"))?;
                    for (p, i) in current {
                        self.plan.try_add_edge(i, ix, p.into())?;
                    }
                    vec![("default", ix)]
                }
                FlowItem::Group(items) => {
                    let mut nodes = Vec::new();
                    for GroupItem { name, flow } in items {
                        nodes.push((name.as_str(), self.build_flow(flow, name, None)?));
                    }
                    nodes
                }
            }
        }

        Ok(current[0].1)
    }

    fn build_branch(
        &mut self,
        branch: &Branch,
        input: NodeIndex,
    ) -> Result<()>
    {
        match &branch.target {
            BranchTarget::Variable(name) => {
                let ix = self.add_var_node(name)?;
                self.plan.try_add_edge(input, ix, branch.name.clone())?;
            }
            BranchTarget::Flow { flow, variable } => {
                let ix = self.build_flow(flow, &branch.name, Some(input))?;
                if let Some(name) = variable {
                    let vx = self.add_var_node(name)?;
                    self.plan.try_add_edge(ix, vx, "default".into())?;
                }
            }
        }

        Ok(())
    }

    fn add_tool_node(&mut self, tr: &ToolRef) -> Result<NodeIndex>
    {
        let ix = if let Some(ix) = self.tools.get(&tr.id) {
            *ix
        } else {
            let ix = self.plan.try_add_node(Node::Tool(tr.clone()))?;
            self.tools.insert(tr.id, ix);
            ix
        };

        Ok(ix)
    }

    fn add_var_node(&mut self, name: &String) -> Result<NodeIndex>
    {
        let ix = if let Some(ix) = self.vars.get(name) {
            *ix
        } else {
            let ix = self.plan.try_add_node(Node::Variable(name.clone()))?;
            self.vars.insert(name.clone(), ix);
            ix
        };

        Ok(ix)
    }
}
