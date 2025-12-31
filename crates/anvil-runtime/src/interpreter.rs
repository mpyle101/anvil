use std::collections::HashMap;
use anyhow::{anyhow, Result};
use datafusion::execution::context::SessionContext;

use anvil_parse::{build_program, anvil::ast::*};
use crate::{tools::tool, Value};

use crate::Planner;

pub async fn eval_program(input: &str) -> Result<()>
{
    let program = build_program(input)?;
    let mut planner = Planner::default();
    let _plan = planner.build(program);
//    println!("PLAN:\n{plan:?}");

    let program = build_program(input)?;

    let mut interpreter = Interpreter::default();
    interpreter.eval(program).await?;

    Ok(())
}

#[derive(Default)]
pub struct Interpreter {
    ctx: SessionContext,
    vars: HashMap<String, Value>,
}

impl Interpreter {
    pub fn reset(&mut self)
    {
        self.ctx = SessionContext::default();
    }

    pub async fn eval(&mut self, program: Program) -> Result<()>
    {
        for stmt in program.statements {
            self.eval_statement(stmt).await?;
        }

        Ok(())
    }

    pub async fn eval_statement(&mut self, stmt: Statement) -> Result<()>
    {
        let value = self.eval_flow(&stmt.flow, Value::None).await?;

        if let Some(name) = &stmt.variable {
            self.bind_variable(name, value.clone())?;
        }

        if let Some(branch) = &stmt.branch {
            self.eval_branch_block(branch, value).await?;
        }

        Ok(())
    }

    async fn eval_flow(
        &mut self,
        flow: &Flow,
        input: Value
    ) -> Result<Value>
    {
        let mut current = input;

        for item in &flow.items {
            current = match item {
                FlowItem::Tool(tool) => {
                    self.eval_tool(tool, current).await?
                }
                FlowItem::Variable(name) => {
                    self.vars.get(name)
                        .cloned()
                        .ok_or_else(|| anyhow!("undefined variable '{name}'"))?
                }
                FlowItem::Group(flows) => {
                    let mut data = Vec::new();
                    for f in flows {
                        match Box::pin(self.eval_flow(&f.flow, Value::None)).await? {
                            Value::None => {},
                            Value::Single(d) => data.push(d),
                            Value::Multiple(mut d) => data.append(&mut d)
                        }
                    }
                    Value::Multiple(data)
                }
            }
        }

        Ok(current)
    }

    async fn eval_branch_block(
        &mut self,
        block: &BranchBlock,
        input: Value,
    ) ->Result<()>
    {
        let dfs = match input {
            Value::Multiple(dfs) => dfs,
            _ => return Err(anyhow!("branch requires multiple values")),
        };

        if dfs.len() != block.branches.len() {
            return Err(anyhow!("branch count mismatch"));
        }

        for (branch, df) in block.branches.iter().zip(dfs) {
            self.eval_branch(branch, Value::Single(df)).await?;
        }

        Ok(())
    }

    async fn eval_branch(
        &mut self,
        branch: &Branch,
        input: Value,
    ) -> Result<()>
    {
        match &branch.target {
            BranchTarget::Variable(name) => {
                self.bind_variable(name, input)?;
            }
            BranchTarget::Flow { flow, variable } => {
                let value = self.eval_flow(flow, input).await?;
                if let Some(name) = variable {
                    self.bind_variable(name, value)?;
                }
            }
        }

        Ok(())
    }

    async fn eval_tool(&mut self, tr: &ToolRef, input: Value) -> Result<Value>
    {
        tool::run(tr, input, &self.ctx).await
    }

    fn bind_variable(&mut self, name: &String, value: Value) -> Result<()>
    {
        if let Value::Multiple(_) = value {
            return Err(anyhow!("cannot bind multiple values to variable '{name}'"));
        }
        self.vars.insert(name.clone(), value);

        Ok(())
    }
}
