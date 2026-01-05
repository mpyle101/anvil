use anyhow::{anyhow, Result};
use pest::Parser;
use pest::iterators::Pair;

use anvil_context::{intern, Symbol};

use crate::anvil::ast::*;
use crate::anvil::{AnvilParser, Rule};


pub fn build_program(builder: &mut ASTBuilder, input: &str) -> Result<Program>
{
    let mut pairs = AnvilParser::parse(Rule::PROGRAM, input)?;
    let program = pairs.next().unwrap();

    builder.build(program)
}

pub fn build_statement(builder: &mut ASTBuilder, input: &str) -> Result<Statement>
{
    let mut pairs = AnvilParser::parse(Rule::STATEMENT, input)?;
    let statement = pairs.next().unwrap();

    builder.build_statement(statement)
}


#[derive(Default)]
pub struct ASTBuilder {
    next_tool_id: usize,
}

impl ASTBuilder {
    pub fn new() -> Self
    {
        Self { next_tool_id: 1 }
    }

    fn get_next_id(&mut self) -> ToolId
    {
        let id = self.next_tool_id;
        self.next_tool_id += 1;

        ToolId(id)
    }

    fn build(&mut self, program: Pair<Rule>) -> Result<Program>
    {
        let mut statements = Vec::new();

        for pair in program.into_inner() {
            if pair.as_rule() == Rule::STATEMENT {
                statements.push(self.build_statement(pair)?);
            }
        }

        Ok(Program { statements })
    }

    fn build_statement(&mut self, pair: Pair<Rule>) -> Result<Statement>
    {
        let mut flow = None;
        let mut branches = None;
        let mut variable = None;

        for inner in pair.into_inner() {
            match inner.as_rule() {
                Rule::FLOW => {
                    flow = Some(self.build_flow(inner)?);
                }
                Rule::BRANCH_BLOCK => {
                    branches = Some(self.build_branches(inner)?);
                }
                Rule::OUTPUT_BINDING => {
                    variable = Some(self.build_variable_binding(inner)?);
                }
                _ => {}
            }
        }

        Ok(Statement {
            flow: flow.ok_or_else(|| anyhow!("statement missing flow"))?,
            branches,
            variable,
        })
    }

    fn build_branches(&mut self, pair: Pair<Rule>) -> Result<Vec<Branch>>
    {
        let mut branches = Vec::new();

        for inner in pair.into_inner() {
            if inner.as_rule() == Rule::BRANCHES {
                for item in inner.into_inner() {
                    if item.as_rule() == Rule::BRANCH {
                        branches.push(self.build_branch(item)?);
                    }
                }
            }
        }

        Ok(branches)
    }

    fn build_branch(&mut self, pair: Pair<Rule>) -> Result<Branch>
    {
        let mut inner = pair.into_inner();
        let name   = inner.next().unwrap();
        let target = inner.next().unwrap();
        let target = self.build_target(target)?;

        Ok(Branch {
            name: intern(name.as_str()),
            target,
        })
    }

    fn build_target(&mut self, pair: Pair<Rule>) -> Result<Target>
    {
        let inner = pair.into_inner().next()
            .ok_or_else(|| anyhow!("branch target must have one child"))?;

        match inner.as_rule() {
            Rule::VARIABLE => {
                Ok(Target::Variable(intern(inner.as_str())))
            }
            Rule::FLOW => {
                let flow = self.build_flow(inner)?;
                Ok(Target::Flow { flow, variable: None })
            }
            _ => Err(anyhow!("invalid branch target")),
        }
    }

    fn build_variable_binding(&self, pair: Pair<Rule>) -> Result<Symbol>
    {
        let var = pair
            .into_inner()
            .find(|p| p.as_rule() == Rule::VARIABLE)
            .ok_or_else(|| anyhow!("output binding missing variable"))?;

        Ok(intern(var.as_str()))
    }

    fn build_flow(&mut self, flow: Pair<Rule>) -> Result<Flow>
    {
        let mut items = vec![];

        for flow_item in flow.into_inner() {
            match flow_item.as_rule() {
                Rule::PIPE => {},
                Rule::TOOL_REF => {
                    items.push(FlowItem::Tool(self.build_tool_ref(flow_item)?))
                }
                Rule::VARIABLE => {
                    items.push(FlowItem::Variable(intern(flow_item.as_str())))
                }
                _ => return Err(anyhow!("invalid flow item: {:?}", flow_item.as_rule()))
            }
        }

        Ok(Flow { items })
    }

    fn build_tool_ref(&mut self, pair: Pair<Rule>) -> Result<ToolRef>
    {
        let mut inner = pair.into_inner();
        let name = intern(inner.next().unwrap().as_str());

        let mut args = vec![];
        if let Some(tool_args) = inner.next() {
            for arg in tool_args.into_inner() {
                match arg.as_rule() {
                    Rule::POSITIONAL => {
                        let value = arg.into_inner().next().unwrap();
                        args.push(ToolArg::Positional(self.build_arg_value(value)?))
                    }
                    Rule::KEYWORD => {
                        let mut inner = arg.into_inner();
                        let ident = intern(inner.next().unwrap().as_str());
                        let value = self.build_arg_value(inner.next().unwrap())?;
                        args.push(ToolArg::Keyword { ident, value })
                    }
                    _ => return Err(anyhow!("unexpected tool argument {:?}", arg.as_rule()))
                }
            }
        }

        Ok(ToolRef { id: self.get_next_id(), name, args })
    }

    fn build_arg_value(&mut self, pair: Pair<Rule>) -> Result<ArgValue>
    {
        let inner = pair.into_inner().next()
            .ok_or_else(|| anyhow!("empty arg value encountered"))?;

        let v = match inner.as_rule() {
            Rule::FLOW       => ArgValue::Flow(self.build_flow(inner)?),
            Rule::LITERAL    => self.build_literal(inner)?,
            Rule::IDENTIFIER => ArgValue::Ident(inner.as_str().to_string()),
            _ => return Err(anyhow!("unexpected arg value {:?}", inner.as_rule()))
        };

        Ok(v)
    }

    fn build_literal(&self, pair: Pair<Rule>) -> Result<ArgValue>
    {
        let inner = pair.into_inner().next().unwrap();

        let av = match inner.as_rule() {
            Rule::BOOLEAN => ArgValue::Boolean(inner.as_str() == "true"),
            Rule::NUMBER  => ArgValue::Integer(inner.as_str().parse::<i64>()?),
            Rule::STRING  => {
                let s = inner.as_str();
                let v = &s[1..s.len() - 1];
                ArgValue::String(v.to_string())
            }
            _ => return Err(anyhow!("unexpected literal {:?}", inner.as_rule()))
        };

        Ok(av)
    }
}
