use anyhow::{anyhow, Result};
use pest::Parser;
use pest::iterators::Pair;

use crate::anvil::ast::*;
use crate::anvil::{AnvilParser, Rule};


pub fn build_program(input: &str) -> Result<Program>
{
    let mut pairs = AnvilParser::parse(Rule::PROGRAM, input)?;
    let program = pairs.next().unwrap();

    let mut builder = ASTBuilder::new();
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
        let mut branch = None;
        let mut variable = None;

        for inner in pair.into_inner() {
            match inner.as_rule() {
                Rule::FLOW => {
                    flow = Some(self.build_flow(inner)?);
                }
                Rule::BRANCH_BLOCK => {
                    branch = Some(self.build_branch_block(inner)?);
                }
                Rule::OUTPUT_BINDING => {
                    variable = Some(self.build_variable_binding(inner)?);
                }
                _ => {}
            }
        }

        Ok(Statement {
            flow: flow.ok_or_else(|| anyhow!("statement missing flow"))?,
            branch,
            variable,
        })
    }

    fn build_branch_block(&mut self, pair: Pair<Rule>) -> anyhow::Result<BranchBlock>
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

        Ok(BranchBlock { branches })
    }

    fn build_variable_binding(&self, pair: Pair<Rule>) -> anyhow::Result<String>
    {
        let var = pair
            .into_inner()
            .find(|p| p.as_rule() == Rule::VARIABLE)
            .ok_or_else(|| anyhow!("output binding missing variable"))?;

        Ok(var.as_str().to_string())
    }

    fn build_flow(&mut self, flow: Pair<Rule>) -> Result<Flow> {
        let mut items = vec![];

        for flow_item in flow.into_inner() {
            match flow_item.as_rule() {
                Rule::PIPE => {},
                Rule::FLOW_ITEM | Rule::FLOW_END => {
                    items.push(self.build_flow_item(flow_item)?)
                }
                _ => return Err(anyhow!("illegal flow element {:?}", flow_item.as_rule()))
            }
        }

        Ok(Flow { items })
    }

    fn build_flow_item(&mut self, flow_item: Pair<Rule>) -> Result<FlowItem> {
        let inner = flow_item.into_inner().next()
            .ok_or_else(|| anyhow!("empty flow item"))?;

        let item = match inner.as_rule() {
            Rule::GROUP => {
                let mut items = Vec::new();
                for item in inner.into_inner() {
                    let mut gi = item.into_inner();
                    let name = gi.next().ok_or_else(|| anyhow!("group port not found"))?;
                    let flow = gi.next().ok_or_else(|| anyhow!("group flow not found"))?;

                    items.push(
                        GroupItem { 
                            name: name.as_str().to_string(),
                            flow: self.build_flow(flow)?
                        }
                    )
                }
                FlowItem::Group(items)
            }
            Rule::TOOL_REF => {
                FlowItem::Tool(self.build_tool_ref(inner)?)
            }
            Rule::VARIABLE => {
                FlowItem::Variable(inner.as_str().to_string())
            }
            _ => return Err(anyhow!("invalid flow item: {:?}", inner.as_rule()))
        };

        Ok(item)
    }

    fn build_tool_ref(&mut self, pair: Pair<Rule>) -> Result<ToolRef>
    {
        let mut inner = pair.into_inner();
        let name = inner.next().unwrap().as_str().to_string();

        let tool_args = inner.next().map(|p| p.as_str()).unwrap_or("");
        let args = if !tool_args.is_empty() {
            self.build_args(tool_args)?
        } else {
            Vec::new()
        };

        Ok(ToolRef { id: self.get_next_id(), name, args })
    }

    fn build_branch(&mut self, pair: Pair<Rule>) -> anyhow::Result<Branch>
    {
        let mut inner = pair.into_inner();
        let name   = inner.next().unwrap();
        let target = inner.next().unwrap();
        let target = self.build_branch_target(target)?;

        Ok(Branch {
            name: name.as_str().to_string(),
            target,
        })
    }

    fn build_branch_target(&mut self, pair: Pair<Rule>) -> anyhow::Result<BranchTarget>
    {
        let inner = pair.into_inner().next()
            .ok_or_else(|| anyhow!("branch target must have one child"))?;

        match inner.as_rule() {
            Rule::VARIABLE => {
                Ok(BranchTarget::Variable(inner.as_str().to_string()))
            }
            Rule::FLOW => {
                let flow = self.build_flow(inner)?;
                Ok(BranchTarget::Flow { flow, variable: None })
            }
            _ => Err(anyhow!("invalid branch target")),
        }
    }

    fn build_args(&self, input: &str) -> Result<Vec<ToolArg>>
    {
        let tokens = tokenize(input);
        let mut args = Vec::new();
        let mut i = 0;

        while i < tokens.len() {
            if i + 2 < tokens.len() && let (
                Token::Ident(key),
                Token::Equals,
                Token::String(val),
            ) = (&tokens[i], &tokens[i + 1], &tokens[i + 2])
            {
                args.push(ToolArg::Keyword {
                    key: key.clone(),
                    value: self.build_literal(val),
                });
                i += 3;
            } else if let Token::String(val) = &tokens[i] {
                args.push(ToolArg::Positional(self.build_literal(val)));
                i += 1;
            } else if let Token::Ident(val) = &tokens[i] {
                args.push(ToolArg::Positional(self.build_literal(val)));
                i += 1;
            } else {
                return Err(anyhow!("invalid tool argument syntax: {input}"));
            }
        }

        Ok(args)
    }

    fn build_literal(&self, s: &str) -> Literal
    {
        if s == "true" || s == "false" {
            Literal::Boolean(s == "true")
        } else if let Ok(n) = s.parse::<i64>() {
            Literal::Integer(n)
        } else {
            Literal::String(s.to_string())
        }
    }
}

#[derive(Debug, Clone)]
enum Token {
    Ident(String),
    String(String),
    Equals,
}

fn tokenize(input: &str) -> Vec<Token>
{
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();

    while let Some(&c) = chars.peek() {
        match c {
            ' ' | '\t' | '\n' => {
                chars.next();
            }
            '=' => {
                chars.next();
                tokens.push(Token::Equals);
            }
            '\'' | '"' => {
                let quote = chars.next().unwrap();
                let mut s = String::new();

                for ch in chars.by_ref() {
                    if ch == quote {
                        break;
                    }
                    s.push(ch);
                }

                tokens.push(Token::String(s));
            }
            _ => {
                let mut s = String::new();
                while let Some(&ch) = chars.peek() {
                    if ch.is_whitespace() || ch == '=' {
                        break;
                    }
                    s.push(ch);
                    chars.next();
                }
                tokens.push(Token::Ident(s));
            }
        }
    }

    tokens
}
