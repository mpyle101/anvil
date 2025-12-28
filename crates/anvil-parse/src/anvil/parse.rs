use anyhow::{anyhow, Result};
use pest::Parser;
use pest::iterators::Pair;

use crate::anvil::ast::*;
use crate::anvil::{AnvilParser, Rule};


pub fn parse_program(input: &str) -> Result<Program>
{
    let mut pairs = AnvilParser::parse(Rule::program, input)?;
    let program = pairs.next().unwrap();

    let mut builder = ASTBuilder::new();
    builder.eval(program)
}

pub fn parse_statement(builder: &mut ASTBuilder, input: &str) -> Result<Statement>
{
    let mut pairs = AnvilParser::parse(Rule::statement, input)?;
    let statement = pairs.next().unwrap();

    builder.parse_statement(statement)
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

    fn eval(&mut self, program: Pair<Rule>) -> Result<Program>
    {
        let mut statements = Vec::new();

        for pair in program.into_inner() {
            if pair.as_rule() == Rule::statement {
                statements.push(self.parse_statement(pair)?);
            }
        }

        Ok(Program { statements })
    }

    fn parse_statement(&mut self, pair: Pair<Rule>) -> Result<Statement>
    {
        let mut flow = None;
        let mut branch = None;
        let mut variable = None;

        for inner in pair.into_inner() {
            match inner.as_rule() {
                Rule::flow => {
                    flow = Some(self.parse_flow(inner)?);
                }
                Rule::branch_block => {
                    branch = Some(self.parse_branch_block(inner)?);
                }
                Rule::output_binding => {
                    variable = Some(self.parse_variable_binding(inner)?);
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

    fn parse_branch_block(&mut self, pair: Pair<Rule>) -> anyhow::Result<BranchBlock>
    {
        let mut branches = Vec::new();

        for inner in pair.into_inner() {
            if inner.as_rule() == Rule::branches {
                for item in inner.into_inner() {
                    if item.as_rule() == Rule::branch {
                        branches.push(self.parse_branch(item)?);
                    }
                }
            }
        }

        Ok(BranchBlock { branches })
    }

    fn parse_variable_binding(&self, pair: Pair<Rule>) -> anyhow::Result<String>
    {
        let var = pair
            .into_inner()
            .find(|p| p.as_rule() == Rule::variable)
            .ok_or_else(|| anyhow!("output binding missing variable"))?;

        Ok(var.as_str().to_string())
    }

    fn parse_flow(&mut self, flow: Pair<Rule>) -> anyhow::Result<Flow> {
        let mut items = Vec::new();

        for flow_item in flow.into_inner() {
            let inner = flow_item.into_inner().next()
                .ok_or_else(|| anyhow!("empty flow item"))?;

            match inner.as_rule() {
                Rule::tool_ref => {
                    items.push(FlowItem::Tool(self.parse_tool_ref(inner)?));
                }
                Rule::variable => {
                    items.push(FlowItem::Variable(inner.as_str().to_string()));
                }
                Rule::group => {
                    let mut flows = Vec::new();
                    for flow in inner.into_inner() {
                        flows.push(self.parse_flow(flow)?)
                    }
                    items.push(FlowItem::Group(flows));
                }
                _ => {
                    return Err(anyhow!("unexpected rule inside flow: {:?}", inner.as_rule()))
                }
            }
        }

        if items.is_empty() {
            return Err(anyhow!("flow must contain at least one item"));
        }

        Ok(Flow { items })
    }

    fn parse_tool_ref(&mut self, pair: Pair<Rule>) -> Result<ToolRef>
    {
        let mut inner = pair.into_inner();
        let name = inner.next().unwrap().as_str().to_string();

        let tool_args = inner.next().map(|p| p.as_str()).unwrap_or("");
        let args = if !tool_args.is_empty() {
            self.parse_args(tool_args)?
        } else {
            Vec::new()
        };

        Ok(ToolRef { id: self.get_next_id(), name, args })
    }

    fn parse_branch(&mut self, pair: Pair<Rule>) -> anyhow::Result<Branch>
    {
        let mut inner = pair.into_inner();
        let name   = inner.next().unwrap();
        let target = inner.next().unwrap();
        let target = self.parse_branch_target(target)?;

        Ok(Branch {
            name: name.as_str().to_string(),
            target,
        })
    }

    fn parse_branch_target(&mut self, pair: Pair<Rule>) -> anyhow::Result<BranchTarget>
    {
        let inner = pair.into_inner().next()
            .ok_or_else(|| anyhow!("branch target must have one child"))?;

        match inner.as_rule() {
            Rule::variable => {
                Ok(BranchTarget::Variable(inner.as_str().to_string()))
            }
            Rule::flow => {
                let flow = self.parse_flow(inner)?;
                Ok(BranchTarget::Flow { flow, variable: None })
            }
            _ => Err(anyhow!("invalid branch target")),
        }
    }

    fn parse_args(&self, input: &str) -> Result<Vec<ToolArg>>
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
                    value: self.parse_literal(val),
                });
                i += 3;
            } else if let Token::String(val) = &tokens[i] {
                args.push(ToolArg::Positional(self.parse_literal(val)));
                i += 1;
            } else if let Token::Ident(val) = &tokens[i] {
                args.push(ToolArg::Positional(self.parse_literal(val)));
                i += 1;
            } else {
                return Err(anyhow!("invalid tool argument syntax: {input}"));
            }
        }

        Ok(args)
    }

    fn parse_literal(&self, s: &str) -> Literal
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
