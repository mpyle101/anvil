use anyhow::{anyhow, Result};
use pest::Parser;
use pest::iterators::Pair;

use crate::ast::*;
use crate::AnvilParser;
use crate::Rule;


pub fn parse_program(input: &str) -> Result<Program>
{
    let mut pairs = AnvilParser::parse(Rule::program, input)?;
    let program = pairs.next().unwrap();

    let mut statements = Vec::new();

    for pair in program.into_inner() {
        if pair.as_rule() == Rule::statement {
            statements.push(parse_statement(pair)?);
        }
    }

    Ok(Program { statements })
}

fn parse_statement(pair: Pair<Rule>) -> Result<Statement>
{
    let mut flow = None;
    let mut branch = None;
    let mut variable = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::flow => {
                flow = Some(parse_flow(inner)?);
            }
            Rule::branch_block => {
                branch = Some(parse_branch_block(inner)?);
            }
            Rule::output_binding => {
                variable = Some(parse_variable_binding(inner)?);
            }
            Rule::EOI | Rule::WHITESPACE => {}
            _ => {}
        }
    }

    Ok(Statement {
        flow: flow.ok_or_else(|| anyhow!("statement missing flow"))?,
        branch,
        variable,
    })
}

fn parse_flow(pair: Pair<Rule>) -> Result<Flow>
{
    let pipeline = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::pipeline)
        .ok_or_else(|| anyhow!("flow missing pipeline"))?;

    parse_pipeline(pipeline)
}

fn parse_branch_block(pair: Pair<Rule>) -> anyhow::Result<BranchBlock>
{
    let mut branches = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::branches {
            for item in inner.into_inner() {
                if item.as_rule() == Rule::branch {
                    branches.push(parse_branch(item)?);
                }
            }
        }
    }

    Ok(BranchBlock { branches })
}

fn parse_variable_binding(pair: Pair<Rule>) -> anyhow::Result<String>
{
    let var = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::variable)
        .ok_or_else(|| anyhow!("output binding missing variable"))?;

    Ok(var.as_str().to_string())
}

fn parse_pipeline(pair: Pair<Rule>) -> anyhow::Result<Flow> {
    let mut items = Vec::new();

    for flow_item in pair.into_inner() {
        let inner = flow_item.into_inner().next()
            .ok_or_else(|| anyhow!("empty flow item"))?;

        match inner.as_rule() {
            Rule::tool_ref => {
                items.push(FlowItem::Tool(parse_tool_ref(inner)?));
            }
            Rule::variable => {
                items.push(FlowItem::Variable(inner.as_str().to_string()));
            }
            _ => {
                return Err(anyhow!("unexpected rule inside pipeline_item: {:?}", inner.as_rule()))
            }
        }
    }

    if items.is_empty() {
        return Err(anyhow!("pipeline must contain at least one item"));
    }

    Ok(Flow { items })
}

fn parse_tool_ref(pair: Pair<Rule>) -> Result<ToolRef>
{
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();

    let mut args = Vec::new();
    if let Some(p) = inner.next() {
        for arg in p.into_inner() {
            args.push(parse_arg(arg)?);
        }
    }

    Ok(ToolRef { name, args })
}

fn parse_arg(pair: Pair<Rule>) -> Result<ToolArg>
{
    let inner = pair.into_inner().next().unwrap();

    match inner.as_rule() {
        Rule::keyword_arg => {
            let mut parts = inner.into_inner();
            let key = parts.next().unwrap().as_str().to_string();
            let value = parse_literal(parts.next().unwrap())?;
            Ok(ToolArg::Keyword { key, value })
        }
        Rule::positional_arg => {
            Ok(ToolArg::Positional(parse_literal(inner.into_inner().next().unwrap())?))
        }
        _ => unreachable!(),
    }
}

fn parse_branch(pair: Pair<Rule>) -> anyhow::Result<Branch>
{
    let mut inner = pair.into_inner();
    let name   = inner.next().unwrap();
    let target = inner.next().unwrap();
    let target = parse_branch_target(target)?;

    Ok(Branch {
        name: name.as_str().to_string(),
        target,
    })
}

fn parse_branch_target(pair: Pair<Rule>) -> anyhow::Result<BranchTarget>
{
    let inner = pair.into_inner().next()
        .ok_or_else(|| anyhow!("branch target must have one child"))?;

    match inner.as_rule() {
        Rule::variable => {
            Ok(BranchTarget::Variable(inner.as_str().to_string()))
        }
        Rule::flow => {
            let flow = parse_flow(inner)?;
            Ok(BranchTarget::Flow { flow, variable: None })
        }
        _ => Err(anyhow!("invalid branch target")),
    }
}

fn parse_literal(pair: Pair<Rule>) -> Result<Literal>
{
    let pair = match pair.as_rule() {
        Rule::literal => pair.into_inner().next().unwrap(),
        _ => pair,
    };

    match pair.as_rule() {
        Rule::string => {
            let s = pair.as_str();
            Ok(Literal::String(s[1..s.len() - 1].to_string()))
        }
        Rule::number => {
            Ok(Literal::Number(pair.as_str().parse()?))
        }
        Rule::boolean => {
            Ok(Literal::Boolean(pair.as_str() == "true"))
        }
        _ => {
            Err(anyhow::anyhow!("unexpected value literal: {:?}", pair.as_rule()))
        }
    }
}
