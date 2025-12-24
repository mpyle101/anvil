use anyhow::{anyhow, Result};
use pest::Parser;
use pest::iterators::Pair;

use crate::expr::ast::*;
use crate::expr::{ExprParser, Rule};


pub fn parse_expression(input: &str) -> Result<Expr>
{
    let mut pairs = ExprParser::parse(Rule::expression, input)?;
    let expr = pairs.next().unwrap();

    parse_expr(expr)
}

pub fn parse_expr(pair: Pair<Rule>) -> Result<Expr>
{
    let inner = pair.into_inner().next()
        .ok_or_else(|| anyhow!("empty expression"))?;

    parse_assignment(inner)
}

fn parse_assignment(pair: Pair<Rule>) -> Result<Expr>
{
    let mut inner = pair.into_inner();
            
    let x = inner.next()
        .ok_or_else(|| anyhow!("empty logical or"))?;
    let left = parse_logical_or(x)?;

    if let Some(rhs) = inner.next() {
        match left {
            Expr::Column(name) => Ok(Expr::Assign {
                target: name,
                value: Box::new(parse_assignment(rhs)?),
            }),
            _ => Err(anyhow!("left side of assignment must be a column")),
        }
    } else {
        Ok(left)
    }
}

fn parse_logical_or(pair: Pair<Rule>) -> Result<Expr>
{
    fold_binary_ops(pair, parse_logical_and, |op| match op {
        "||" => BinaryOp::Or,
        _ => unreachable!(),
    })
}

fn parse_logical_and(pair: Pair<Rule>) -> Result<Expr>
{
    fold_binary_ops(pair, parse_comparison, |op| match op {
        "&&" => BinaryOp::And,
        _ => unreachable!(),
    })
}

fn parse_comparison(pair: Pair<Rule>) -> Result<Expr>
{
    let mut inner = pair.into_inner();
    let x = inner.next()
        .ok_or_else(|| anyhow!("empty additive"))?;
    let mut expr = parse_additive(x)?;

    while let Some(op) = inner.next() {
        let x = inner.next()
            .ok_or_else(|| anyhow!("empty right hand side"))?;
        let rhs = parse_additive(x)?;

        let bin_op = match op.as_str() {
            "==" => BinaryOp::Eq,
            "!=" => BinaryOp::Ne,
            ">"  => BinaryOp::Gt,
            "<"  => BinaryOp::Lt,
            ">=" => BinaryOp::Ge,
            "<=" => BinaryOp::Le,
            _ => return Err(anyhow!("unrecognized operator {}", op.as_str()))
        };

        expr = Expr::Binary {
            left: Box::new(expr),
            op: bin_op,
            right: Box::new(rhs),
        };
    }

    Ok(expr)
}

fn parse_additive(pair: Pair<Rule>) -> Result<Expr>
{
    fold_binary_ops(pair, parse_multiplicative, |op| match op {
        "+" => BinaryOp::Add,
        "-" => BinaryOp::Sub,
        _ => unreachable!(),
    })
}

fn parse_multiplicative(pair: Pair<Rule>) -> Result<Expr> {
    fold_binary_ops(pair, parse_unary, |op| match op {
        "*" => BinaryOp::Mul,
        "/" => BinaryOp::Div,
        _ => unreachable!(),
    })
}

fn fold_binary_ops<F>(
    pair: Pair<Rule>,
    next: fn(Pair<Rule>) -> Result<Expr>,
    map_op: F,
) -> Result<Expr>
where
    F: Fn(&str) -> BinaryOp,
{
    let mut inner = pair.into_inner();
    let x = inner.next()
        .ok_or_else(|| anyhow!("empty expression"))?;
    let mut expr = next(x)?;

    while let Some(op) = inner.next() {
        let x = inner.next()
            .ok_or_else(|| anyhow!("empty right hand side"))?;
        let rhs = next(x)?;

        expr = Expr::Binary {
            left: Box::new(expr),
            op: map_op(op.as_str()),
            right: Box::new(rhs),
        };
    }

    Ok(expr)
}

fn parse_unary(pair: Pair<Rule>) -> Result<Expr> {
    let mut ops = Vec::new();
    let mut primary = None;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::unary_op => ops.push(p.as_str()),
            _ => primary = Some(p),
        }
    }
        
    let x = primary
        .ok_or_else(|| anyhow!("empty primary"))?;
    let mut expr = parse_primary(x)?;

    for op in ops.into_iter().rev() {
        expr = Expr::Unary {
            op: match op {
                "-" => UnaryOp::Neg,
                "!" => UnaryOp::Not,
                _ => return Err(anyhow!("unknown unary operator {op}")),
            },
            expr: Box::new(expr),
        };
    }

    Ok(expr)
}

fn parse_primary(pair: Pair<Rule>) -> Result<Expr>
{
    let inner = pair.into_inner().next()
        .ok_or_else(|| anyhow!("empty primary"))?;

    let expr = match inner.as_rule() {
        Rule::column => {
            Expr::Column(inner.as_str()[1..].to_string())
        }
        Rule::literal => parse_literal(inner)?,
        Rule::expression => parse_assignment(inner)?,
        Rule::function_call => parse_call(inner)?,
        _ => return Err(anyhow!("invalid prmary")),
    };

    Ok(expr)
}

fn parse_call(pair: Pair<Rule>) -> Result<Expr>
{
    let mut inner = pair.into_inner();
    let x = inner.next()
        .ok_or_else(|| anyhow!("empty primary"))?;
    let name = x.as_str().to_string();

    let args = inner.map(parse_expr).collect::<Result<Vec<_>, _>>()?;

    Ok(Expr::Call { name, args })
}

fn parse_literal(pair: Pair<Rule>) -> Result<Expr>
{
    let inner = pair.into_inner().next()
        .ok_or_else(|| anyhow!("empty literal"))?;

    let expr = match inner.as_rule() {
        Rule::integer => {
            let x = inner.as_str().parse::<i64>()
                .or(Err(anyhow!("failed to parse integer {}", inner.as_str())))?;
            Expr::Literal(Literal::Int(x))
        },
        Rule::float   => {
            let x = inner.as_str().parse::<f64>()
                .or(Err(anyhow!("failed to parse floating point number {}", inner.as_str())))?;
            Expr::Literal(Literal::Float(x))
        },
        Rule::boolean => Expr::Literal(Literal::Bool(inner.as_str() == "true")),
        _ => return Err(anyhow!("unknown literal {}", inner.as_str())),
    };

    Ok(expr)
}
