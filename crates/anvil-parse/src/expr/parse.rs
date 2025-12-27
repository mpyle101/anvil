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
        .ok_or_else(|| anyhow!("empty assignment"))?;

    let left = parse_logical(x)?;

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

fn parse_logical(pair: Pair<Rule>) -> Result<Expr>
{
    let mut inner = pair.into_inner();

    let x = inner.next()
        .ok_or_else(|| anyhow!("empty logial expression"))?;
    let mut expr = parse_comparison(x)?;

    while let Some(op) = inner.next() {
        let x = inner.next()
            .ok_or_else(|| anyhow!("empty rhs expression"))?;
        let rhs = parse_comparison(x)?;

        let op = match op.as_str() {
            "&&" => BinaryOp::And,
            "||" => BinaryOp::Or,
            _ => return Err(anyhow!("unknown logical operator {op}"))
        };

        expr = Expr::Binary {
            left: Box::new(expr),
            op,
            right: Box::new(rhs),
        };
    }

    Ok(expr)
}

fn parse_comparison(pair: Pair<Rule>) -> Result<Expr>
{
    let mut inner = pair.into_inner();
    let x = inner.next()
        .ok_or_else(|| anyhow!("empty comparison"))?;
    let mut expr = parse_arithmetic(x)?;

    while let Some(op) = inner.next() {
        let x = inner.next()
            .ok_or_else(|| anyhow!("empty right hand side"))?;
        let rhs = parse_arithmetic(x)?;

        let bin_op = match op.as_str() {
            "==" => BinaryOp::Eq,
            "!=" => BinaryOp::Ne,
            ">"  => BinaryOp::Gt,
            "<"  => BinaryOp::Lt,
            ">=" => BinaryOp::Ge,
            "<=" => BinaryOp::Le,
            _ => return Err(anyhow!("unrecognized comparison {}", op.as_str()))
        };

        expr = Expr::Binary {
            left: Box::new(expr),
            op: bin_op,
            right: Box::new(rhs),
        };
    }

    Ok(expr)
}

fn parse_arithmetic(pair: Pair<Rule>) -> Result<Expr>
{
    let mut ops = Vec::new();
    let mut output = Vec::new();

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::unary    => output.push(parse_unary(p)?),
            Rule::literal  => output.push(parse_literal(p)?),
            Rule::arith_op => ops.push(p.as_str()),
            _ => return Err(anyhow!("unrecognized math {}", p.as_str())),
        }
    }

    // First pass: * and /
    let mut i = 0;
    while i < ops.len() {
        if ops[i] == "*" || ops[i] == "/" {
            let rhs = output.remove(i + 1);
            let lhs = output.remove(i);
            let op = if ops[i] == "*" {
                BinaryOp::Mul
            } else {
                BinaryOp::Div
            };
            ops.remove(i);
            output.insert(i, Expr::Binary {
                left: Box::new(lhs),
                op,
                right: Box::new(rhs),
            });
        } else {
            i += 1;
        }
    }

    // Second pass: + and -
    let mut expr = output.remove(0);
    for (op, rhs) in ops.into_iter().zip(output) {
        let op = if op == "+" {
            BinaryOp::Add
        } else {
            BinaryOp::Sub
        };

        expr = Expr::Binary {
            left: Box::new(expr),
            op,
            right: Box::new(rhs),
        };
    }

    Ok(expr)
}

fn parse_unary(pair: Pair<Rule>) -> Result<Expr>
{
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
        Rule::column => parse_column(inner)?,
        Rule::literal => parse_literal(inner)?,
        Rule::expression => parse_assignment(inner)?,
        Rule::function_call => parse_call(inner)?,
        _ => return Err(anyhow!("invalid prmary {:?}", inner)),
    };

    Ok(expr)
}

fn parse_column(pair: Pair<Rule>) -> Result<Expr>
{
    let inner = pair.into_inner().next()
        .ok_or_else(|| anyhow!("empty column name"))?;

    let name = match inner.as_rule() {
        Rule::identifier => inner.as_str().to_string(),
        Rule::quoted_identifier => {
            let s = inner.as_str();
            s[1..s.len() - 1].to_string() // strip quotes
        }
        _ => return Err(anyhow!("invalid column name")),
    };

    Ok(Expr::Column(name))
}

fn parse_call(pair: Pair<Rule>) -> Result<Expr>
{
    let mut inner = pair.into_inner();
    let x = inner.next()
        .ok_or_else(|| anyhow!("empty function"))?;
    let name = x.as_str().to_string();

    let args = inner.map(parse_logical).collect::<Result<Vec<_>, _>>()?;

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
