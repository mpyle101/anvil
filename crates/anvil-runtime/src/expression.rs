use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use anyhow::{anyhow, Result};
use datafusion::prelude::*;
use datafusion::logical_expr::{Expr, Operator, ScalarUDF};

use anvil_parse::expr::ast;

type Funcs = HashMap<String, Arc<ScalarUDF>>;

static FUNCTIONS: LazyLock<Funcs> = LazyLock::new(|| {
    datafusion::functions::all_default_functions()
        .iter()
        .map(|f| (f.name().to_string(), f.clone()))
        .collect::<Funcs>()
});

pub fn eval_expression(expr: &ast::Expr) -> Result<Expr>
{
    let expr = match expr {
        ast::Expr::Column(name) => {
            col(format!(r#""{name}""#))
        }
        ast::Expr::Literal(l) => {
            eval_literal(l)
        }
        ast::Expr::Unary { op, expr } => {
            let inner = eval_expression(expr)?;

            match op {
                ast::UnaryOp::Neg => Expr::Negative(Box::new(inner)),
                ast::UnaryOp::Not => not(inner),
            }
        }
        ast::Expr::Binary { left, op, right } => {
            binary_expr(
                eval_expression(left)?,
                eval_binary_op(*op),
                eval_expression(right)?,
            )
        }
        ast::Expr::Call { name, args } => {
            eval_function_call(name, args)?
        }
        ast::Expr::Assign { target, value } => {
            // Assignment is semantic — only valid in formula/projection
            let expr = eval_expression(value)?;
            expr.alias(target)
        }
    };

    Ok(expr)
}

fn eval_literal(litval: &ast::Literal) -> Expr
{
    match litval {
        ast::Literal::Int(v)   => lit(*v),
        ast::Literal::Float(v) => lit(*v),
        ast::Literal::Bool(v)  => lit(*v),
    }
}

fn eval_binary_op(op: ast::BinaryOp) -> Operator
{
    match op {
        ast::BinaryOp::Add => Operator::Plus,
        ast::BinaryOp::Sub => Operator::Minus,
        ast::BinaryOp::Mul => Operator::Multiply,
        ast::BinaryOp::Div => Operator::Divide,
        ast::BinaryOp::Mod => Operator::Modulo,

        ast::BinaryOp::Eq => Operator::Eq,
        ast::BinaryOp::Ne => Operator::NotEq,
        ast::BinaryOp::Gt => Operator::Gt,
        ast::BinaryOp::Lt => Operator::Lt,
        ast::BinaryOp::Ge => Operator::GtEq,
        ast::BinaryOp::Le => Operator::LtEq,

        ast::BinaryOp::And => Operator::And,
        ast::BinaryOp::Or  => Operator::Or,
    }
}

fn eval_function_call(name: &str, args: &[ast::Expr]) -> Result<Expr>
{
    let args = args.iter()
        .map(eval_expression)
        .collect::<Result<Vec<_>>>()?;

    match FUNCTIONS.get(name) {
        Some(func) => Ok(func.call(args)),
        None => Err(anyhow!("unknown function '{name}'"))
    }
}
