use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "expr/expr.pest"]
pub struct ExprParser;

pub mod ast;
pub mod eval;
pub mod parse;