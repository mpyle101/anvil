use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "anvil/anvil.pest"]
pub struct AnvilParser;

pub mod ast;
pub mod parse;