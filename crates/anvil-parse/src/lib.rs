use pest_derive::Parser;


#[derive(Parser)]
#[grammar = "anvil.pest"]
pub struct AnvilParser;

pub mod ast;
pub mod parser;

