mod expression;
mod interpreter;
mod repl;
mod tools;

pub use expression::eval_expression;
pub use interpreter::{Interpreter, eval_program};
pub use repl::run_repl;
pub use tools::Value;
