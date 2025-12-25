mod expression;
mod interpreter;
mod tools;

pub use tools::Value;
pub use expression::eval_expression;
pub use interpreter::eval_program;
