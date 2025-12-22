use anvil_runtime::Interpreter;


#[tokio::main]
async fn main() -> anyhow::Result<()>
{
    let mut interpreter = Interpreter::default();

    let source = std::fs::read_to_string("./scripts/jester.anvil")?;
    let program = anvil_parse::parser::parse_program(&source)?;
    interpreter.eval(program).await?;

    Ok(())
}
