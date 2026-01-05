use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use petgraph::dot::{Config, Dot};

use anvil_context::{intern, resolve, syms};
use anvil_parse::ASTBuilder;
use anvil_runtime::{run_repl, Executor, ExecNode, ExecEdge, Planner};


#[derive(Parser)]
struct Cli {
    /// Path to the Anvil script to run, not provided run repl
    script: Option<PathBuf>,

    /// Write execution graph in DOT format (optional output path)
    #[arg(
        short = 'd',
        long = "dot",
        value_name = "FILE",
        num_args = 0..=1,
    )]
    dot: Option<Option<PathBuf>>,
}

#[tokio::main]
async fn main() -> Result<()>
{
    let mut builder  = ASTBuilder::new();
    let mut planner  = Planner::default();
    let mut executor = Executor::default();

    let cli = Cli::parse();
    if let Some(script) = cli.script {
        let source = std::fs::read_to_string(&script)?;
        if let Some(cmd) = cli.dot {
            let program = anvil_parse::build_program(&mut builder, &source)?;
            let plan = planner.build(program)?;
            let dot  = Dot::with_attr_getters(
                &plan,
                &[Config::NodeIndexLabel], // optional
                &|_, edge| edge_attrs(edge.weight()),
                &|_, (_, node)| node_attrs(node),
            );

            if let Some(path) = cmd {
                std::fs::write(path, dot.to_string())?;
            } else {
                println!("{dot}")
            }
        } else {
            let program = anvil_parse::build_program(&mut builder, &source)?;
            let plan = planner.build(program)?;
            executor.run(plan).await?
        }
    } else {
        run_repl(&mut builder, &mut planner, &mut executor).await?;
    }

    Ok(())
}

fn node_attrs(node: &ExecNode) -> String
{
    match node {
        ExecNode::Tool(tool) => {
            format!(
                r#"label="{} ({})", shape=box, style=filled, fillcolor=lightblue"#,
                tool.name(), tool.id()
            )
        }
        ExecNode::Variable(sym) => {
            format!(
                r#"label="{}", shape=ellipse, style=filled, fillcolor=lightgray"#,
                resolve(*sym)
            )
        }
    }
}

fn edge_attrs(edge: &ExecEdge) -> String
{
    if edge.port == syms().default {
        r#"label="""#.to_string()
    } else if edge.port == intern("true") {
        r#"label="true", color=green"#.to_string()
    } else if edge.port == intern("false") {
        r#"label="false", color=red"#.to_string()
    } else {
        format!(r#"label="{}""#, resolve(edge.port))
    }
}
