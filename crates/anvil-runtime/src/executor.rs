#![allow(dead_code, unused)]

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use petgraph::{
    Incoming,
    algo::toposort,
    graph::NodeIndex,
    visit::EdgeRef,
};

use crate::{ExecutionPlan, ExecNode};
use crate::tools::{tool, Values};

type Inputs = HashMap<NodeIndex, Values>;

#[derive(Default)]
pub struct Executor {
    ctx: SessionContext,
    dfs: Inputs,
}

impl Executor {
    pub fn reset(&mut self)
    {
        self.ctx = SessionContext::default();
    }

    pub async fn run(&mut self, plan: &ExecutionPlan) -> Result<()>
    {
        let nodes = match toposort(plan, None) {
            Ok(nodes)  => nodes,
            Err(cycle) => return Err(anyhow!("cycle detected at node {:?}", cycle.node_id()))
        };

        // Separate the nodes into pure sources and nodes requiring
        // input in one way or another (pipeline, argument, variable).
        let (sources, sinks) = nodes.iter()
            .fold((vec![], vec![]), |(mut src, mut nds), ix| {
                if plan[*ix].is_source() { src.push(*ix) } else { nds.push(*ix) }
                (src, nds)
            });

        // Run source tools first
        self.exec_nodes(&sources, plan).await?;
        self.exec_nodes(&sinks, plan).await?;

        Ok(())
    }

    async fn exec_nodes(&mut self, nodes: &[NodeIndex], plan: &ExecutionPlan) -> Result<()>
    {
        for ix in nodes {
            let inputs  = self.dfs.remove(ix);
            let outputs = match &plan[*ix] {
                ExecNode::Tool(tool) => {
                    tool.run(inputs, &self.ctx).await?
                },
                ExecNode::Variable(name) => {
                    if let Some(values) = inputs {
                        self.dfs.insert(*ix, values.clone());
                        values
                    } else {
                        return Err(anyhow!("uninitialized variable: {name}"))
                    }
                }
            };

            if !outputs.dfs.is_empty() {
                for edge in plan.edges(*ix) {
                    let e = edge.weight();
                    let t = edge.target();
                    let node = &plan[t];

                    let mut v = self.dfs.entry(t).or_default();
                    for (p, df) in &outputs.dfs {
                        match node {
                            ExecNode::Tool(tool) => {
                                if *p == e.port || p == "*" || e.port == "*" {
                                    v.set(&e.port, df.clone())
                                }
                            }
                            ExecNode::Variable(_) => {
                                v.set("*", df.clone())
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

}
