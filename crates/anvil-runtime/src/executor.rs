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

use crate::{tools::{tool, Values}, ExecutionPlan, ExecNode};


#[derive(Default)]
pub struct Executor {
    ctx: SessionContext,
    dfs: HashMap<(NodeIndex, String), DataFrame>,
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

        //println!("\nNODES:\n{nodes:?}");
        for ix in nodes {
            let inputs  = self.collect_inputs(plan, ix);
            //println!("{ix:?} INPUTS: {:?}", inputs.dfs.keys());
            let outputs = match &plan[ix] {
                ExecNode::Tool(tool) => tool.run(inputs, &self.ctx).await?,
                ExecNode::Variable => inputs
            };
            //println!("{ix:?} OUTPUTS: {:?}", outputs.dfs.keys());

            // If there's only one edge out of the node, it's either a single
            // branch or a named port for the default output. If the tool only
            // outputs on the default port, take that value.
            let ports = plan.edges(ix).map(|e| e.weight().port.clone()).collect::<Vec<_>>();
            match ports.len() {
                0 => {},
                1 => {
                    let port = &ports[0];
                    if let Some(df) = outputs.dfs.get("default") {
                        self.dfs.insert((ix, port.clone()), df.clone());
                    } else if let Some(df) = outputs.dfs.get(port) {
                        self.dfs.insert((ix, port.clone()), df.clone());
                    }
                }
                _ => {
                    for port in ports {
                        if let Some(df) = outputs.dfs.get(&port) {
                            self.dfs.insert((ix, port.clone()), df.clone());
                        }
                    }
                }
            }
            plan.edges(ix)
                .filter_map(|edge| {
                    let wt = edge.weight();
                    outputs.dfs.get(&wt.port).map(|df| (wt.port.clone(), df.clone()))
                })
                .for_each(|(port, df)| {
                    self.dfs.insert((ix, port), df);
                });
        }

        Ok(())
    }

    fn collect_inputs(&mut self, plan: &ExecutionPlan, ix: NodeIndex) -> Values
    {
        plan.edges_directed(ix, Incoming)
            .filter_map(|edge| {
                let src  = edge.source();
                let port = &edge.weight().port;
                self.dfs.get(&(src, port.clone()))
                    .map(|df| (port.as_str(), df))
            })
            .fold(Values::default(), |mut v, (port, df)| {
                v.set(df.clone(), port);
                v
            })
    }
}
