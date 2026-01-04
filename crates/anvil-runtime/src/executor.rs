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

        let (sources, nodes) = nodes.iter()
            .fold((vec![], vec![]), |(mut src, mut nds), ix| {
                if plan[*ix].is_source() { src.push(*ix) } else { nds.push(*ix) }
                (src, nds)
            });

        // Run source tools first
        self.exec_nodes(&sources, plan).await?;
        self.exec_nodes(&nodes, plan).await?;

        Ok(())
    }

    async fn exec_nodes(&mut self, nodes: &[NodeIndex], plan: &ExecutionPlan) -> Result<()>
    {
        for ix in nodes {
            let inputs  = self.collect_inputs(plan, *ix);
            let outputs = match &plan[*ix] {
                ExecNode::Tool(tool) => {
                    tool.run(inputs, &self.ctx).await?
                },
                ExecNode::Variable => inputs,
            };
            self.apply_outputs(plan, *ix, outputs);
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

    fn apply_outputs(&mut self, plan: &ExecutionPlan, ix: NodeIndex, values: Values)
    {
        // If there's only one edge out of the node, it's either a single
        // branch or a named port for the default output. If the tool only
        // outputs on the default port, take that value.
        let ports = plan.edges(ix)
            .map(|e| e.weight().port.clone())
            .collect::<Vec<_>>();

        match ports.len() {
            0 => {},
            1 => {
                let port = &ports[0];
                if let Some(df) = values.dfs.get("default") {
                    self.dfs.insert((ix, port.clone()), df.clone());
                } else if let Some(df) = values.dfs.get(port) {
                    self.dfs.insert((ix, port.clone()), df.clone());
                }
            }
            _ => {
                for port in ports {
                    if let Some(df) = values.dfs.get(&port) {
                        self.dfs.insert((ix, port.clone()), df.clone());
                    }
                }
            }
        }
    }
}
