use std::collections::HashMap;

use datafusion::prelude::DataFrame;

use anvil_context::{syms, Symbol};


#[derive(Clone, Debug, Default)]
pub struct Values {
    pub dfs: HashMap<Symbol, DataFrame>,
}

impl Values {
    pub fn new(df: DataFrame) -> Self
    {
        Values { dfs: HashMap::from([(syms().default, df)]) }
    }

    pub fn get_one(&self) -> Option<&DataFrame>
    {
        self.dfs.values().next()
    }

    pub fn set(&mut self, port: Symbol, df: DataFrame)
    {
        self.dfs.insert(port, df);
    }
}
