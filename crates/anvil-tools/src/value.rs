use datafusion::dataframe::DataFrame;

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum Value {
    /// No output, e.g., for terminal tools
    None,
    /// Single DataFrame, e.g., most tools
    Single(Data),
    /// Multiple DataFrames, e.g., branching tools like filter
    Multiple(Vec<Data>),
}

#[derive(Clone, Debug)]
pub struct Data {
    pub df: DataFrame,
    pub src: String,
}