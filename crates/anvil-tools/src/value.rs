use datafusion::dataframe::DataFrame;

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum Value {
    /// No output, e.g., for terminal tools
    None,
    /// Single DataFrame, e.g., most tools
    Single(DataFrame),
    /// Multiple DataFrames, e.g., branching tools like filter
    Multiple(Vec<DataFrame>),
}
