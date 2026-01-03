#[derive(Debug)]
pub struct Program {
    pub statements: Vec<Statement>,
}

#[derive(Debug)]
pub struct Statement {
    /// Initial linear flow
    pub flow: Flow,

    /// Optional branch fan-out
    pub branches: Option<Vec<Branch>>,

    /// Optional variable binding for the entire statement
    pub variable: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Flow {
    /// Linear sequence of tools/variables
    pub items: Vec<FlowItem>,
}

#[derive(Clone, Debug)]
pub enum FlowItem {
    Tool(ToolRef),
    Variable(String),
}

#[derive(Debug)]
pub struct Branch {
    /// Branch name (e.g. "true", "false", "joined")
    pub name: String,

    /// Where this branch sends its data
    pub target: Target,
}

#[derive(Debug)]
pub enum Target {
    /// Execute a flow, optionally binding its result
    Flow {
        flow: Flow,
        variable: Option<String>,
    },

    /// Directly bind to an existing variable
    Variable(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ToolId(pub usize);

#[derive(Clone, Debug)]
pub struct ToolRef {
    pub id: ToolId,
    pub name: String,
    pub args: Vec<ToolArg>,
}

#[derive(Clone, Debug)]
pub enum ToolArg {
    Keyword { ident: String, value: ArgValue },
    Positional(ArgValue),
}

#[derive(Clone, Debug)]
pub enum ArgValue {
    Flow(Flow),
    Ident(String),
    Boolean(bool),
    Integer(i64),
    String(String),
}
