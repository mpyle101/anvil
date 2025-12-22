#[derive(Debug)]
pub struct Program {
    pub statements: Vec<Statement>,
}

#[derive(Debug)]
pub struct Statement {
    /// Initial linear flow
    pub flow: Flow,

    /// Optional branch fan-out
    pub branch: Option<BranchBlock>,

    /// Optional variable binding for the entire statement
    pub variable: Option<String>,
}

#[derive(Debug)]
pub struct Flow {
    /// Linear sequence of tools/variables
    pub items: Vec<FlowItem>,
}

#[derive(Debug)]
pub enum FlowItem {
    Tool(ToolRef),
    Variable(String),
}

#[derive(Debug)]
pub struct BranchBlock {
    pub branches: Vec<Branch>,
}

#[derive(Debug)]
pub struct Branch {
    /// Branch name (e.g. "true", "false", "joined")
    pub name: String,

    /// Where this branch sends its data
    pub target: BranchTarget,
}

#[derive(Debug)]
pub enum BranchTarget {
    /// Execute a flow, optionally binding its result
    Flow {
        flow: Flow,
        variable: Option<String>,
    },

    /// Directly bind to an existing variable
    Variable(String),
}

#[derive(Debug)]
pub struct ToolRef {
    pub name: String,
    pub args: Vec<ToolArg>,
}

#[derive(Debug)]
pub enum ToolArg {
    Positional(Literal),
    Keyword { key: String, value: Literal },
}

impl ToolArg {
    pub fn as_string(&self) -> Option<String>
    {
        match self {
            ToolArg::Positional(Literal::String(s)) => Some(s.clone()),
            ToolArg::Keyword { value: Literal::String(s), .. } => Some(s.clone()),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum Literal {
    String(String),
    Number(f64),
    Boolean(bool),
}
