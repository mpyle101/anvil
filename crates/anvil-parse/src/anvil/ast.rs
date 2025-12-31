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
    Group(Vec<GroupItem>),
    Tool(ToolRef),
    Variable(String),
}

#[derive(Debug)]
pub enum FlowEnd {
    Tool(ToolRef),
    Variable(String),
}

#[derive(Debug)]
pub struct GroupItem {
    pub name: String,
    pub flow: Flow,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ToolId(pub usize);

impl std::fmt::Display for ToolId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result
    {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug)]
pub struct ToolRef {
    pub id: ToolId,
    pub name: String,
    pub args: Vec<ToolArg>,
}

#[derive(Clone, Debug)]
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
    Boolean(bool),
    Integer(i64),
    String(String),
}

impl std::fmt::Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result
    {
        use Literal::*;

        match self {
            Boolean(v) => write!(f, "{v}"),
            Integer(v) => write!(f, "{v}"),
            String(v)  => write!(f, "{v}"),
        }
    }
}