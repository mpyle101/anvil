use anyhow::{anyhow, Result};
use datafusion::arrow::datatypes::DataType;
use pest::Parser;
use pest::iterators::Pair;

use anvil_context::{intern, Symbol};

use crate::anvil::ast::*;
use crate::anvil::{AnvilParser, Rule};


pub fn build_program(builder: &mut ASTBuilder, input: &str) -> Result<Program>
{
    let mut pairs = AnvilParser::parse(Rule::PROGRAM, input)?;
    let program = pairs.next().unwrap();

    builder.build(program)
}

pub fn build_statement(builder: &mut ASTBuilder, input: &str) -> Result<Statement>
{
    let mut pairs = AnvilParser::parse(Rule::STATEMENT, input)?;
    let statement = pairs.next().unwrap();

    builder.build_statement(statement)
}


#[derive(Default)]
pub struct ASTBuilder {
    next_tool_id: usize,
}

impl ASTBuilder {
    pub fn new() -> Self
    {
        Self { next_tool_id: 1 }
    }

    fn get_next_id(&mut self) -> ToolId
    {
        let id = self.next_tool_id;
        self.next_tool_id += 1;

        ToolId(id)
    }

    fn build(&mut self, program: Pair<Rule>) -> Result<Program>
    {
        let mut statements = Vec::new();

        for pair in program.into_inner() {
            if pair.as_rule() == Rule::STATEMENT {
                statements.push(self.build_statement(pair)?);
            }
        }

        Ok(Program { statements })
    }

    fn build_statement(&mut self, pair: Pair<Rule>) -> Result<Statement>
    {
        let mut flow = None;
        let mut branches = None;
        let mut variable = None;

        for inner in pair.into_inner() {
            match inner.as_rule() {
                Rule::FLOW => {
                    flow = Some(self.build_flow(inner)?);
                }
                Rule::BRANCH_BLOCK => {
                    branches = Some(self.build_branches(inner)?);
                }
                Rule::OUTPUT_BINDING => {
                    variable = Some(self.build_variable_binding(inner)?);
                }
                _ => {}
            }
        }

        Ok(Statement {
            flow: flow.ok_or_else(|| anyhow!("statement missing flow"))?,
            branches,
            variable,
        })
    }

    fn build_branches(&mut self, pair: Pair<Rule>) -> Result<Vec<Branch>>
    {
        let mut branches = Vec::new();

        for inner in pair.into_inner() {
            if inner.as_rule() == Rule::BRANCHES {
                for item in inner.into_inner() {
                    if item.as_rule() == Rule::BRANCH {
                        branches.push(self.build_branch(item)?);
                    }
                }
            }
        }

        Ok(branches)
    }

    fn build_branch(&mut self, pair: Pair<Rule>) -> Result<Branch>
    {
        let mut inner = pair.into_inner();
        let name   = inner.next().unwrap();
        let target = inner.next().unwrap();
        let target = self.build_target(target)?;

        Ok(Branch {
            name: intern(name.as_str()),
            target,
        })
    }

    fn build_target(&mut self, pair: Pair<Rule>) -> Result<Target>
    {
        let inner = pair.into_inner().next()
            .ok_or_else(|| anyhow!("branch target must have one child"))?;

        match inner.as_rule() {
            Rule::VARIABLE => {
                Ok(Target::Variable(intern(inner.as_str())))
            }
            Rule::FLOW => {
                let flow = self.build_flow(inner)?;
                Ok(Target::Flow { flow, variable: None })
            }
            _ => Err(anyhow!("invalid branch target")),
        }
    }

    fn build_variable_binding(&self, pair: Pair<Rule>) -> Result<Symbol>
    {
        let var = pair
            .into_inner()
            .find(|p| p.as_rule() == Rule::VARIABLE)
            .ok_or_else(|| anyhow!("output binding missing variable"))?;

        Ok(intern(var.as_str()))
    }

    fn build_flow(&mut self, flow: Pair<Rule>) -> Result<Flow>
    {
        let mut items = vec![];

        for flow_item in flow.into_inner() {
            match flow_item.as_rule() {
                Rule::PIPE => {},
                Rule::TOOL_REF => {
                    items.push(FlowItem::Tool(self.build_tool_ref(flow_item)?))
                }
                Rule::VARIABLE => {
                    items.push(FlowItem::Variable(intern(flow_item.as_str())))
                }
                _ => return Err(anyhow!("invalid flow item: {:?}", flow_item.as_rule()))
            }
        }

        Ok(Flow { items })
    }

    fn build_tool_ref(&mut self, pair: Pair<Rule>) -> Result<ToolRef>
    {
        let mut inner = pair.into_inner();
        let name = intern(inner.next().unwrap().as_str());

        let mut args = vec![];
        if let Some(tool_args) = inner.next() {
            for arg in tool_args.into_inner() {
                match arg.as_rule() {
                    Rule::POSITIONAL => {
                        let value = arg.into_inner().next().unwrap();
                        args.push(ToolArg::Positional(self.build_arg_value(value)?))
                    }
                    Rule::KEYWORD => {
                        args.push(self.build_keyword(arg)?)
                    }
                    _ => return Err(anyhow!("unexpected tool argument {:?}", arg.as_rule()))
                }
            }
        }

        Ok(ToolRef { id: self.get_next_id(), name, args })
    }

    fn build_keyword(&mut self, pair: Pair<Rule>) -> Result<ToolArg>
    {
        let ident: Symbol;
        let value: ArgValue;
        let dtype: Option<DataType>;

        let mut inner = pair.into_inner();
        match inner.len() {
            0 => return Err(anyhow!("keyword identifier not found")),
            1 => return Err(anyhow!("keyword requires value: {}", inner.as_str())),
            2 => {
                ident = intern(inner.next().unwrap().as_str());
                dtype = None;
                value = self.build_arg_value(inner.next().unwrap())?;
            }
            3 => {
                ident = intern(inner.next().unwrap().as_str());
                dtype = Some(self.build_datatype(inner.next().unwrap())?);
                value = self.build_arg_value(inner.next().unwrap())?;
            }
            _ => return Err(anyhow!("invalid keyword expresion: {}", inner.as_str())),
        }

        Ok(ToolArg::Keyword { ident, value, dtype })
    }

    fn build_arg_value(&mut self, pair: Pair<Rule>) -> Result<ArgValue>
    {
        let inner = pair.into_inner().next()
            .ok_or_else(|| anyhow!("empty arg value encountered"))?;

        let v = match inner.as_rule() {
            Rule::FLOW       => ArgValue::Flow(self.build_flow(inner)?),
            Rule::LITERAL    => self.build_literal(inner)?,
            Rule::IDENTIFIER => ArgValue::Ident(inner.as_str().to_string()),
            _ => return Err(anyhow!("unexpected arg value {:?}", inner.as_rule()))
        };

        Ok(v)
    }

    fn build_literal(&self, pair: Pair<Rule>) -> Result<ArgValue>
    {
        let inner = pair.into_inner().next().unwrap();

        let av = match inner.as_rule() {
            Rule::BOOLEAN => ArgValue::Boolean(inner.as_str() == "true"),
            Rule::NUMBER  => ArgValue::Integer(inner.as_str().parse::<i64>()?),
            Rule::STRING  => {
                let s = inner.as_str();
                let v = &s[1..s.len() - 1];
                ArgValue::String(v.to_string())
            }
            _ => return Err(anyhow!("unexpected literal {:?}", inner.as_rule()))
        };

        Ok(av)
    }

    fn build_datatype(&self, pair: Pair<Rule>) -> Result<DataType>
    {
        use datafusion::arrow::datatypes::TimeUnit;

        let mut inner = pair.into_inner();

        let type_name = inner.next().ok_or_else(|| anyhow!("empty datatype identifier encountered"))?;
        let dtype = match type_name.as_str() {
            "bool" => DataType::Boolean,
            "f64"  => DataType::Float64,
            "f32"  => DataType::Float32,
            "i32"  => DataType::Int32,
            "i64"  => DataType::Int64,
            "u32"  => DataType::UInt32,
            "u64"  => DataType::UInt64,
            "s"    => DataType::Timestamp(TimeUnit::Second, None),
            "us"   => DataType::Timestamp(TimeUnit::Microsecond, None),
            "ms"   => DataType::Timestamp(TimeUnit::Millisecond, None),
            "ns"   => DataType::Timestamp(TimeUnit::Nanosecond, None),
            "utf8" => DataType::Utf8,
            "decimal" => {
                if inner.len() != 2 {
                    return Err(anyhow!("decimal types require precision and scale"))
                }
                let prec  = get_integer(inner.next().unwrap())? as u8;
                let scale = get_integer(inner.next().unwrap())? as i8;
                DataType::Decimal64(prec, scale)
            }
            "bigdec" => {
                if inner.len() != 2 {
                    return Err(anyhow!("big decimal types require precision and scale"))
                }
                let prec  = get_integer(inner.next().unwrap())? as u8;
                let scale = get_integer(inner.next().unwrap())? as i8;
                DataType::Decimal128(prec, scale)
            }
            "hugedec" => {
                if inner.len() != 2 {
                    return Err(anyhow!("huge decimal types require precision and scale"))
                }
                let prec  = get_integer(inner.next().unwrap())? as u8;
                let scale = get_integer(inner.next().unwrap())? as i8;
                DataType::Decimal256(prec, scale)
            }
            _ => return Err(anyhow!("unknown datatype {}", type_name.as_str()))
        };

        Ok(dtype)
    }
}


fn get_integer(pair: Pair<Rule>) -> Result<i64>
{
    let v = if let Rule::NUMBER = pair.as_rule() {
        pair.as_str().parse::<i64>()?
    } else {
        return Err(anyhow!("unexpected integer {:?}", pair.as_rule()))
    };

    Ok(v)
}
