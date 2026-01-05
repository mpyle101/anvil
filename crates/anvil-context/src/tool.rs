use std::sync::OnceLock;
use std::collections::HashMap;

use crate::{Symbol, intern};

static TOOL_TYPES: OnceLock<HashMap<Symbol, ToolType>> = OnceLock::new();

pub fn tool_types() -> &'static HashMap<Symbol, ToolType> {
    TOOL_TYPES.get_or_init(|| {
        HashMap::from([
            (intern("count"),     ToolType::Count),
            (intern("describe"),  ToolType::Describe),
            (intern("distinct"),  ToolType::Distinct),
            (intern("drop"),      ToolType::Drop),
            (intern("file"),      ToolType::Fill),
            (intern("filter"),    ToolType::Filter),
            (intern("input"),     ToolType::Input),
            (intern("intersect"), ToolType::Intersect),
            (intern("join"),      ToolType::Join),
            (intern("limit"),     ToolType::Limit),
            (intern("output"),    ToolType::Output),
            (intern("print"),     ToolType::Print),
            (intern("project"),   ToolType::Project),
            (intern("register"),  ToolType::Register),
            (intern("schema"),    ToolType::Schema),
            (intern("select"),    ToolType::Select),
            (intern("sort"),      ToolType::Sort),
            (intern("sql"),       ToolType::Sql),
            (intern("union"),     ToolType::Union),
        ])
    })
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum ToolType {
    Count,
    Describe,
    Distinct,
    Drop,
    Fill,
    Filter,
    Input,
    Intersect,
    Join,
    Limit,
    Output,
    Print,
    Project,
    Register,
    Schema,
    Select,
    Sort,
    Sql,
    Union,
}
