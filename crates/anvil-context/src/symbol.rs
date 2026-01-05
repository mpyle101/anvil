use std::sync::{OnceLock, RwLock};

use string_interner::DefaultStringInterner;
use string_interner::symbol::SymbolU32;

pub type Symbol = SymbolU32;
pub type Interner = DefaultStringInterner;

static SYMS: OnceLock<Syms> = OnceLock::new();
static INTERNER: OnceLock<RwLock<Interner>> = OnceLock::new();

fn interner() -> &'static RwLock<Interner>
{
    INTERNER.get_or_init(|| RwLock::new(Interner::default()))
}

pub fn intern(s: &str) -> Symbol
{
    interner().write().unwrap().get_or_intern(s)
}

pub fn resolve(sym: Symbol) -> &'static str
{
    let i = interner().read().unwrap();
    let s = i.resolve(sym).unwrap();
    Box::leak(s.to_string().into_boxed_str())
}

pub fn syms() -> &'static Syms
{
    SYMS.get_or_init(|| {
        Syms {
            default: intern("*"),
            left: intern("left"),
            right: intern("right"),
            port_true: intern("true"),
            port_false: intern("false"),
            join_type: intern("type"),
            cols_lt: intern("cols_lt"),
            cols_rt: intern("cols_rt")
        }
    })
}

pub struct Syms {
    pub default: Symbol,
    pub left: Symbol,
    pub right: Symbol,
    pub port_true: Symbol,
    pub port_false: Symbol,
    pub join_type: Symbol,
    pub cols_lt: Symbol,
    pub cols_rt: Symbol,
}