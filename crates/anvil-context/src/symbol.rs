use std::sync::{OnceLock, RwLock};

use string_interner::DefaultStringInterner;
use string_interner::symbol::SymbolU32;

pub type Symbol = SymbolU32;
pub type Interner = DefaultStringInterner;

static STAR: OnceLock<Symbol> = OnceLock::new();
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

pub fn star() -> Symbol {
    *STAR.get_or_init(|| intern("*"))
}