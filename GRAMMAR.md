# Anvil Grammar Specification

This document formally describes the Anvil language grammar and its semantics. Anvil is a **dataflow-oriented DSL** where programs describe pipelines of tools operating on tabular data. Programs are parsed into an AST and then transformed into an execution graph.

The grammar is implemented using **Pest** and is reproduced here with explanations and examples.

---

## Lexical Structure

### Whitespace and Comments

Whitespace is generally insignificant and may appear between tokens. Comments begin with `#` and run to the end of the line.

```pest
COMMENT    = _{ "#"  ~ (!NEWLINE ~ ANY)* ~ NEWLINE? }
WHITESPACE = _{ " " | "\t" | NEWLINE | COMMENT }
NEWLINE    = _{ "\r\n" | "\n" }
```

---

## Programs and Statements

An Anvil program is a sequence of **statements**.

```pest
PROGRAM    = { SOI ~ STATEMENT* ~ EOI }
```

Each statement represents a complete dataflow expression and must end with a semicolon.

```pest
STATEMENT  = { FLOW ~ BRANCH_BLOCK? ~ OUTPUT_BINDING? ~ ";" }
```

A statement may:

* consist solely of a flow
* fan out into named branches
* bind its final output to a variable

---

## Flows

A **flow** is a left-to-right pipeline of tools and/or variables.

```pest
FLOW = { (TOOL_REF | VARIABLE) ~ (PIPE ~ (TOOL_REF | VARIABLE))* }
```

The pipe operator (`|`) connects the output of one step to the input of the next.

### Example

```anvil
[input: './data/file.parquet'] | [schema] | [print];
```

Variables may appear anywhere a tool can appear.

```anvil
df | [select: 'id,name'] | [print];
```

---

## Output Binding

A statement may bind its final output to a variable using the bind operator (`>`).

```pest
OUTPUT_BINDING = { BIND ~ VARIABLE }
BIND = { ">" }
```

### Example

```anvil
[input: './data/file.parquet'] | [select: 'id'] > df;
```

---

## Branching (Fan-Out)

Branching allows a tool to emit **multiple named outputs**.

```pest
BRANCH_BLOCK  = { ":" ~ BRANCHES }
BRANCHES      = { BRANCH ~ ("," ~ BRANCH)* }
BRANCH        = { IDENTIFIER ~ "=>" ~ TARGET }
TARGET        = { VARIABLE | FLOW ~ OUTPUT_BINDING? }
```

### Semantics

* Branching always applies to the **immediately preceding flow**
* Branch names must correspond to outputs produced by the tool
* Branches do **not** implicitly rejoin
* Merging must be done explicitly via tools such as `join`, `union`, or `intersect`

### Example

```anvil
[input: './data/messy.parquet']
| [filter: '$three == true']:
    true  => [print],
    false => df;

df | [print];
```

---

## Tool Invocation

Tools are invoked using square brackets.

```pest
TOOL_REF = { "[" ~ IDENTIFIER ~ (":" ~ TOOL_ARGS)? ~ "]" }
```

### Tool Arguments

Tools may accept positional arguments, keyword arguments, or both.

```pest
TOOL_ARGS = {
    POSITIONAL ~ ("," ~ POSITIONAL)* ~ ("," ~ KEYWORD ~ ("," ~ KEYWORD)*)? |
    KEYWORD ~ ("," ~ KEYWORD)*
}

KEYWORD    = { IDENTIFIER ~ "=" ~ VALUE }
POSITIONAL = { !(IDENTIFIER ~ "=") ~ VALUE }
```

Positional arguments must appear **before** keyword arguments.

---

## Values

Tool arguments may be literals, identifiers, or embedded flows.

```pest
VALUE = { LITERAL | IDENTIFIER | "(" ~ FLOW ~ ")" }
```

### Flow Values

Flows wrapped in parentheses are parsed as **subflows**. These allow tools to accept entire pipelines as inputs.

```anvil
[join:
  df_lt=(left_df),
  df_rt=([input: './data/right.parquet'] | [select: 'id,name'])
]
```

---

## Literals and Identifiers

```pest
LITERAL     = { STRING | NUMBER | BOOLEAN }
STRING      = @{ "'" ~ (!"'" ~ ANY)* ~ "'" }
NUMBER      = @{ "-"? ~ ASCII_DIGIT+ }
BOOLEAN     = { "true" | "false" }
```

Identifiers and variables share the same lexical form but differ semantically.

```pest
IDENTIFIER  = @{ (ASCII_ALPHANUMERIC | "_")+ }
VARIABLE    = @{ (ASCII_ALPHANUMERIC | "_")+ }
```

---

## Tools

The following tools are currently available:

* `input` — read files into a dataframe
* `output` — write dataframes to disk
* `register` — register a dataframe as a SQL table
* `schema` — produce a dataframe describing schema
* `describe` — produce dataset metadata
* `select` — select columns using DataFusion expressions
* `filter` — filter rows using expressions
* `print` — write dataframe to stdout
* `limit` — limit number of rows
* `union` — union dataframes
* `intersect` — intersect dataframes
* `join` — join dataframes
* `sort` — sort using expressions
* `project` — compute new columns from expressions
* `sql` — execute SQL against registered tables

Tool arity and semantics are validated during execution graph construction and execution, not during parsing.

---

## Design Notes

* The grammar enforces **syntax only**; semantic validation is deferred
* Variables remain first-class nodes in the execution graph
* Branching represents fan-out only
* Fan-in must be modeled explicitly with tools
* Parenthesized flows enable graph composition without grammar-level grouping constructs

---

This grammar is intentionally conservative to keep parsing deterministic and error messages readable while allowing expressive dataflow graphs.
