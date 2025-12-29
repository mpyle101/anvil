# Anvil

Anvil is an experimental **dataflow language** built on top of **Apache Arrow DataFusion** and implemented in **Rust**. It is designed to make dataframe-oriented pipelines explicit, composable, and inspectable, with a strong focus on **data lineage**.

Anvil scripts describe *how data flows through tools*, not just what SQL query is executed. This makes pipelines easier to reason about, debug, and eventually optimize.

> ⚠️ Anvil is a work in progress. Syntax and semantics may evolve.

---

## Core Concepts

### Tools

Tools are the fundamental execution units in Anvil. A tool:

* consumes zero or more dataframes
* produces zero, one, or many dataframes
* has a unique identity for lineage tracking

Tools are written in **square brackets**:

```anvil
[input: './data/users.parquet']
```

### Flows

A **flow** is a pipeline of tools connected with `|`.

```anvil
[input: './data/users.parquet']
  | [filter: '$age > 30']
  | [select: 'id,name,age']
  | [print];
```

### Variables

You can bind the output of a flow to a variable using `|>`:

```anvil
[input: './data/users.parquet'] |> users;
```

Variables can be referenced later in other flows:

```anvil
users | [show];
```

### Grouped Inputs (Multiple Inputs)

Some tools (like `join`) consume more than one input. Anvil supports **grouped inputs** using parentheses:

```anvil
([input: './data/left.parquet'], users)
  | [join: type='inner' left='id' right='id']
  | [show];
```

Grouped inputs produce multiple dataframes internally and are validated by tool arity checks.

---

## Expressions

Anvil has its own expression language (separate from SQL) which is later translated into DataFusion expressions.

### Expression syntax

* Infix operators: `+ - * / == != > < >= <= && ||`
* Assignment: `=`
* Booleans: `true`, `false`
* Integers (`i64`) and floating point numbers (`f64`)
* Column references use `$` notation

Examples:

```anvil
$age > 30
$total = $price * $quantity
($a > 10) && ($b < 5)
```

### Tools using expressions

```anvil
[filter: '$distance > 1000 && $active == true']

[formula: '$total = $col1 + $col2 - 34.5']
```

---

## Branching

Anvil supports **branching flows**, where a single input fans out into multiple pipelines:

```anvil
df:
  adults  => [filter: '$age >= 18'],
  minors  => [filter: '$age < 18'];
```

Branches can later be rejoined or consumed independently.

---

## Data Lineage

Every tool invocation is assigned a **unique ID** during parsing. Each produced dataframe records:

* which tool instance produced it
* which parent dataframes it was derived from

This allows Anvil to build a full **data lineage DAG**, enabling:

* debugging
* explain plans
* future caching and optimization

(Lineage visualization and inspection tools are planned.)

---

## Comments

Anvil supports comments that are ignored by the parser:

```anvil
# Single-line comment

#
#  Block comment
#
```

---

## REPL Mode

If no script file is provided, Anvil runs in **REPL mode**.

Features:

* execute one statement at a time
* variables persist across statements
* errors do not terminate the session

Example:

```text
anvil> [input: './data/users.parquet'] |> users
anvil> users | [print]
```

### REPL Commands

Commands are handled outside the language grammar:

```text
run path/to/script.anvil
help
exit
```

Anything that is not a recognized command is treated as an Anvil statement.

---

## Example Scripts

### Simple pipeline

```anvil
[input: './data/users.parquet']
  | [filter: '$active == true']
  | [select: 'id,name,email']
  | [show];
```

### Join pipeline

```anvil
[input: './data/right.parquet'] |> right;

([input: './data/left.parquet'], right)
  | [join: type='inner' left='id' right='id']
  | [print];
```

### Project with expressions

```anvil
[input: './data/orders.parquet']
  | [project: total='$price * $quantity', discounted='$price * 0.9']
  | [print];
```

---

## Project Status

Anvil is under active development. Major areas of work include:

* expression → DataFusion translation
* richer lineage inspection
* optimizer passes
* improved error diagnostics
* documentation and examples

---

## License

MIT License (see `LICENSE` file).
