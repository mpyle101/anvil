# Anvil

**Anvil** is a dataflow-oriented scripting language and execution engine built on **Rust** and **Apache DataFusion**.

It is designed for **readable, composable, graph-friendly data pipelines** that can be parsed, analyzed, and executed deterministically.

At its core, Anvil treats data processing as a sequence of **tools** connected by **flows**, with optional branching and variable binding.

---

## Command Line Interface

Anvil can be run either by providing a script file or by entering an interactive REPL (when no script is specified).

### Usage

~~~bash
anvil [OPTIONS] [SCRIPT]
~~~

- `SCRIPT`  
  Optional path to an Anvil script file.  
  If omitted, Anvil starts in REPL mode.

---

### Options

#### `-d, --dot [PATH]`

Emit the execution plan as a **Graphviz DOT** graph instead of executing the plan.

- If `PATH` is provided, the DOT output is written to that file.
- If `PATH` is omitted, the DOT output is written to **stdout**.

This is useful for inspecting execution order, data lineage, tool dependencies, and branching behavior.

---

## Examples

### Run an Anvil script normally

~~~bash
anvil examples/join.avl
~~~

### Generate DOT output to stdout

~~~bash
anvil --dot examples/join.avl
~~~

### Write DOT output to a file

~~~bash
anvil --dot plan.dot examples/join.avl
~~~

### Generate a PNG using Graphviz

You can pipe the DOT output directly into the `dot` binary to generate an image:

~~~bash
anvil --dot examples/join.avl | dot -Tpng -o plan.png
~~~

Or, if you wrote the DOT file explicitly:

~~~bash
anvil --dot plan.dot examples/join.avl
dot -Tpng plan.dot -o plan.png
~~~

The resulting graph visually distinguishes tools and variables, and edge labels represent data ports (including branch outputs such as `true` and `false`).

---

## Key Concepts

### Flow-based execution

An Anvil script is a sequence of **statements**. Each statement defines a **flow** of tools and variables connected by pipes (`|`).

```anvil
[input: './data/users.parquet'] | [select: 'id,email'] | [print];
```

Each tool consumes one or more dataframes and produces zero or more dataframes.

---

### Tools

Tools are written using **bracket syntax**:

```anvil
[tool_name: arguments]
```

Examples:

```anvil
[input: './data/users.parquet']
[select: 'id,email']
[print]
```

The brackets clearly distinguish tools from variables and make branching explicit.

---

### Variables

A statement can bind its final result to a variable using `>`:

```anvil
[input: './data/users.parquet'] > users;
```

Variables may be used as inputs to later flows:

```anvil
users | [count] | [print];
```

Variables are first-class graph nodes — they represent stored data, not execution.

---

### Comments and whitespace

* Comments start with `#`
* Whitespace is flexible and mostly insignificant

```anvil
# Load users
[input: './data/users.parquet'] > users;
```

---

## Grammar Overview (Informal)

* Statements end with `;`
* Tools are chained with `|`
* Variable binding uses `>`
* Tool arguments support:

  * positional arguments
  * keyword arguments
  * flow arguments (nested pipelines in parentheses)

---

## Tool Arguments

### Positional arguments

```anvil
[limit: 10]
```

### Keyword arguments

```anvil
[register: './data/users.parquet', table='users']
```

### Mixed positional + keyword (positional first)

```anvil
[sort: 'id', descending=true]
```

### Flow arguments (subflows)

Some tools accept **flows as argument values**. Flows used as arguments must be wrapped in parentheses.

```anvil
[join:
    df_lt=users
    df_rt=([input: './data/orders.parquet'])
    left_cols='id'
    right_cols='user_id'
]
```

A flow argument may reference:

* a variable
* a tool
* an entire pipeline

---

## Branching

Flows may branch using `:` and named branch targets.

```anvil
users | [filter: '$age < 18']
    : true => minors
    , false => adults;
```

Each branch produces its own output flow.

---

## Available Tools

### I/O

* **input** — read a file into a dataframe
* **output** — write a dataframe to a file
* **print** — write a dataframe to stdout
* **register** — register a file as a SQL table

### Inspection

* **schema** — produce a dataframe describing the schema
* **describe** — metadata and statistics
* **count** — count rows
* **distinct** — distinct rows

### Transformation

* **select** — select columns / expressions
* **filter** — filter rows using expressions
* **project** — compute new columns from expressions
* **sort** — sort by expressions
* **limit** — limit number of rows
* **drop** — drop columns
* **fill** — fill null values

### Set operations

* **union**
* **intersect**
* **join**

### SQL

* **sql** — execute SQL against registered tables

---

## Expressions

Many tools accept **DataFusion expressions** as strings.

Examples:

```anvil
[filter: '$age > 30']
[project: total='$price * $quantity']
[sort: '$created_at']
```

Column references use `$column_name`.

---

## Example Scripts

### Load and inspect data

```anvil
[input: './data/users.parquet'] > users;

users | [schema] | [print];
users | [count]  | [print];
```

---

### Filtering and projection

```anvil
[input: './data/users.parquet']
| [filter: '$age >= 18']
| [project:
      full_name='$first_name || " " || $last_name',
      age_bucket='$age / 10'
  ]
| [print];
```

---

### Join with subflows

```anvil
[join:
    type='inner'
    df_lt=([input: './data/users.parquet'])
    df_rt=([input: './data/orders.parquet'])
    cols_lt='id'
    cols_rt='user_id'
]
| [print];
```

---

### Branching example

```anvil
[input: './data/messy.parquet'] | [filter: '$three == true']:
	true => [print],
	false => df;

df | [print];
```

---

### SQL example

```anvil
[register: './data/users.parquet', table='users'];

[sql: 'SELECT age, COUNT(*) FROM users GROUP BY age']
| [print];
```

---

## Design Goals

* **Readable pipelines**
* **Explicit dataflow**
* **Graph-based execution**
* **Static analyzability (lineage, dependencies)**
* **Tight integration with DataFusion**

---

## Status

Anvil is under active development.

Current areas of focus:

* execution graph construction
* data lineage tracking
* REPL support
* richer expression semantics
