# chsql for DuckDB

Hello stranger! 

This DuckDB extension implements various macros using ClickHouse SQL syntax, making it easier to transition knowledge, users and scripts between the two database systems. Since ClickHouse has hundreds of commands, this extension is a perpetual WIP.

## JOIN & HELP

Here's how you can help this extension by adding, fixing or extending its scope of SQL macros:

1) Find a ClickHouse function you are interested into the in [functions list](https://clickhouse.com/docs/en/sql-reference/functions)
2) Find if [DuckDB functions](https://duckdb.org/docs/sql/functions/) offer a viable method to alias the target function
3) Create the macro and extend to neighboring class functions with similar scope


## Examples

Here's a couple random examples:

#### ClickHouse [`tuplePlus`](https://clickhouse.com/docs/en/sql-reference/functions/tuple-functions#tupleplus)
Calculates the sum of corresponding values of two tuples of the same size.

##### Syntax
```
tuplePlus(tuple1, tuple2)

```

##### Arguments

```
tuple1 — First tuple. [Tuple](https://clickhouse.com/docs/en/sql-reference/data-types/tuple).
tuple2 — Second tuple. [Tuple](https://clickhouse.com/docs/en/sql-reference/data-types/tuple).
```
##### Returned value

Tuple with the sum. [Tuple](https://clickhouse.com/docs/en/sql-reference/data-types/tuple).

##### Example


Query:
```
SELECT tuplePlus((1, 2), (2, 3));

```
Result:

```
┌─tuplePlus((1, 2), (2, 3))─┐
│ (3,5)                                │
└───────────────┘
```

### DuckDB Macro

Let's convert our function to a DuckDB equivalent macro using a lambda function or any other method:

```
CREATE OR REPLACE MACRO tuplePlus(a, b) AS (apply(a, (x,i) -> apply(b, x -> CAST(x AS BIGINT))[i] + CAST(x AS BIGINT)));
```


#### Example
Query:
```
SELECT tuplePlus([1, 2], [2, 3]);

```
Result:

```
tupleplus(main.list_value(1, 2), main.list_value(2, 3))
--
[3,5]
```


<br> 

### Submit a PR

- If you're an SQL wizard, just add your new function(s) to the `aliases.sql` index for testing and validation.
- If you're a developer, implement the new function(s) directly in the [source code](chsql_macros) and submit a full PR.
- If you're a pro, you can also implement a test case for your new function(s) in the `tests/sql` directory.

<br>

👍 That's it! Simpler functions are trivial while others are puzzles. Have fun!
