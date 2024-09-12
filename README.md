<a href="https://community-extensions.duckdb.org/extensions/chsql.html" target="_blank">
<img src="https://github.com/user-attachments/assets/9003897d-db6f-4a79-9443-9b72766b511b" width=200>
</a>

# DuckDB ClickHouse SQL extension

The DuckDB [chsql](https://community-extensions.duckdb.org/extensions/chsql.html) community extension implements popular **ClickHouse SQL** syntax macros and functions,<br>
making it easier for users to transition between the two database systems ⭐ designed for [Quackpipe](https://github.com/metrico/quackpipe) 

<br>

## Installation

**chsql** is distributed as a [DuckDB Community Extension](https://github.com/duckdb/community-extensions) and can be installed using SQL:

```sql
INSTALL chsql FROM community;
LOAD chsql;
```

If you previously installed the `chsql` extension, upgrade using the FORCE command
```sql
FORCE INSTALL chsql FROM community;
LOAD chsql;
```

## Usage Examples
Once installed, the macro functions provided by the extension can be used just like built-in functions.

```sql
D INSTALL chsql FROM community;
D LOAD chsql;
D SELECT IPv4StringToNum('127.0.0.1'), IPv4NumToString(2130706433);
┌──────────────────────────────┬─────────────────────────────┐
│ ipv4stringtonum('127.0.0.1') │ ipv4numtostring(2130706433) │
│            int32             │           varchar           │
├──────────────────────────────┼─────────────────────────────┤
│                   2130706433 │ 127.0.0.1                   │
└──────────────────────────────┴─────────────────────────────┘
```

### Remote Queries
The built-in `ch_scan` function can be used to query remote ClickHouse servers using the HTTP/s API

```sql
D SELECT * FROM ch_scan("SELECT number * 2 FROM numbers(10)","https://play.clickhouse.com");
```

## Supported Functions

The [list of supported functions](https://community-extensions.duckdb.org/extensions/chsql.html#added-functions) is available on the [dedicated extension page](https://community-extensions.duckdb.org/extensions/chsql.html). 

<br>


## Motivation

✔ DuckDB SQL is awesome and full of great functions.<br>
✔ ClickHouse SQL is awesome and full of great functions. 

✔ The DuckDB library is ~51M and modular. Can LOAD extensions.<br>
❌ The ClickHouse monolith is ~551M and growing. No extensions. 

✔ DuckDB is open source and _protected by a no-profit foundation._<br>
❌ ClickHouse is open core and _controlled by for-profit corporation._ 

✔ DuckDB embedded is fast, mature and elegantly integrated in many languages.<br>
❌ ClickHouse chdb is experimental, unstable and _currently only supports Python_. 

<img src="https://github.com/user-attachments/assets/a17efd68-d2e1-42a7-8ab9-1ea4c2ff11e3" width=700 />

<br>

<br>


## Development
The extension is automatically build and distributed. This section is only required for development.

### Managing dependencies
DuckDB extensions uses VCPKG for dependency management. Enabling VCPKG is very simple: follow the [installation instructions](https://vcpkg.io/en/getting-started) or just run the following:
```shell
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```
Note: VCPKG is only required for extensions that want to rely on it for dependency management. If you want to develop an extension without dependencies, or want to do your own dependency management, just skip this step. Note that the example extension uses VCPKG to build with a dependency for instructive purposes, so when skipping this step the build may not work without removing the dependency.

### Build steps
Clone the repository and fetch all required submodules:
```sh
git submodule update --init
```

Build the extension:
```sh
GEN=ninja make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/dynamic_sql_clickhouse/dynamic_sql_clickhouse.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `dynamic_sql_clickhouse.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`.

Now we can use the features from the extension directly in DuckDB. See `/tests/sql` for a list of supported functions.

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

<!--
### Installing the deployed binaries
To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:
```shell
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension
you want to install. To do this run the following SQL query in DuckDB:
```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```
Note that the `/latest` path will allow you to install the latest extension version available for your current version of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:
```sql
INSTALL dynamic_sql_clickhouse
LOAD dynamic_sql_clickhouse
```

-->


###### Disclaimer
> DuckDB ® is a trademark of DuckDB Foundation. ClickHouse® is a trademark of ClickHouse Inc. All trademarks, service marks, and logos mentioned or depicted are the property of their respective owners. The use of any third-party trademarks, brand names, product names, and company names is purely informative or intended as parody and does not imply endorsement, affiliation, or association with the respective owners.
