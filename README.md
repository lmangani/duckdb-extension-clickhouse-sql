<a href="https://community-extensions.duckdb.org/extensions/chsql.html" target="_blank">
<img src="https://github.com/lmangani/duckdb-extension-clickhouse-sql/assets/1423657/144dc202-f88a-4a2b-903d-51e30be75f6a" width=200>
</a>

# DuckDB ClickHouse SQL extension

The [chsql](https://community-extensions.duckdb.org/extensions/chsql.html) DuckDB extension implements various macros using ClickHouse SQL syntax, making it easier<br>
to transition between the two database systems ⭐ designed for [Quackpipe](https://github.com/metrico/quackpipe) 

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

## Supported Functions

The [list of supported functions](https://community-extensions.duckdb.org/extensions/chsql.html#added-functions) is available on the [dedicated extension page](https://community-extensions.duckdb.org/extensions/chsql.html). 

## Usage Examples
Once installed, macro functions provided by the extension can be used just like built-in functions.

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


<br>

## Development
The extension is automatically build and distributed. This section is not required unless you are a developer extending the code.

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
