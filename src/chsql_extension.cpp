#define DUCKDB_EXTENSION_MAIN

#include "chsql_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

#include "default_functions.hpp"
#include "default_table_functions.hpp"

namespace duckdb {

// To add a new scalar SQL macro, add a new macro to this array!
// Copy and paste the top item in the array into the 
// second-to-last position and make some modifications. 
// (essentially, leave the last entry in the array as {nullptr, nullptr, {nullptr}, nullptr})

// Keep the DEFAULT_SCHEMA (no change needed)
// Replace "times_two" with a name for your macro
// If your function has parameters, add their names in quotes inside of the {}, with a nullptr at the end
//      If you do not have parameters, simplify to {nullptr}
// Add the text of your SQL macro as a raw string with the format R"( select 42 )"


static DefaultMacro chsql_macros[] = {
    {DEFAULT_SCHEMA, "times_two", {"x", nullptr}, R"(x*2)"},
    // -- Type conversion macros
    {DEFAULT_SCHEMA, "toString", {"x", nullptr}, R"(CAST(x AS VARCHAR))"},
    {DEFAULT_SCHEMA, "toInt8", {"x", nullptr}, R"(CAST(x AS INT8))"},
    {DEFAULT_SCHEMA, "toInt16", {"x", nullptr}, R"(CAST(x AS INT16))"},
    {DEFAULT_SCHEMA, "toInt32", {"x", nullptr}, R"(CAST(x AS INT32))"},
    {DEFAULT_SCHEMA, "toInt64", {"x", nullptr}, R"(CAST(x AS INT64))"},
    {DEFAULT_SCHEMA, "toInt128", {"x", nullptr}, R"(CAST(x AS INT128))"},
    {DEFAULT_SCHEMA, "toInt256", {"x", nullptr}, R"(CAST(x AS HUGEINT))"},
    {DEFAULT_SCHEMA, "toInt8OrZero", {"x", nullptr}, R"(CASE WHEN TRY_CAST(x AS INT8) IS NOT NULL THEN CAST(x AS INT8) ELSE 0 END)"},
    {DEFAULT_SCHEMA, "toInt16OrZero", {"x", nullptr}, R"(CASE WHEN TRY_CAST(x AS INT16) IS NOT NULL THEN CAST(x AS INT16) ELSE 0 END)"},
    {DEFAULT_SCHEMA, "toInt32OrZero", {"x", nullptr}, R"(CASE WHEN TRY_CAST(x AS INT32) IS NOT NULL THEN CAST(x AS INT32) ELSE 0 END)"},
    {DEFAULT_SCHEMA, "toInt64OrZero", {"x", nullptr}, R"(CASE WHEN TRY_CAST(x AS INT64) IS NOT NULL THEN CAST(x AS INT64) ELSE 0 END)"},
    {DEFAULT_SCHEMA, "toInt128OrZero", {"x", nullptr}, R"(CASE WHEN TRY_CAST(x AS INT128) IS NOT NULL THEN CAST(x AS INT128) ELSE 0 END)"},
    {DEFAULT_SCHEMA, "toInt256OrZero", {"x", nullptr}, R"(CASE WHEN TRY_CAST(x AS HUGEINT) IS NOT NULL THEN CAST(x AS HUGEINT) ELSE 0 END)"},
    {DEFAULT_SCHEMA, "toInt8OrNull", {"x", nullptr}, R"(TRY_CAST(x AS INT8))"},
    {DEFAULT_SCHEMA, "toInt16OrNull", {"x", nullptr}, R"(TRY_CAST(x AS INT16))"},
    {DEFAULT_SCHEMA, "toInt32OrNull", {"x", nullptr}, R"(TRY_CAST(x AS INT32))"},
    {DEFAULT_SCHEMA, "toInt64OrNull", {"x", nullptr}, R"(TRY_CAST(x AS INT64))"},
    {DEFAULT_SCHEMA, "toInt128OrNull", {"x", nullptr}, R"(TRY_CAST(x AS INT128))"},
    {DEFAULT_SCHEMA, "toInt256OrNull", {"x", nullptr}, R"(TRY_CAST(x AS HUGEINT))"},
    // -- Unsigned integer conversion macros
    {DEFAULT_SCHEMA, "toUInt8", {"x", nullptr}, R"(CAST(x AS UTINYINT))"},
    {DEFAULT_SCHEMA, "toUInt16", {"x", nullptr}, R"(CAST(x AS USMALLINT))"},
    {DEFAULT_SCHEMA, "toUInt32", {"x", nullptr}, R"(CAST(x AS UINTEGER))"},
    {DEFAULT_SCHEMA, "toUInt64", {"x", nullptr}, R"(CAST(x AS UBIGINT))"},
    {DEFAULT_SCHEMA, "toUInt8OrZero", {"x", nullptr}, R"(CASE WHEN TRY_CAST(x AS UTINYINT) IS NOT NULL THEN CAST(x AS UTINYINT) ELSE 0 END)"},
    {DEFAULT_SCHEMA, "toUInt16OrZero", {"x", nullptr}, R"(CASE WHEN TRY_CAST(x AS USMALLINT) IS NOT NULL THEN CAST(x AS USMALLINT) ELSE 0 END)"},
    {DEFAULT_SCHEMA, "toUInt32OrZero", {"x", nullptr}, R"(CASE WHEN TRY_CAST(x AS UINTEGER) IS NOT NULL THEN CAST(x AS UINTEGER) ELSE 0 END)"},
    {DEFAULT_SCHEMA, "toUInt64OrZero", {"x", nullptr}, R"(CASE WHEN TRY_CAST(x AS UBIGINT) IS NOT NULL THEN CAST(x AS UBIGINT) ELSE 0 END)"},
    {DEFAULT_SCHEMA, "toUInt8OrNull", {"x", nullptr}, R"(TRY_CAST(x AS UTINYINT))"}, // Fixed comma here
    {DEFAULT_SCHEMA, "toUInt16OrNull", {"x", nullptr}, R"(TRY_CAST(x AS USMALLINT))"}, // And here
    {DEFAULT_SCHEMA, "toUInt32OrNull", {"x", nullptr}, R"(TRY_CAST(x AS UINTEGER))"}, // Also here
    {DEFAULT_SCHEMA, "toUInt64OrNull", {"x", nullptr}, R"(TRY_CAST(x AS UBIGINT))"}, // And here
    // -- Floating-point conversion macros
    {DEFAULT_SCHEMA, "toFloat", {"x", nullptr}, R"(CAST(x AS DOUBLE))"},
    {DEFAULT_SCHEMA, "toFloatOrNull", {"x", nullptr}, R"(TRY_CAST(x AS DOUBLE))"},
    {DEFAULT_SCHEMA, "toFloatOrZero", {"x", nullptr}, R"(CASE WHEN TRY_CAST(x AS DOUBLE) IS NOT NULL THEN CAST(x AS DOUBLE) ELSE 0 END)"},
    // -- Arithmetic macros
    {DEFAULT_SCHEMA, "intDiv", {"a", "b"}, R"((CAST(a AS BIGINT) // CAST(b AS BIGINT)))"},
    {DEFAULT_SCHEMA, "tupleDivide", {"a", "b"}, R"(apply(a, (x,i) -> apply(b, x -> CAST(x AS BIGINT))[i] // CAST(x AS BIGINT)))"},
    // -- String matching macros
    {DEFAULT_SCHEMA, "match", {"string", "token"}, R"(string LIKE token)"},
    // -- Array macros
    {DEFAULT_SCHEMA, "arrayExists", {"needle", "haystack"}, R"(haystack @> ARRAY[needle])"},
    {DEFAULT_SCHEMA, "arrayMap", {"e", "arr"}, R"(array_transform(arr, e -> (e * e)))"},
    {nullptr, nullptr, {nullptr}, nullptr}};

// To add a new table SQL macro, add a new macro to this array!
// Copy and paste the top item in the array into the
// second-to-last position and make some modifications.
// (essentially, leave the last entry in the array as {nullptr, nullptr, {nullptr}, nullptr})

// Keep the DEFAULT_SCHEMA (no change needed)
// Replace "times_two_table" with a name for your macro
// If your function has parameters without default values, add their names in quotes inside of the {}, with a nullptr at the end
//      If you do not have parameters, simplify to {nullptr}
// If your function has parameters with default values, add their names and values in quotes inside of {}'s inside of the {}.
// Be sure to keep {nullptr, nullptr} at the end
//      If you do not have parameters with default values, simplify to {nullptr, nullptr}
// Add the text of your SQL macro as a raw string with the format R"( select 42; )"

// clang-format off
static const DefaultTableMacro chsql_table_macros[] = {
	{DEFAULT_SCHEMA, "times_two_table", {"x", nullptr}, {{"two", "2"}, {nullptr, nullptr}},  R"(SELECT x * two as output_column;)"},
	{nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr}
	};
// clang-format on

inline void ChSqlScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "ChSql "+name.GetString()+" 🐥");;
        });
}

inline void ChSqlOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "ChSql " + name.GetString() +
                                                     ", my linked OpenSSL version is " +
                                                     OPENSSL_VERSION_TEXT );;
        });
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register a scalar function
    auto chsql_scalar_function = ScalarFunction("chsql", {LogicalType::VARCHAR}, LogicalType::VARCHAR, ChSqlScalarFun);
    ExtensionUtil::RegisterFunction(instance, chsql_scalar_function);

    // Register another scalar function
    auto chsql_openssl_version_scalar_function = ScalarFunction("chsql_openssl_version", {LogicalType::VARCHAR},
                                                LogicalType::VARCHAR, ChSqlOpenSSLVersionScalarFun);
    ExtensionUtil::RegisterFunction(instance, chsql_openssl_version_scalar_function);

    // Macros
	for (idx_t index = 0; chsql_macros[index].name != nullptr; index++) {
		auto info = DefaultFunctionGenerator::CreateInternalMacroInfo(chsql_macros[index]);
		ExtensionUtil::RegisterFunction(instance, *info);
	}
    // Table Macros
    for (idx_t index = 0; chsql_table_macros[index].name != nullptr; index++) {
		auto table_info = DefaultTableFunctionGenerator::CreateTableMacroInfo(chsql_table_macros[index]);
        ExtensionUtil::RegisterFunction(instance, *table_info);
	}
}

void ChsqlExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string ChsqlExtension::Name() {
	return "chsql";
}

std::string ChsqlExtension::Version() const {
#ifdef EXT_VERSION_CHSQL
	return EXT_VERSION_CHSQL;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void chsql_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::ChsqlExtension>();
}

DUCKDB_EXTENSION_API const char *chsql_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
