#define DUCKDB_EXTENSION_MAIN

#include "dynamic_sql_examples_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/catalog/default/default_table_functions.hpp"

namespace duckdb {

static DefaultMacro dynamic_sql_macros[] = {
    {DEFAULT_SCHEMA, "times_two", {"x", nullptr}, "x*2"},
    {nullptr, nullptr, {nullptr}, nullptr}};

// clang-format off
static const DefaultTableMacro dynamic_sql_table_macros[] = {
	{DEFAULT_SCHEMA, "times_two_table", {"x", nullptr}, {{"two", "2"}, {nullptr, nullptr}},  R"(
SELECT x * two as output_column;
)"},
	{nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr}
	};
// clang-format on

inline void DynamicSqlExamplesScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "DynamicSqlExamples "+name.GetString()+" 🐥");;
        });
}

inline void DynamicSqlExamplesOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "DynamicSqlExamples " + name.GetString() +
                                                     ", my linked OpenSSL version is " +
                                                     OPENSSL_VERSION_TEXT );;
        });
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register a scalar function
    auto dynamic_sql_examples_scalar_function = ScalarFunction("dynamic_sql_examples", {LogicalType::VARCHAR}, LogicalType::VARCHAR, DynamicSqlExamplesScalarFun);
    ExtensionUtil::RegisterFunction(instance, dynamic_sql_examples_scalar_function);

    // Register another scalar function
    auto dynamic_sql_examples_openssl_version_scalar_function = ScalarFunction("dynamic_sql_examples_openssl_version", {LogicalType::VARCHAR},
                                                LogicalType::VARCHAR, DynamicSqlExamplesOpenSSLVersionScalarFun);
    ExtensionUtil::RegisterFunction(instance, dynamic_sql_examples_openssl_version_scalar_function);

    // Macros
	for (idx_t index = 0; dynamic_sql_macros[index].name != nullptr; index++) {
		auto info = DefaultFunctionGenerator::CreateInternalMacroInfo(dynamic_sql_macros[index]);
		ExtensionUtil::RegisterFunction(instance, *info);
	}
    // Table Macros
    for (idx_t index = 0; dynamic_sql_table_macros[index].name != nullptr; index++) {
		auto table_info = DefaultTableFunctionGenerator::CreateTableMacroInfo(dynamic_sql_table_macros[index]);
        ExtensionUtil::RegisterFunction(instance, *table_info);
	}
}

void DynamicSqlExamplesExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string DynamicSqlExamplesExtension::Name() {
	return "dynamic_sql_examples";
}

std::string DynamicSqlExamplesExtension::Version() const {
#ifdef EXT_VERSION_DYNAMIC_SQL_EXAMPLES
	return EXT_VERSION_DYNAMIC_SQL_EXAMPLES;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void dynamic_sql_examples_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::DynamicSqlExamplesExtension>();
}

DUCKDB_EXTENSION_API const char *dynamic_sql_examples_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
