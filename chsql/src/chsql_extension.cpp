#define DUCKDB_EXTENSION_MAIN

#include "chsql_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/catalog/default/default_table_functions.hpp"
#include <string>
#include "parquet_reader.hpp"


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
		// -- Type conversion macros
		{DEFAULT_SCHEMA, "toString", {"x"}, {}, R"(CAST(x AS VARCHAR))"},
		{DEFAULT_SCHEMA, "toInt8", {"x"}, {}, R"(CAST(x AS INT8))"},
		{DEFAULT_SCHEMA, "toInt16", {"x"}, {}, R"(CAST(x AS INT16))"},
		{DEFAULT_SCHEMA, "toInt32", {"x"}, {}, R"(CAST(x AS INT32))"},
		{DEFAULT_SCHEMA, "toInt64", {"x"}, {}, R"(CAST(x AS INT64))"},
		{DEFAULT_SCHEMA, "toInt128", {"x"}, {}, R"(CAST(x AS INT128))"},
		{DEFAULT_SCHEMA, "toInt256", {"x"}, {}, R"(CAST(x AS HUGEINT))"},
		{DEFAULT_SCHEMA, "toInt8OrZero", {"x"}, {}, R"(CASE WHEN TRY_CAST(x AS INT8) IS NOT NULL THEN CAST(x AS INT8) ELSE 0 END)"},
		{DEFAULT_SCHEMA, "toInt16OrZero", {"x"}, {}, R"(CASE WHEN TRY_CAST(x AS INT16) IS NOT NULL THEN CAST(x AS INT16) ELSE 0 END)"},
		{DEFAULT_SCHEMA, "toInt32OrZero", {"x"}, {}, R"(CASE WHEN TRY_CAST(x AS INT32) IS NOT NULL THEN CAST(x AS INT32) ELSE 0 END)"},
		{DEFAULT_SCHEMA, "toInt64OrZero", {"x"}, {}, R"(CASE WHEN TRY_CAST(x AS INT64) IS NOT NULL THEN CAST(x AS INT64) ELSE 0 END)"},
		{DEFAULT_SCHEMA, "toInt128OrZero", {"x"}, {}, R"(CASE WHEN TRY_CAST(x AS INT128) IS NOT NULL THEN CAST(x AS INT128) ELSE 0 END)"},
		{DEFAULT_SCHEMA, "toInt256OrZero", {"x"}, {}, R"(CASE WHEN TRY_CAST(x AS HUGEINT) IS NOT NULL THEN CAST(x AS HUGEINT) ELSE 0 END)"},
		{DEFAULT_SCHEMA, "toInt8OrNull", {"x"}, {}, R"(TRY_CAST(x AS INT8))"},
		{DEFAULT_SCHEMA, "toInt16OrNull", {"x"}, {}, R"(TRY_CAST(x AS INT16))"},
		{DEFAULT_SCHEMA, "toInt32OrNull", {"x"}, {}, R"(TRY_CAST(x AS INT32))"},
		{DEFAULT_SCHEMA, "toInt64OrNull", {"x"}, {}, R"(TRY_CAST(x AS INT64))"},
		{DEFAULT_SCHEMA, "toInt128OrNull", {"x"}, {}, R"(TRY_CAST(x AS INT128))"},
		{DEFAULT_SCHEMA, "toInt256OrNull", {"x"}, {}, R"(TRY_CAST(x AS HUGEINT))"},
		// -- Unsigned integer conversion macros
		{DEFAULT_SCHEMA, "toUInt8", {"x"}, {}, R"(CAST(x AS UTINYINT))"},
		{DEFAULT_SCHEMA, "toUInt16", {"x"}, {}, R"(CAST(x AS USMALLINT))"},
		{DEFAULT_SCHEMA, "toUInt32", {"x"}, {}, R"(CAST(x AS UINTEGER))"},
		{DEFAULT_SCHEMA, "toUInt64", {"x"}, {}, R"(CAST(x AS UBIGINT))"},
		{DEFAULT_SCHEMA, "toUInt8OrZero", {"x"}, {}, R"(CASE WHEN TRY_CAST(x AS UTINYINT) IS NOT NULL THEN CAST(x AS UTINYINT) ELSE 0 END)"},
		{DEFAULT_SCHEMA, "toUInt16OrZero", {"x"}, {}, R"(CASE WHEN TRY_CAST(x AS USMALLINT) IS NOT NULL THEN CAST(x AS USMALLINT) ELSE 0 END)"},
		{DEFAULT_SCHEMA, "toUInt32OrZero", {"x"}, {}, R"(CASE WHEN TRY_CAST(x AS UINTEGER) IS NOT NULL THEN CAST(x AS UINTEGER) ELSE 0 END)"},
		{DEFAULT_SCHEMA, "toUInt64OrZero", {"x"}, {}, R"(CASE WHEN TRY_CAST(x AS UBIGINT) IS NOT NULL THEN CAST(x AS UBIGINT) ELSE 0 END)"},
		{DEFAULT_SCHEMA, "toUInt8OrNull", {"x"}, {}, R"(TRY_CAST(x AS UTINYINT))"}, // Fixed comma here
		{DEFAULT_SCHEMA, "toUInt16OrNull", {"x"}, {}, R"(TRY_CAST(x AS USMALLINT))"}, // And here
		{DEFAULT_SCHEMA, "toUInt32OrNull", {"x"}, {}, R"(TRY_CAST(x AS UINTEGER))"}, // Also here
		{DEFAULT_SCHEMA, "toUInt64OrNull", {"x"}, {}, R"(TRY_CAST(x AS UBIGINT))"}, // And here
		// -- Floating-point conversion macros
		{DEFAULT_SCHEMA, "toFloat", {"x"}, {}, R"(CAST(x AS DOUBLE))"},
		{DEFAULT_SCHEMA, "toFloatOrNull", {"x"}, {}, R"(TRY_CAST(x AS DOUBLE))"},
		{DEFAULT_SCHEMA, "toFloatOrZero", {"x"}, {}, R"(CASE WHEN TRY_CAST(x AS DOUBLE) IS NOT NULL THEN CAST(x AS DOUBLE) ELSE 0 END)"},
		// -- Arithmetic macros
		{DEFAULT_SCHEMA, "intDiv", {"a", "b"}, {}, R"((CAST(a AS BIGINT) // CAST(b AS BIGINT)))"},
		{DEFAULT_SCHEMA, "intDivOrNull", {"a", "b"}, {}, R"(TRY_CAST((TRY_CAST(a AS BIGINT) // TRY_CAST(b AS BIGINT)) AS BIGINT))"},
		{DEFAULT_SCHEMA, "intDivOZero", {"x"}, {}, R"(COALESCE((TRY_CAST((TRY_CAST(a AS BIGINT) // TRY_CAST(b AS BIGINT)) AS BIGINT)),0))"},
		{DEFAULT_SCHEMA, "plus", {"a", "b"}, {}, R"(add(a, b))"},
		{DEFAULT_SCHEMA, "minus", {"a", "b"}, {}, R"(subtract(a, b))"},
		{DEFAULT_SCHEMA, "modulo", {"a", "b"}, {}, R"(CAST(a AS BIGINT) % CAST(b AS BIGINT))"},
		{DEFAULT_SCHEMA, "moduloOrZero", {"a", "b"}, {}, R"(COALESCE(((TRY_CAST(a AS BIGINT) % TRY_CAST(b AS BIGINT))),0))"},
		// -- Tuple macros
		{DEFAULT_SCHEMA, "tupleIntDiv", {"a", "b"}, {}, R"(apply(a, (x,i) -> apply(b, x -> CAST(x AS BIGINT))[i] // CAST(x AS BIGINT)))"},
		{DEFAULT_SCHEMA, "tupleIntDivByNumber", {"a", "b"}, {}, R"(apply(a, (x) -> CAST(apply(b, x -> CAST(x AS BIGINT))[1] as BIGINT) // CAST(x AS BIGINT)))"},
		{DEFAULT_SCHEMA, "tupleDivide", {"a", "b"}, {}, R"(apply(a, (x,i) -> apply(b, x -> CAST(x AS BIGINT))[i] / CAST(x AS BIGINT)))"},
		{DEFAULT_SCHEMA, "tupleMultiply", {"a", "b"}, {}, R"(apply(a, (x,i) -> CAST(apply(b, x -> CAST(x AS BIGINT))[i] as BIGINT) * CAST(x AS BIGINT)))"},
		{DEFAULT_SCHEMA, "tupleMinus", {"a", "b"}, {}, R"(apply(a, (x,i) -> apply(b, x -> CAST(x AS BIGINT))[i] - CAST(x AS BIGINT)))"},
		{DEFAULT_SCHEMA, "tuplePlus", {"a", "b"}, {}, R"(apply(a, (x,i) -> apply(b, x -> CAST(x AS BIGINT))[i] + CAST(x AS BIGINT)))"},
		{DEFAULT_SCHEMA, "tupleMultiplyByNumber", {"a", "b"}, {}, R"(apply(a, (x) -> CAST(apply(b, x -> CAST(x AS BIGINT))[1] as BIGINT) * CAST(x AS BIGINT)))"},
		{DEFAULT_SCHEMA, "tupleDivideByNumber", {"a", "b"}, {}, R"(apply(a, (x) -> CAST(apply(b, x -> CAST(x AS BIGINT))[1] as BIGINT) / CAST(x AS BIGINT)))"},
		{DEFAULT_SCHEMA, "tupleModulo", {"a", "b"}, {}, R"(apply(a, (x) -> CAST(apply(b, x -> CAST(x AS BIGINT))[1] as BIGINT) % CAST(x AS BIGINT)))"},
		{DEFAULT_SCHEMA, "tupleModuloByNumber", {"a", "b"}, {}, R"(apply(a, (x) -> CAST(apply(b, x -> CAST(x AS BIGINT))[1] as BIGINT) % CAST(x AS BIGINT)))"},
		{DEFAULT_SCHEMA, "tupleConcat", {"a", "b"}, {}, R"(list_concat(a, b))"},
		// -- String matching macros
		{DEFAULT_SCHEMA, "match", {"string", "token"}, {}, R"(string LIKE token)"},
		// -- Array macros
		{DEFAULT_SCHEMA, "arrayExists", {"needle", "haystack"}, {}, R"(haystack @> ARRAY[needle])"},
		{DEFAULT_SCHEMA, "arrayMap", {"e", "arr"}, {}, R"(array_transform(arr, e -> (e * e)))"},
		// Date and Time Functions
		{DEFAULT_SCHEMA, "toYear", {"date_expression"}, {}, R"(EXTRACT(YEAR FROM date_expression))"},
		{DEFAULT_SCHEMA, "toMonth", {"date_expression"}, {}, R"(EXTRACT(MONTH FROM date_expression))"},
		{DEFAULT_SCHEMA, "toDayOfMonth", {"date_expression"}, {}, R"(EXTRACT(DAY FROM date_expression))"},
		{DEFAULT_SCHEMA, "toHour", {"date_expression"}, {}, R"(EXTRACT(HOUR FROM date_expression))"},
		{DEFAULT_SCHEMA, "toMinute", {"date_expression"}, {}, R"(EXTRACT(MINUTE FROM date_expression))"},
		{DEFAULT_SCHEMA, "toSecond", {"date_expression"}, {}, R"(EXTRACT(SECOND FROM date_expression))"},
		{DEFAULT_SCHEMA, "toYYYYMM", {"date_expression"}, {}, R"(DATE_FORMAT(date_expression, '%Y%m'))"},
		{DEFAULT_SCHEMA, "toYYYYMMDD", {"date_expression"}, {}, R"(DATE_FORMAT(date_expression, '%Y%m%d'))"},
		{DEFAULT_SCHEMA, "toYYYYMMDDhhmmss", {"date_expression"}, {}, R"(DATE_FORMAT(date_expression, '%Y%m%d%H%M%S'))"},
		{DEFAULT_SCHEMA, "formatDateTime", {"time", "format", "timezone"}, {}, R"(CASE  WHEN timezone IS NULL THEN strftime(time, format) ELSE strftime(time AT TIME ZONE timezone, format) END)"},
		// String Functions
		{DEFAULT_SCHEMA, "empty", {"str"}, {}, R"(LENGTH(str) = 0)"},
		{DEFAULT_SCHEMA, "notEmpty", {"str"}, {}, R"(LENGTH(str) > 0)"},
		{DEFAULT_SCHEMA, "lengthUTF8", {"str"}, {}, R"(LENGTH(str))"},
		{DEFAULT_SCHEMA, "leftPad", {"str", "length", "pad_str"}, {}, R"(LPAD(str, length, pad_str))"},
		{DEFAULT_SCHEMA, "rightPad", {"str", "length", "pad_str"}, {}, R"(RPAD(str, length, pad_str))"},
		{DEFAULT_SCHEMA, "extractAllGroups", {"text", "pattern"}, {}, R"(regexp_extract_all(text, pattern))"},
		{DEFAULT_SCHEMA, "toFixedString", {"str", "length"}, {}, R"(RPAD(LEFT(str, length), length, '\0'))"},
		{DEFAULT_SCHEMA, "ifNull", {"x", "y"}, {}, R"(COALESCE(x, y))"},
		{DEFAULT_SCHEMA, "arrayJoin", {"arr"}, {}, R"(UNNEST(arr))"},
		{DEFAULT_SCHEMA, "splitByChar", {"separator", "str"}, {}, R"(string_split(str, separator))"},
		// URL Functions
		{DEFAULT_SCHEMA, "protocol", {"url"}, {}, R"(REGEXP_EXTRACT(url, '^(\w+)://', 1))"},
		{DEFAULT_SCHEMA, "domain", {"url"}, {}, R"(REGEXP_EXTRACT(url, '://([^/]+)', 1))"},
		{DEFAULT_SCHEMA, "topLevelDomain", {"url"}, {}, R"(REGEXP_EXTRACT(url, '\.([^./:]+)([:/]|$)', 1))"},
		{DEFAULT_SCHEMA, "path", {"url"}, {}, R"(REGEXP_EXTRACT(url, '://[^/]+(/.*)', 1))"},
		// IP Address Functions
		{DEFAULT_SCHEMA, "IPv4NumToString", {"num"}, {}, R"(CONCAT(CAST((num >> 24) & 255 AS VARCHAR), '.', CAST((num >> 16) & 255 AS VARCHAR), '.', CAST((num >> 8) & 255 AS VARCHAR), '.', CAST(num & 255 AS VARCHAR)))"},
		{DEFAULT_SCHEMA, "IPv4StringToNum", {"ip"}, {}, R"(CAST(SPLIT_PART(ip, '.', 1) AS INTEGER) * 256 * 256 * 256 + CAST(SPLIT_PART(ip, '.', 2) AS INTEGER) * 256 * 256 + CAST(SPLIT_PART(ip, '.', 3) AS INTEGER) * 256 + CAST(SPLIT_PART(ip, '.', 4) AS INTEGER))"},
		// -- Misc macros
		{DEFAULT_SCHEMA, "generateUUIDv4", {}, {}, R"(toString(uuid()))"},
		{DEFAULT_SCHEMA, "parseURL", {"url", "part"}, {}, R"(CASE part WHEN 'protocol' THEN REGEXP_EXTRACT(url, '^(\w+)://') WHEN 'domain' THEN REGEXP_EXTRACT(url, '://([^/:]+)') WHEN 'port' THEN REGEXP_EXTRACT(url, ':(\d+)') WHEN 'path' THEN REGEXP_EXTRACT(url, '://[^/]+(/.+?)(\?|#|$)') WHEN 'query' THEN REGEXP_EXTRACT(url, '\?([^#]+)') WHEN 'fragment' THEN REGEXP_EXTRACT(url, '#(.+)$') END)"},
		{DEFAULT_SCHEMA, "bitCount", {"num"}, {}, R"(BIT_COUNT(num))"},
		//{nullptr, nullptr, {nullptr}, nullptr}
		};

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
		{DEFAULT_SCHEMA, "tableMultiply", {"x", nullptr}, {{"two", "2"}, {nullptr, nullptr}},  R"(SELECT x * two as output_column;)"},
			{DEFAULT_SCHEMA, "numbers", {"x", nullptr}, {{"z", "0"}, {nullptr, nullptr}},  R"(SELECT * as number FROM generate_series(z,x-1);)"},
		{nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr},
	{DEFAULT_SCHEMA, "numbers", {"x", nullptr}, {{"z", "0"}, {nullptr, nullptr}},  R"(SELECT * as number FROM generate_series(z,x-1);)"},
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
	for (idx_t index = 0; index < sizeof(chsql_macros)/sizeof(chsql_macros[0]); index++) {
		auto info = DefaultFunctionGenerator::CreateInternalMacroInfo(chsql_macros[index]);
		ExtensionUtil::RegisterFunction(instance, *info);
	}
    // Table Macros
    for (idx_t index = 0; chsql_table_macros[index].name != nullptr; index++) {
		auto table_info = DefaultTableFunctionGenerator::CreateTableMacroInfo(chsql_table_macros[index]);
        ExtensionUtil::RegisterFunction(instance, *table_info);
	}
	ExtensionUtil::RegisterFunction(instance, ReadParquetOrderedFunction());
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
