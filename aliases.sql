-- Type conversion macros
CREATE OR REPLACE MACRO toString(expr) AS CAST(expr AS VARCHAR);
CREATE OR REPLACE MACRO toInt8(expr) AS CAST(expr AS INT8);
CREATE OR REPLACE MACRO toInt16(expr) AS CAST(expr AS INT16);
CREATE OR REPLACE MACRO toInt32(expr) AS CAST(expr AS INT32);
CREATE OR REPLACE MACRO toInt64(expr) AS CAST(expr AS INT64);
CREATE OR REPLACE MACRO toInt128(expr) AS CAST(expr AS INT128);
CREATE OR REPLACE MACRO toInt256(expr) AS CAST(expr AS HUGEINT);
-- Type conversion with default values
CREATE OR REPLACE MACRO toInt8OrZero(expr) AS COALESCE(TRY_CAST(expr AS INT8), 0);
CREATE OR REPLACE MACRO toInt16OrZero(expr) AS COALESCE(TRY_CAST(expr AS INT16), 0);
CREATE OR REPLACE MACRO toInt32OrZero(expr) AS COALESCE(TRY_CAST(expr AS INT32), 0);
CREATE OR REPLACE MACRO toInt64OrZero(expr) AS COALESCE(TRY_CAST(expr AS INT64), 0);
CREATE OR REPLACE MACRO toInt128OrZero(expr) AS COALESCE(TRY_CAST(expr AS INT128), 0);
CREATE OR REPLACE MACRO toInt256OrZero(expr) AS COALESCE(TRY_CAST(expr AS HUGEINT), 0);
CREATE OR REPLACE MACRO toInt8OrNull(expr) AS TRY_CAST(expr AS INT8);
CREATE OR REPLACE MACRO toInt16OrNull(expr) AS TRY_CAST(expr AS INT16);
CREATE OR REPLACE MACRO toInt32OrNull(expr) AS TRY_CAST(expr AS INT32);
CREATE OR REPLACE MACRO toInt64OrNull(expr) AS TRY_CAST(expr AS INT64);
CREATE OR REPLACE MACRO toInt128OrNull(expr) AS TRY_CAST(expr AS INT128);
CREATE OR REPLACE MACRO toInt256OrNull(expr) AS TRY_CAST(expr AS HUGEINT);
-- Unsigned integer conversion macros
CREATE OR REPLACE MACRO toUInt8(expr) AS CAST(expr AS UTINYINT);
CREATE OR REPLACE MACRO toUInt16(expr) AS CAST(expr AS USMALLINT);
CREATE OR REPLACE MACRO toUInt32(expr) AS CAST(expr AS UINTEGER);
CREATE OR REPLACE MACRO toUInt64(expr) AS CAST(expr AS UBIGINT);
-- Unsigned integer conversion with default values
CREATE OR REPLACE MACRO toUInt8OrZero(expr) AS COALESCE(TRY_CAST(expr AS UTINYINT), 0);
CREATE OR REPLACE MACRO toUInt16OrZero(expr) AS COALESCE(TRY_CAST(expr AS USMALLINT), 0);
CREATE OR REPLACE MACRO toUInt32OrZero(expr) AS COALESCE(TRY_CAST(expr AS UINTEGER), 0);
CREATE OR REPLACE MACRO toUInt64OrZero(expr) AS COALESCE(TRY_CAST(expr AS UBIGINT), 0);
CREATE OR REPLACE MACRO toUInt8OrNull(expr) AS TRY_CAST(expr AS UTINYINT);
CREATE OR REPLACE MACRO toUInt16OrNull(expr) AS TRY_CAST(expr AS USMALLINT);
CREATE OR REPLACE MACRO toUInt32OrNull(expr) AS TRY_CAST(expr AS UINTEGER);
CREATE OR REPLACE MACRO toUInt64OrNull(expr) AS TRY_CAST(expr AS UBIGINT);
-- Floating-point conversion macros
CREATE OR REPLACE MACRO toFloat(expr) AS CAST(expr AS DOUBLE);
CREATE OR REPLACE MACRO toFloatOrNull(expr) AS TRY_CAST(expr AS DOUBLE);
CREATE OR REPLACE MACRO toFloatOrZero(expr) AS COALESCE(TRY_CAST(expr AS DOUBLE), 0);
-- Arithmetic macros
CREATE OR REPLACE MACRO intDiv(a, b) AS (CAST(a AS BIGINT) // CAST(b AS BIGINT));
CREATE OR REPLACE MACRO intDivOrZero(a, b) AS COALESCE((a / b), 0);
CREATE OR REPLACE MACRO tupleDivide(a, b) AS (apply(a, (x,i) -> apply(b, x -> CAST(x AS BIGINT))[i] // CAST(x AS BIGINT)));
CREATE OR REPLACE MACRO tupleMinus(a, b) AS (apply(a, (x,i) -> apply(b, x -> CAST(x AS BIGINT))[i] - CAST(x AS BIGINT)));
CREATE OR REPLACE MACRO tuplePlus(a, b) AS (apply(a, (x,i) -> apply(b, x -> CAST(x AS BIGINT))[i] + CAST(x AS BIGINT)));
CREATE OR REPLACE MACRO tupleMultiply(a, b) AS (apply(a, (x,i) -> CAST(apply(b, x -> CAST(x AS BIGINT))[i] as BIGINT) * CAST(x AS BIGINT)));
CREATE OR REPLACE MACRO tupleMultiplyByNumber(a, b) AS (apply(a, (x) -> CAST(apply(b, x -> CAST(x AS BIGINT))[1] as BIGINT) * CAST(x AS BIGINT)));
-- String matching macro
CREATE OR REPLACE MACRO match(string, token) AS (string LIKE token);
-- Array macros
CREATE OR REPLACE MACRO arrayExists(needle, haystack) AS (haystack @> ARRAY[needle]);
CREATE OR REPLACE MACRO arrayMap(e, arr) AS (array_transform(arr, e -> (e * e)));
