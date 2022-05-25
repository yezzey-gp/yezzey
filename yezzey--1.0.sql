\echo Use "CREATE EXTENSION yezzey" to load this file. \quit

CREATE FUNCTION test_function()
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;
