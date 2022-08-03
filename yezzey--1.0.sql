\echo Use "CREATE EXTENSION yezzey" to load this file. \quit

CREATE SCHEMA yezzey;

CREATE TABLE yezzey.metatable(name text NOT NULL UNIQUE, processed bool) DISTRIBUTED REPLICATED;

CREATE OR REPLACE FUNCTION move_to_s3(text) RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE
LANGUAGE C STRICT;

