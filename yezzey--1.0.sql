\echo Use "CREATE EXTENSION yezzey" to load this file. \quit

CREATE SCHEMA yezzey;

CREATE TABLE yezzey.move_table(oid text NOT NULL UNIQUE,
			       forkName text NOT NULL,
			       segNum text NOT NULL,
			       isLocal bool);

CREATE OR REPLACE FUNCTION move_to_s3(text) RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE
LANGUAGE C STRICT;
