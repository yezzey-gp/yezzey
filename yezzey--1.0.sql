\echo Use "CREATE EXTENSION yezzey" to load this file. \quit
-- this should be run in database yezzey!

-- create database yezzey;

CREATE SCHEMA yezzey;

CREATE OR REPLACE FUNCTION yezzey.offload_relation(reloid OID) RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;

-- manually relocated relations
CREATE TABLE relocated_relations(relname TEXT, last_archived TIMESTAMP)
DISTRIBUTED REPLICATED;

CREATE OR REPLACE FUNCTION
yezzey.offload_relation(offload_relname TEXT)
RETURNS VOID
AS $$
    INSERT INTO relocated_relations VALUES(offload_relname, NOW());
    SELECT yezzey.offload_relation(
        (SELECT OID FROM pg_class WHERE relname=offload_relname)
    );
$$
LANGUAGE SQL;


CREATE OR REPLACE FUNCTION
yezzey.load_relation(reloid OID)
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


-- CREATE OR REPLACE FUNCTION yezzey.offload_relation_status(relname TEXT) RETURNS void
-- AS 'MODULE_PATHNAME'
-- VOLATILE
-- LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION yezzey.force_segment_offload(reloid OID, segid INT) RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE
LANGUAGE C STRICT;

CREATE TABLE yezzey.auto_offload_relations(
    reloid OID,
    expire_date DATE
)
DISTRIBUTED REPLICATED;


CREATE OR REPLACE FUNCTION yezzey.auto_offload_relation(offload_relname TEXT, expire_date DATE) RETURNS VOID
AS $$
    INSERT INTO yezzey.auto_offload_relations (reloid, expire_date) VALUES ((select oid from pg_class where relname=offload_relname), expire_date);
$$
LANGUAGE SQL;

