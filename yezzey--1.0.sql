\echo Use "CREATE EXTENSION yezzey" to load this file. \quit
-- this should be run in database yezzey!

-- create database yezzey;

CREATE SCHEMA yezzey;

CREATE OR REPLACE FUNCTION yezzey_offload_relation(reloid OID) RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;

CREATE TYPE offload_policy AS ENUM ('local', 'remote_always', 'cron');

-- manually/automatically relocated relations
CREATE TABLE yezzey.offload_metadata(
    relname          TEXT NOT NULL UNIQUE,
    relpolicy        offload_policy NOT NULL,
    relext_date      DATE,
    rellast_archived TIMESTAMP
)
DISTRIBUTED REPLICATED;

CREATE OR REPLACE FUNCTION
yezzey_define_offload_policy(offload_relname TEXT, policy offload_policy DEFAULT 'remote_always')
RETURNS VOID
AS $$
DECLARE
    v_pg_class_entry pg_catalog.pg_class%rowtype;
BEGIN
    SELECT * FROM pg_catalog.pg_class INTO v_pg_class_entry WHERE relname = offload_relname;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'relation % is not found in pg_class', offload_relname;
    END IF;
    INSERT INTO yezzey.offload_metadata VALUES(offload_relname, policy, NULL, NOW());
    -- get xid before executing offload realtion func
    PERFORM yezzey_offload_relation(
        (SELECT oid FROM pg_catalog.pg_class WHERE relname=offload_relname)
    );
END;
$$
LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION
yezzey_offload_relation(offload_relname TEXT)
RETURNS VOID
AS $$
DECLARE
    v_tmp_relname yezzey.offload_metadata%rowtype;
BEGIN
    SELECT * FROM yezzey.offload_metadata INTO v_tmp_relname WHERE relname = offload_relname;
    IF NOT FOUND THEN
        RAISE WARNING'relation % is not in offload metadata table', offload_relname;
    END IF;
    UPDATE yezzey.offload_metadata SET rellast_archived = NOW() WHERE relname=offload_relname;
    PERFORM yezzey_offload_relation(
        (SELECT OID FROM pg_class WHERE relname=offload_relname)
    );
    UPDATE yezzey.offload_metadata SET rellast_archived = NOW() WHERE relname=offload_relname;
END;
$$
LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION
yezzey.load_relation(reloid OID)
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


-- external bytes always commited

CREATE OR REPLACE FUNCTION yezzey_offload_relation_status_internal(reloid OID) 
RETURNS TABLE (reloid OID, segindex INTEGER, local_bytes BIGINT, local_commited_bytes BIGINT, external_bytes BIGINT)
AS 'MODULE_PATHNAME'
VOLATILE
LANGUAGE C STRICT;


-- more detailed debug about relations file segments
CREATE OR REPLACE FUNCTION yezzey_offload_relation_status_per_filesegment(reloid OID) 
RETURNS TABLE (reloid OID, segindex INTEGER, segfileindex INTEGER, local_bytes BIGINT, local_commited_bytes BIGINT, external_bytes BIGINT)
AS 'MODULE_PATHNAME'
VOLATILE
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION yezzey_offload_relation_status(i_relname TEXT) 
RETURNS TABLE (reloid OID, segindex INTEGER, local_bytes BIGINT, local_commited_bytes BIGINT, external_bytes BIGINT)
AS $$
DECLARE
    v_tmp_relname yezzey.offload_metadata%rowtype;
BEGIN
    SELECT * FROM yezzey.offload_metadata INTO v_tmp_relname WHERE relname = i_relname;
    IF NOT FOUND THEN
        RAISE WARNING'relation % is not in offload metadata table', i_relname;
    END IF;

    RETURN QUERY SELECT 
        *
    FROM yezzey_offload_relation_status_internal(
        i_relname::regclass::oid
    );
END;
$$
EXECUTE ON ALL SEGMENTS
LANGUAGE PLPGSQL;



CREATE OR REPLACE FUNCTION yezzey_offload_relation_status_per_filesegment(i_relname TEXT) 
RETURNS TABLE (reloid OID, segindex INTEGER, segfileindex INTEGER, local_bytes BIGINT, local_commited_bytes BIGINT, external_bytes BIGINT)
AS $$
DECLARE
    v_tmp_relname yezzey.offload_metadata%rowtype;
BEGIN
    SELECT * FROM yezzey.offload_metadata INTO v_tmp_relname WHERE relname = i_relname;
    IF NOT FOUND THEN
        RAISE WARNING'relation % is not in offload metadata table', i_relname;
    END IF;
 
    RETURN QUERY SELECT 
        *
    FROM yezzey_offload_relation_status_per_filesegment(
        i_relname::regclass::oid
    );
END;
$$
EXECUTE ON ALL SEGMENTS
LANGUAGE PLPGSQL;


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

