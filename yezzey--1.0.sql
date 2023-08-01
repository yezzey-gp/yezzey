\echo Use "CREATE EXTENSION yezzey" to load this file. \quit
-- this should be run in database yezzey!

-- create database yezzey;

CREATE SCHEMA yezzey;

-- since GP uses segment-file discovery technique
-- in can fail to remove some AO/AOCS relation files locally
-- in cases when table write happened after folloading
-- see ao_foreach_extent_file
-- 

CREATE OR REPLACE FUNCTION 
yezzey_offload_relation(reloid OID, remove_locally BOOLEAN) RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION
yezzey_load_relation_seg(reloid OID, dest_path TEXT)
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION
yezzey_load_relation(reloid OID, dest_path TEXT)
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON MASTER
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION yezzey_offload_relation_to_external_path(
    reloid OID, 
    remove_locally BOOLEAN, 
    external_storage_path TEXT
) RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION yezzey_delete_chunk(
    external_storage_path TEXT
) RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION yezzey_show_relation_external_path(
    reloid OID,
    segno INT
) RETURNS TEXT
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION yezzey_show_relation_external_path(
    offload_relname TEXT,
    segno INT
) RETURNS TEXT
AS $$
    SELECT * FROM yezzey_show_relation_external_path(
        (SELECT OID FROM pg_class WHERE relname=offload_relname),
        segno
    )
$$
EXECUTE ON ALL SEGMENTS
LANGUAGE SQL;


CREATE OR REPLACE FUNCTION yezzey_define_relation_offload_policy_internal(reloid OID) RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON MASTER
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION yezzey_define_relation_offload_policy_internal_seg(reloid OID) RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;

CREATE TYPE offload_policy AS ENUM ('remote_always', 'cache_writes');

CREATE OR REPLACE FUNCTION yezzey_init_metadata() RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON MASTER
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION yezzey_init_metadata_seg() RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;

-- manually/automatically relocated relations

SELECT yezzey_init_metadata();
SELECT yezzey_init_metadata_seg();

CREATE OR REPLACE FUNCTION
yezzey_define_offload_policy(i_offload_nspname TEXT, i_offload_relname TEXT, i_policy offload_policy DEFAULT 'remote_always')
RETURNS VOID
AS $$
DECLARE
    v_reloid OID;
BEGIN
    SELECT 
        oid
    FROM 
        pg_catalog.pg_class
    INTO v_reloid 
    WHERE 
        relname = i_offload_relname AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = i_offload_nspname);

    IF NOT FOUND THEN
        RAISE EXCEPTION 'relation % is not found in pg_class', i_offload_relname;
    END IF;

    PERFORM yezzey_define_relation_offload_policy_internal_seg(
        v_reloid
    );
    PERFORM yezzey_define_relation_offload_policy_internal(
        v_reloid
    );
END;
$$
LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION yezzey_set_relation_expirity_seg(
    i_reloid OID,
    i_relpolicy INT,
    i_relexp TIMESTAMP
)
RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION yezzey_relation_expirity_seg(
    i_reloid OID,
    i_relpolicy INT,
    i_relexp TIMESTAMP
)
RETURNS void
AS $$ 
SELECT yezzey_set_relation_expirity_seg(i_reloid, i_relpolicy, i_relexp);
$$
EXECUTE ON ALL SEGMENTS
LANGUAGE SQL;


CREATE OR REPLACE FUNCTION
yezzey_set_relation_expirity(i_offload_nspname TEXT, i_offload_relname TEXT, i_relexp TIMESTAMP, 
    i_relpolicy offload_policy DEFAULT 'remote_always')
RETURNS VOID
AS $$
DECLARE
    v_reloid OID;
BEGIN
    SELECT 
        oid
    FROM 
        pg_catalog.pg_class
    INTO v_reloid 
    WHERE 
        relname = i_offload_relname AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = i_offload_nspname);

    IF NOT FOUND THEN
        RAISE EXCEPTION 'relation % is not found in pg_class', i_offload_relname;
    END IF;

    PERFORM yezzey_relation_expirity_seg(
        v_reloid,
        1,
        i_relexp
    );
END;
$$
LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION
yezzey_define_offload_policy(i_offload_relname TEXT, i_policy offload_policy DEFAULT 'remote_always')
RETURNS VOID
AS $$
BEGIN
    PERFORM yezzey_define_offload_policy('public', i_offload_relname, i_policy);
END;
$$
LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION
yezzey_offload_relation(offload_nspname TEXT, offload_relname TEXT, remove_locally BOOLEAN DEFAULT TRUE)
RETURNS VOID
AS $$
DECLARE
    v_tmp_relname yezzey.offload_metadata%rowtype;
    v_reloid OID;
BEGIN
    SELECT 
        oid
    FROM 
        pg_catalog.pg_class
    INTO v_reloid 
    WHERE 
        relname = offload_relname AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = offload_nspname);

    -- SELECT * FROM yezzey.offload_metadata INTO v_tmp_relname WHERE reloid = v_reloid;
    -- IF NOT FOUND THEN
        -- RAISE WARNING'relation %.% is not in offload metadata table', offload_nspname, offload_relname;
    -- END IF;
    PERFORM yezzey_offload_relation(
        v_reloid,
        remove_locally
    );
END;
$$
LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION
yezzey_offload_relation(offload_relname TEXT, remove_locally BOOLEAN DEFAULT TRUE)
RETURNS VOID
AS $$
BEGIN
    PERFORM yezzey_offload_relation('public', offload_relname, remove_locally);
END;
$$
LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION
yezzey_offload_relation_to_external_path(
    offload_nspname TEXT,
    offload_relname TEXT, 
    remove_locally BOOLEAN DEFAULT TRUE, 
    external_storage_path TEXT DEFAULT NULL)
RETURNS VOID
AS $$
DECLARE
    v_tmp_relname yezzey.offload_metadata%rowtype;
    v_reloid OID;
BEGIN
    SELECT 
        oid
    FROM 
        pg_catalog.pg_class
    INTO v_reloid 
    WHERE 
        relname = offload_relname AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = offload_nspname);

    -- SELECT * FROM yezzey.offload_metadata INTO v_tmp_relname WHERE reloid = v_reloid;
    -- IF NOT FOUND THEN
    --     RAISE WARNING'relation %.% is not in offload metadata table', offload_nspname, offload_relname;
    -- END IF;
    PERFORM yezzey_offload_relation_to_external_path(
        v_reloid,
        remove_locally,
        external_storage_path
    );
END;
$$
LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION
yezzey_offload_relation_to_external_path(
    offload_relname TEXT, 
    remove_locally BOOLEAN DEFAULT TRUE, 
    external_storage_path TEXT DEFAULT NULL)
RETURNS VOID
AS $$
BEGIN
    PERFORM yezzey_offload_relation_to_external_path('public', offload_relname, remove_locally, external_storage_path);
END;
$$
LANGUAGE PLPGSQL;



CREATE OR REPLACE FUNCTION
yezzey_load_relation(load_nspname TEXT, load_relname TEXT)
RETURNS VOID
AS $$
DECLARE
    v_tmp_relname yezzey.offload_metadata%rowtype;
    v_reloid OID;
BEGIN
    SELECT 
        oid
    FROM 
        pg_catalog.pg_class
    INTO v_reloid 
    WHERE 
        relname = load_relname AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = load_nspname);

    -- SELECT * FROM yezzey.offload_metadata INTO v_tmp_relname WHERE reloid = v_reloid;
    -- IF NOT FOUND THEN
    --     RAISE WARNING'relation % is not in offload metadata table', load_relname;
    -- END IF;
    PERFORM yezzey_load_relation_seg(
        v_reloid,
        ''-- omit dest path 
    );

    PERFORM yezzey_load_relation(
        v_reloid,
        ''-- omit dest path 
    );

    RAISE INFO'loaded relation %s to local storage', load_relname;
END;
$$
LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION
yezzey_load_relation(load_relname TEXT)
RETURNS VOID
AS $$
DECLARE
    v_tmp_relname yezzey.offload_metadata%rowtype;
BEGIN
    PERFORM yezzey_load_relation(
        'public',
        load_relname
    );
END;
$$
LANGUAGE PLPGSQL;



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

-- even more detailed debug about relations file segments
CREATE OR REPLACE FUNCTION yezzey_relation_describe_external_storage_structure_internal(reloid OID) 
RETURNS TABLE (reloid OID, segindex INTEGER, segfileindex INTEGER, external_storage_filepath TEXT, local_bytes BIGINT, local_commited_bytes BIGINT, external_bytes BIGINT)
AS 'MODULE_PATHNAME'
VOLATILE
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION yezzey_offload_relation_status(
    i_nspname TEXT,
    i_relname TEXT
) 
RETURNS TABLE (offload_reloid OID, segindex INTEGER, local_bytes BIGINT, local_commited_bytes BIGINT, external_bytes BIGINT)
AS $$
DECLARE
    v_tmp_relname yezzey.offload_metadata%rowtype;
    v_reloid OID;
BEGIN

    SELECT 
        oid
    FROM 
        pg_catalog.pg_class
    INTO v_reloid 
    WHERE 
        relname = i_relname AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = i_nspname);

    -- SELECT * FROM yezzey.offload_metadata INTO v_tmp_relname WHERE reloid = v_reloid;
    -- IF NOT FOUND THEN
    --     RAISE WARNING'relation %.% is not in offload metadata table', i_nspname, i_relname;
    -- END IF;

    RETURN QUERY SELECT 
        *
    FROM yezzey_offload_relation_status_internal(
        v_reloid
    );
END;
$$
EXECUTE ON ALL SEGMENTS
LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION yezzey_offload_relation_status(i_relname TEXT) 
RETURNS TABLE (offload_reloid OID, segindex INTEGER, local_bytes BIGINT, local_commited_bytes BIGINT, external_bytes BIGINT)
AS $$
BEGIN
    RETURN QUERY SELECT 
        *
    FROM yezzey_offload_relation_status(
        'public',
        i_relname
    );
END;
$$
EXECUTE ON ALL SEGMENTS
LANGUAGE PLPGSQL;



CREATE OR REPLACE FUNCTION yezzey_offload_relation_status_per_filesegment(
    i_nspname TEXT,
    i_relname TEXT
    ) 
RETURNS TABLE (offload_reloid OID, segindex INTEGER, segfileindex INTEGER, local_bytes BIGINT, local_commited_bytes BIGINT, external_bytes BIGINT)
AS $$
DECLARE
    v_tmp_relname yezzey.offload_metadata%rowtype;
    v_reloid OID;
BEGIN

    SELECT 
        oid
    FROM 
        pg_catalog.pg_class
    INTO v_reloid 
    WHERE 
        relname = i_relname AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = i_nspname);

    -- SELECT * FROM yezzey.offload_metadata INTO v_tmp_relname WHERE reloid = v_reloid;

    -- IF NOT FOUND THEN
    --     RAISE WARNING'relation %.% is not in offload metadata table', i_nspname, i_relname;
    -- END IF;
 
    RETURN QUERY SELECT 
        *
    FROM yezzey_offload_relation_status_per_filesegment(
        v_reloid
    );
END;
$$
EXECUTE ON ALL SEGMENTS
LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION yezzey_offload_relation_status_per_filesegment(i_relname TEXT) 
RETURNS TABLE (offload_reloid OID, segindex INTEGER, segfileindex INTEGER, local_bytes BIGINT, local_commited_bytes BIGINT, external_bytes BIGINT)
AS $$
DECLARE
    v_tmp_relname yezzey.offload_metadata%rowtype;
BEGIN
 
    RETURN QUERY SELECT 
        *
    FROM yezzey_offload_relation_status_per_filesegment(
        'public',
        i_relname
    );
END;
$$
EXECUTE ON ALL SEGMENTS
LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION yezzey_relation_describe_external_storage_structure(
    i_nspname TEXT, i_relname TEXT) 
RETURNS TABLE (offload_reloid OID, segindex INTEGER, segfileindex INTEGER, external_storage_filepath TEXT, local_bytes BIGINT, local_commited_bytes BIGINT, external_bytes BIGINT)
AS $$
DECLARE
    v_tmp_relname yezzey.offload_metadata%rowtype;
    v_reloid OID;
BEGIN
    SELECT 
        oid
    FROM 
        pg_catalog.pg_class
    INTO v_reloid 
    WHERE 
        relname = i_relname AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = i_nspname);

    -- SELECT * FROM yezzey.offload_metadata INTO v_tmp_relname WHERE reloid = v_reloid;

    -- IF NOT FOUND THEN
    --     RAISE WARNING'relation %.% is not in offload metadata table', i_nspname, i_relname;
    -- END IF;
 
    RETURN QUERY SELECT 
        *
    FROM yezzey_relation_describe_external_storage_structure_internal(
        v_reloid
    );
END;
$$
EXECUTE ON ALL SEGMENTS
LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION yezzey_relation_describe_external_storage_structure(i_relname TEXT) 
RETURNS TABLE (offload_reloid OID, segindex INTEGER, segfileindex INTEGER, external_storage_filepath TEXT, local_bytes BIGINT, local_commited_bytes BIGINT, external_bytes BIGINT)
AS $$
BEGIN
    RETURN QUERY SELECT 
        *
    FROM yezzey_relation_describe_external_storage_structure(
        'public',
        i_relname
    );
END;
$$
EXECUTE ON ALL SEGMENTS
LANGUAGE PLPGSQL;



CREATE TABLE yezzey.auto_offload_relations(
    reloid OID,
    expire_date DATE
)
DISTRIBUTED REPLICATED;


CREATE OR REPLACE FUNCTION yezzey_auto_offload_relation(offload_nspname TEXT, offload_relname TEXT, expire_date DATE) RETURNS VOID
AS $$
    INSERT INTO yezzey.auto_offload_relations (reloid, expire_date) VALUES ((select oid from pg_class where relname=offload_relname), expire_date);
$$
LANGUAGE SQL;

CREATE OR REPLACE FUNCTION yezzey_auto_offload_relation(offload_relname TEXT, expire_date DATE) RETURNS VOID
AS $$
    SELECT yezzey_auto_offload_relation('public', offload_relname, expire_date);
$$
LANGUAGE SQL;

CREATE OR REPLACE FUNCTION yezzey_part_date_eval(expression text) RETURNS DATE
AS
$$
declare
  result date;
BEGIN
  EXECUTE expression INTO result;
  RETURN result;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION yezzey_dump_virtual_index(i_relname text) 
RETURNS TABLE(segno integer, relfilenode OID, offset_start bigint, offset_finish bigint, modcount bigint, lsn pg_lsn, x_path TEXT)
AS $$
DECLARE
    v_reloid OID;
BEGIN
    select oid from pg_class INTO v_reloid where relname = i_relname;
    RETURN QUERY EXECUTE 'SELECT * FROM yezzey.yezzey_virtual_index'||v_reloid||';';
END;
$$
EXECUTE ON ALL SEGMENTS
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION yezzey_check_part_exr(
    i_node pg_node_tree
)
RETURNS BOOLEAN
AS 'MODULE_PATHNAME'
VOLATILE
LANGUAGE C STRICT;

