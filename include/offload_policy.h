
#pragma once

#ifdef __cplusplus
extern "C" {
#include "postgres.h"

#include "catalog/dependency.h"
#include "catalog/pg_extension.h"
#include "commands/extension.h"

#include "access/xlog.h"
#include "catalog/storage_xlog.h"
#include "common/relpath.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "pgstat.h"
#include "utils/builtins.h"

#include "access/aocssegfiles.h"
#include "access/aosegfiles.h"
#include "storage/lmgr.h"
#include "utils/tqual.h"

#include "utils/fmgroids.h"

#include "catalog/catalog.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_tablespace.h"
#include "catalog/storage.h"

#include "catalog/indexing.h"

#include "utils/snapmgr.h"

#include "utils/guc.h"

#include "fmgr.h"

// #include "smgr_s3_frontend.h"

#if PG_VERSION_NUM >= 130000
#include "postmaster/interrupt.h"
#endif

#if PG_VERSION_NUM >= 100000
#include "common/file_perm.h"
#else
#include "access/xact.h"
#endif

#include "utils/elog.h"

#ifdef GPBUILD
#include "cdb/cdbvars.h"
#endif

// For GpIdentity
#include "c.h"
#include "catalog/pg_namespace.h"
#include "utils/catcache.h"
#include "utils/syscache.h"

#include "catalog/heap.h"
#include "catalog/pg_namespace.h"

#include "nodes/primnodes.h"

#include "utils/timestamp.h"
}

#include <string>
#else
#include "postgres.h"
#endif

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

#define YEZZEY_OFFLOAD_POLICY_RELATION 8600


/*

CREATE TABLE yezzey.offload_metadata(
    reloid           OID,
    relpolicy        offload_policy NOT NULL,
    relext_date      DATE,
    rellast_archived TIMESTAMP
)
DISTRIBUTED REPLICATED;
*/

#define Natts_offload_metadata 4

#define Anum_offload_metadata_reloid 1
#define Anum_offload_metadata_relpolicy 2
#define Anum_offload_metadata_relext_date 3
#define Anum_offload_metadata_rellast_archived 4

#define Offload_policy_always_remote 1
#define Offload_policy_cache_writes 2

EXTERNC void YezzeyOffloadPolicyRelation();

EXTERNC 
void YezzeyOffloadPolicyInsert(Oid reloid /* offload relation oid */, int offload_policy);
