#ifndef YEZZEY_PG_H
#define YEZZEY_PG_H

#ifdef __cplusplus
extern "C" {
#endif

#include "c.h"
#include "postgres.h"

#if PG_VERSION_NUM >= 130000
#include "postmaster/interrupt.h"
#endif

#include "utils/timestamp.h"
#if PG_VERSION_NUM >= 100000
#include "common/file_perm.h"
#else
#include "access/xact.h"
#endif

#include "utils/elog.h"

#include "pgstat.h"
#include "utils/builtins.h"

#include "access/aosegfiles.h"
#include "access/htup_details.h"
#include "utils/tqual.h"

#include "catalog/pg_namespace.h"
#include "catalog/pg_tablespace.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "storage/smgr.h"
#include "utils/catcache.h"
#include "utils/syscache.h"

// For GpIdentity
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

#if PG_VERSION_NUM >= 130000
#include "postmaster/interrupt.h"
#endif

#if PG_VERSION_NUM >= 100000
#include "common/file_perm.h"
#else
#include "access/xact.h"
#endif

#include "utils/elog.h"

// For GpIdentity
#ifdef GPBUILD
#include "catalog/pg_tablespace.h"
#include "cdb/cdbappendonlyxlog.h"
#include "cdb/cdbvars.h"
#endif

#include "catalog/pg_namespace.h"
#include "utils/catcache.h"
#include "utils/syscache.h"

#include "catalog/heap.h"
#include "catalog/pg_namespace.h"

#include "access/multixact.h"
#include "nodes/primnodes.h"
#include "utils/ps_status.h"

#include "catalog/index.h"
#include "catalog/oid_dispatch.h"

#include "catalog/pg_opclass.h"

#ifdef __cplusplus
}
#endif

#endif /* YEZZEY_PG_H */
