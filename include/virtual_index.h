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
}

#include <string>
#endif

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

/* ----------------
 *		compiler constants for pg_database
 * ----------------
 */
#define Natts_yezzey_virtual_index 5
#define Anum_yezzey_virtual_index 1
#define Anum_yezzey_virtual_start_off 2
#define Anum_yezzey_virtual_finish_off 3
#define Anum_yezzey_virtual_modcount 4
#define Anum_yezzey_virtual_ext_path 5

EXTERNC void YezzeyCreateAuxIndex(Relation aorel, Oid reloid);

EXTERNC Oid YezzeyFindAuxIndex(Oid reloid);

EXTERNC void emptyYezzeyIndex(Oid yezzey_index_oid);

#ifdef __cplusplus
void YezzeyVirtualIndexInsert(Oid yandexoid /*yezzey auxiliary index oid*/,
                              int64_t segindx, int64_t modcount,
                              const std::string &ext_path);
#else
#endif
