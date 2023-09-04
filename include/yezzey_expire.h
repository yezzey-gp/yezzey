#ifndef YEZZEY_EXPIRE
#define YEZZEY_EXPIRE

#include "pg.h"

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

#define YEZZEY_EXPIRE_INDEX_RELATION 8700
#define YEZZEY_EXPIRE_INDEX_RELATION_INDX 8701
/*

CREATE TABLE yezzey.yezzey_expire_index(
    reloid           OID,
    relfileoid       OID,
    fqnmd5           TEXT,
    last_use_lsn     LSN,
    expire_lsn       LSN
)
DISTRIBUTED REPLICATED;
*/

// Relfileoid changes after alter table stmt.
// We save md5 of fully qualified name (relschema.relname)

typedef struct {
  Oid yreloid;             /* Expired Yezzey relation OID */
  Oid yrelfileoid;         /* Expired fileoid. */
  XLogRecPtr last_use_lsn; /* Last Alive LSN */
  XLogRecPtr expire_lsn;   /* Last Alive LSN */
  text fqdnmd5;            /* md5(relschema.relname) */
} FormData_yezzey_expire_index;

typedef FormData_yezzey_expire_index *Form_yezzey_expire_index;

#define Natts_yezzey_expire_index 5
#define Anum_yezzey_expire_index_reloid 1
#define Anum_yezzey_expire_index_relfileoid 2
#define Anum_yezzey_expire_index_last_use_lsn 3
#define Anum_yezzey_expire_index_lsn 4
#define Anum_yezzey_expire_index_fqnmd5 5

/* record that current relation relfilenode is expired */
EXTERNC void YezzeyRecordRelationExpireLsn(Relation rel);

EXTERNC void YezzeyCreateRelationExpireIndex(void);

EXTERNC void YezzeyUpsertLastUseLsn(Oid reloid, Oid relfileoid, const char *md5,
                                    XLogRecPtr lsn);

#endif /* YEZZEY_EXPIRE */