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
    expire_lsn       LSN
)
DISTRIBUTED REPLICATED;
*/

// Relfileoid changes after alter table stmt.
// We save md5 of fully qualified name (relschema.relname)

typedef struct {
    Oid yreloid;            /* Expired Yezzey relation OID */
    Oid yrelfileoid;        /* Expired fileoid. */
    text fqdnmd5;           /* md5(relschema.relname) */
    XLogRecPtr expire_lsn;  /* Last Alive LSN */
} FormData_yezzey_expire_index;

#define Natts_yezzey_expire_index 4
#define Anum_yezzey_expire_index_reloid 1
#define Anum_yezzey_expire_index_relfileoid 2
#define Anum_yezzey_expire_index_fqnmd5 3
#define Anum_yezzey_expire_index_lsn 4

EXTERNC void
YezzeyRecordRelationExpireLsn(Relation rel);

EXTERNC void YezzeyCreateRelationExpireIndex(void);



#endif /* YEZZEY_EXPIRE */