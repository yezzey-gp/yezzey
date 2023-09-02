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
    expire_lsn       LSN
)
DISTRIBUTED REPLICATED;
*/

typedef struct {
    Oid yreloid;            /* Expired Yezzey relation OID */
    XLogRecPtr expire_lsn;  /* Last Alive LSN */
} FormData_yezzey_expire_index;

#define Natts_yezzey_expire_index 2
#define Anum_yezzey_expire_index_reloid 1
#define Anum_yezzey_expire_index_lsn 2

EXTERNC void
YezzeyRecordRelationExpireLsn(Oid reloid);

EXTERNC Oid YezzeyCreateRelationExpireIndex(void);



#endif /* YEZZEY_EXPIRE */