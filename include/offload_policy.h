
#pragma once

#include "pg.h"
#include "virtual_index.h"
#include "virtual_tablespace.h"

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

#define YEZZEY_OFFLOAD_POLICY_RELATION 8600

#define YEZZEY_OFFLOAD_POLICY_RELATION_INDX 8601
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
#define Anum_offload_metadata_relext_time 3
#define Anum_offload_metadata_rellast_archived 4

typedef struct {
  Oid reloid;                 /* OID of offloaded relation */
  int64_t relpolicy;          /* offloading policy of relation */
  Timestamp relext_time;      /* Relation (local) expiry time  */
  Timestamp rellast_archived; /* Relation last archived time */
} FormData_yezzey_offload_metadata;

typedef FormData_yezzey_offload_metadata *Form_yezzey_offload_metadata;

#define Offload_policy_always_remote 1
#define Offload_policy_cache_writes 2
/* Status for loaded relation  */
#define Offload_policy_local 3

#ifdef __cplusplus
bool YezzeyCheckRelationOffloaded(Oid relid);
#endif

EXTERNC void YezzeyCreateOffloadPolicyRelation();

EXTERNC void
YezzeySetRelationExpiritySeg(Oid i_reloid /* offload relation oid */,
                             int i_relpolicy /* offload relation policy */,
                             Timestamp i_relexp);

EXTERNC void YezzeyDefineOffloadPolicy(Oid reloid);

/*
 * YezzeyLoadRealtion:
 * do all offload-metadata related work for relation loading:
 * 1) simply change relation offload policy in yezzey.offload_metadata
 * 2) ????
 * 3) success
 */
EXTERNC void YezzeyLoadRealtion(Oid i_reloid);