
#pragma once

#include "pg.h"

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
#define Anum_offload_metadata_relext_time 3
#define Anum_offload_metadata_rellast_archived 4

typedef struct {
  Oid reloid;                 /* OID of offloaded relation */
  int32_t relpolicy;          /* offloading policy of relation */
  Timestamp relext_time;      /* Relation (local) expiry time  */
  Timestamp rellast_archived; /* Relation last archived time */
} FormData_yezzey_offload_metadata;

typedef FormData_yezzey_offload_metadata *Form_yezzey_offload_metadata;

#define Offload_policy_always_remote 1
#define Offload_policy_cache_writes 2

EXTERNC void YezzeyOffloadPolicyRelation();

EXTERNC
void YezzeyOffloadPolicyInsert(Oid reloid /* offload relation oid */,
                               int offload_policy);
