
#pragma once

#include "pg.h"
#include "virtual_index.h"
#include "virtual_tablespace.h"

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

/*

CREATE TABLE yezzey.offload_tablespace_map(
    reloid                 OID PRIMARY KEY,
    origin_tablespace_name TEXT
) DISTRIBUTED REPLICATED;
*/

#define Natts_offload_tablespace_map 2

#define Anum_offload_tablespace_map_reloid 1
#define Anumoffload_tablespace_map_origin_tablespace_name 2

typedef struct {
  Oid reloid;                 /* OID of offloaded relation */
  text origin_tablespace_name; /* offloading original tablespace of relation */
} FormData_offload_tablespace_map;

typedef FormData_offload_tablespace_map *Form_offload_tablespace_map;

#ifdef __cplusplus
std::string YezzeyGetRelationOriginTablespace(Oid i_reloid);
#endif