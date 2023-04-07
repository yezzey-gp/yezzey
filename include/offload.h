
#ifndef YEZZEY_OFFLOAD_H
#define YEZZEY_OFFLOAD_H

#include "pg.h"

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

EXTERNC int
yezzey_offload_relation_internal_rel(Relation aorel, bool remove_locally,
                                     const char *external_storage_path);

EXTERNC int yezzey_offload_relation_internal(Oid reloid, bool remove_locally,
                                             const char *external_storage_path);

#endif /* YEZZEY_OFFLOAD_H */