#ifndef YEZZEY_VIRTUAL_TABLESPACE
#define YEZZEY_VIRTUAL_TABLESPACE

#include "pg.h"

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

EXTERNC void YezzeyATExecSetTableSpace(Relation aorel, Oid reloid,
                                Oid desttablespace_oid);

#endif /* YEZZEY_VIRTUAL_TABLESPACE */