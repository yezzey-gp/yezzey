#ifndef YEZZEY_PARTITION_H
#define YEZZEY_PARTITION_H

#include "pg.h"

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

EXTERNC bool yezzey_get_expr_worker(text *expr);

#endif /* YEZZEY_PARTITION_H */