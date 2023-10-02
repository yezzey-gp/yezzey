#ifndef YEZZEY_HEAP_API_H
#define YEZZEY_HEAP_API_H

#include "pg.h"

#if PG_VERSION_NUM >= 120000
#define yezzey_relation_open table_open
#else
#define yezzey_relation_open heap_open
#endif

#endif /* YEZZEY_HEAP_API_H */