#ifndef YEZZEY_WORKER_H
#define YEZZEY_WORKER_H

#include "pg.h"

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

EXTERNC void processOffloadedRelations();
EXTERNC void processPartitionOffload();

#endif /*YEZZEY_WORKER_H*/