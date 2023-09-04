#pragma once

#include "pg.h"
#include "url.h"
#include "virtual_index.h"
#include "yezzey_expire.h"

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

EXTERNC void YezzeyUpdateMetadataRelations(
    Oid yandexoid /*yezzey auxiliary index oid*/, Oid reloid,
    Oid relfilenodeOid, int64_t blkno, int64_t offset_start,
    int64_t offset_finish, int32_t encrypted, int32_t reused, int64_t modcount,
    XLogRecPtr lsn, const char *x_path /* external path */, const char *md5);
