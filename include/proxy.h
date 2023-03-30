#ifndef YEZZEY_PROXY_H
#define YEZZEY_PROXY_H

#ifdef __cplusplus
extern "C" {

#include "postgres.h"

#if PG_VERSION_NUM >= 130000
#include "postmaster/interrupt.h"
#endif

#if PG_VERSION_NUM >= 100000
#include "common/file_perm.h"
#else
#include "access/xact.h"
#endif

#include "utils/elog.h"

#ifdef GPBUILD
#include "cdb/cdbvars.h"
#endif
}

#include "gucs.h"
#include "storage.h"

#endif

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

EXTERNC int64 yezzey_NonVirtualCurSeek(SMGRFile file);
EXTERNC void yezzey_FileClose(SMGRFile file);
EXTERNC int64 yezzey_FileSeek(SMGRFile file, int64 offset, int whence);
EXTERNC int yezzey_FileSync(SMGRFile file);
EXTERNC SMGRFile yezzey_AORelOpenSegFile(Oid reloid, char *nspname,
                                         char *relname, FileName fileName,
                                         int fileFlags, int fileMode,
                                         int64 modcount);
EXTERNC int yezzey_FileWrite(SMGRFile file, char *buffer, int amount);
EXTERNC int yezzey_FileRead(SMGRFile file, char *buffer, int amount);
EXTERNC int yezzey_FileTruncate(SMGRFile file, int64 offset);

#endif /* YEZZEY_PROXY_H */