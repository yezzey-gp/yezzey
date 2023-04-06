#ifndef YEZZEY_PROXY_H
#define YEZZEY_PROXY_H

#include "pg.h"

#include "gucs.h"
#include "storage.h"

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