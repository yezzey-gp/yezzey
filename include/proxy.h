#ifndef YEZZEY_PROXY_H
#define YEZZEY_PROXY_H

#include "pg.h"

#include "gucs.h"
#include "storage.h"

#include "ygpver.h"

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

#if IsModernYezzey
typedef File SMGRFile;
#endif

EXTERNC int64 yezzey_NonVirtualCurSeek(SMGRFile file);
EXTERNC void yezzey_FileClose(SMGRFile file);
EXTERNC int64 yezzey_FileSeek(SMGRFile file, int64 offset, int whence);

#if IsModernYezzey
EXTERNC int yezzey_FileSync(SMGRFile file, uint32 wait_event_info);
#else
EXTERNC int yezzey_FileSync(SMGRFile file);
#endif

#if IsModernYezzey
EXTERNC SMGRFile yezzey_AORelOpenSegFile(const char *fileName, int fileFlags);
#else
EXTERNC SMGRFile yezzey_AORelOpenSegFile(Oid reloid, char *nspname,
                                         char *relname, const char *fileName,
                                         int fileFlags, int fileMode,
                                         int64 modcount);
#endif

#if IsModernYezzey
EXTERNC int yezzey_FileWrite(SMGRFile file, char *buffer, int amount,
                             off_t offset, uint32 wait_event_info);
EXTERNC int yezzey_FileRead(SMGRFile file, char *buffer, int amount,
                            off_t offset, uint32 wait_event_info);
#else
EXTERNC int yezzey_FileWrite(SMGRFile file, char *buffer, int amount);
EXTERNC int yezzey_FileRead(SMGRFile file, char *buffer, int amount);
#endif

#if IsModernYezzey
EXTERNC int yezzey_FileTruncate(SMGRFile file, int64 offset,
                                uint32 wait_event_info);
#else
EXTERNC int yezzey_FileTruncate(SMGRFile file, int64 offset);
#endif

#if IsModernYezzey
EXTERNC int yezzey_FileDiskSize(SMGRFile file);
#endif

#endif /* YEZZEY_PROXY_H */