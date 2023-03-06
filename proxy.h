
#ifdef __cplusplus
extern "C"
{
#endif

#include "postgres.h"
#include "fmgr.h"

#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>

#include "utils/snapmgr.h"
#include "miscadmin.h"

#include "smgr_s3.h"

#include "external_storage.h"

#if PG_VERSION_NUM >= 130000
#include "postmaster/interrupt.h"
#endif

#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "executor/spi.h"
#include "storage/smgr.h"
#include "storage/md.h"
#include "common/relpath.h"
#include "catalog/catalog.h"

#if PG_VERSION_NUM >= 100000
#include "common/file_perm.h"
#else
#include "access/xact.h"
#endif

#include "utils/guc.h"
#include "lib/stringinfo.h"
#include "postmaster/bgworker.h"
#include "storage/proc.h"
#include "storage/latch.h"
#include "pgstat.h"

#include "utils/elog.h"

#ifdef GPBUILD
#include "cdb/cdbvars.h"
#endif

#include "yezzey.h"
#ifdef __cplusplus
}
#endif

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif


EXTERNC int64 yezzey_NonVirtualCurSeek (SMGRFile file);
EXTERNC void yezzey_FileClose(SMGRFile file);
EXTERNC int64 yezzey_FileSeek(SMGRFile file, int64 offset, int whence);
EXTERNC int yezzey_FileSync(SMGRFile file);
EXTERNC SMGRFile yezzey_AORelOpenSegFile(char * nspname, char * relname, FileName fileName, int fileFlags, int fileMode, int64 modcount);
EXTERNC int yezzey_FileWrite(SMGRFile file, char *buffer, int amount);
EXTERNC int yezzey_FileRead(SMGRFile file, char *buffer, int amount);
EXTERNC int yezzey_FileTruncate(SMGRFile file, int offset);