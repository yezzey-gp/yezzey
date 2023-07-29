#pragma once

#include "pg.h"

#ifdef __cplusplus

#include <string>
#include <vector>

#include "chunkinfo.h"

#endif

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

/* ----------------
 *		compiler constants for pg_database
 * ----------------
 */

#define YEZZEY_VIRTUAL_INDEX_RELATION 8602

#define YEZZEY_VIRTUAL_INDEX_RELATION_INDX 8603

#define YEZZEY_CHUNK_STATUS_ACTIVE 1
#define YEZZEY_CHUNK_STATUS_BLOAT 2
#define YEZZEY_CHUNK_STATUS_OBSOLETE 3

typedef struct {
  int segno;             /* AO relation block file no */
  int64_t start_offset;  /* start_offset of block file chunk */
  int64_t finish_offset; /* finish_offset of block file chunk */
  int64_t modcount;      /* modcount of block file chunk */
  XLogRecPtr lsn;        /* Chunk lsn */
  int status;            /* Chunk status (see YEZZEY_CHUNK_STATUS_* above) */
  text x_path;           /* external path */
} FormData_yezzey_virtual_index;

/* we dont need relation oid for chucks index */
#define Natts_yezzey_virtual_index 8
#define Anum_yezzey_virtual_index_blkno 1
#define Anum_yezzey_virtual_index_filenode 2
#define Anum_yezzey_virtual_start_off 3
#define Anum_yezzey_virtual_finish_off 4
#define Anum_yezzey_virtual_modcount 5
#define Anum_yezzey_virtual_lsn 6
#define Anum_yezzey_virtual_status 7
#define Anum_yezzey_virtual_x_path 8

EXTERNC void YezzeyCreateAuxIndex(void);

EXTERNC void emptyYezzeyIndex(Oid relfilenode);

EXTERNC void emptyYezzeyIndexBlkno(int blkno, Oid relfilenode);

#ifdef __cplusplus
void YezzeyVirtualIndexInsert(int64_t blkno, Oid relfilenodeOid,
                              int64_t offset_start, int64_t offset_finish,
                              int64_t modcount, XLogRecPtr lsn,
                              const char *x_path /* external path */);

std::vector<ChunkInfo> YezzeyVirtualGetOrder(int blkno, Oid relfilenode);
#else
#endif
