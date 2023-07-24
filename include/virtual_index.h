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

typedef struct {
  int segno;             /* AO relation block file no */
  int64_t start_offset;  /* start_offset of block file chunk */
  int64_t finish_offset; /* finish_offset of block file chunk */
  int64_t modcount;      /* modcount of block file chunk */
  XLogRecPtr lsn;        /* Chunk lsn */
  text x_path;       /* external path */
} FormData_yezzey_virtual_index;

#define Natts_yezzey_virtual_index 6
#define Anum_yezzey_virtual_index_segno 1
#define Anum_yezzey_virtual_start_off 2
#define Anum_yezzey_virtual_finish_off 3
#define Anum_yezzey_virtual_modcount 4
#define Anum_yezzey_virtual_lsn 5
#define Anum_yezzey_virtual_x_path 6

EXTERNC Oid YezzeyCreateAuxIndex(Relation aorel);

EXTERNC Oid YezzeyFindAuxIndex(Oid reloid);

EXTERNC void emptyYezzeyIndex(Oid yezzey_index_oid);

EXTERNC void emptyYezzeyIndexBlkno(Oid yezzey_index_oid, int blkno);

#ifdef __cplusplus
void YezzeyVirtualIndexInsert(Oid yandexoid /*yezzey auxiliary index oid*/,
                              int64_t segindx, int64_t modcount,
                              XLogRecPtr lsn, const char * x_path /* external path */);

std::vector<ChunkInfo>
YezzeyVirtualGetOrder(Oid yandexoid /*yezzey auxiliary index oid*/, int blkno);
#else
#endif
