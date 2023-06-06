#pragma once

#include "pg.h"

#ifdef __cplusplus

#include <string>
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
#define Natts_yezzey_virtual_index 5
#define Anum_yezzey_virtual_index_segno 1
#define Anum_yezzey_virtual_start_off 2
#define Anum_yezzey_virtual_finish_off 3
#define Anum_yezzey_virtual_modcount 4
#define Anum_yezzey_virtual_ext_path 5

EXTERNC Oid YezzeyCreateAuxIndex(Relation aorel);

EXTERNC Oid YezzeyFindAuxIndex(Oid reloid);

EXTERNC void emptyYezzeyIndex(Oid yezzey_index_oid);

EXTERNC void emptyYezzeyIndexBlkno(Oid yezzey_index_oid, int blkno);

#ifdef __cplusplus
void YezzeyVirtualIndexInsert(Oid yandexoid /*yezzey auxiliary index oid*/,
                              int64_t segindx, int64_t modcount,
                              const std::string &ext_path);
#else
#endif
