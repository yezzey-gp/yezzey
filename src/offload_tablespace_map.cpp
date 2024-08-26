/*
 * file: src/offload_tablespace_map.cpp
 */

#include "pg.h"
#include "offload_policy.h"
#include "offload.h"
#include "yezzey_heap_api.h"
#include <unistd.h>

const std::string offload_metadata_relname = "offload_tablespace_map";

std::string YezzeyGetRelationOriginTablespace(Oid i_reloid) {
  /**/
  ScanKeyData skey[2];

  auto snap = RegisterSnapshot(GetTransactionSnapshot());

  auto classrel =
      yezzey_relation_open(RelationRelationId, RowExclusiveLock);

  ScanKeyInit(&skey[0], Anum_pg_class_relname, BTEqualStrategyNumber,
              F_TEXTEQ, CStringGetDatum(offload_metadata_relname.c_str()));


  ScanKeyInit(&skey[1], Anum_pg_class_relnamespace, BTEqualStrategyNumber,
              F_OIDEQ, ObjectIdGetDatum(YEZZEY_AUX_NAMESPACE));

  auto scan = yezzey_beginscan(classrel, snap, 2, skey);

  auto oldtuple = heap_getnext(scan, ForwardScanDirection);

  /* No map relation created. Assume 'BASE' by default */
  if (!HeapTupleIsValid(oldtuple)) {
      heap_close(classrel, RowExclusiveLock);

      yezzey_endscan(scan);
      UnregisterSnapshot(snap);
      return "BASE";
  }

  Oid yezzey_tablespace_map_oid = HeapTupleGetOid(oldtuple);

  heap_close(classrel, RowExclusiveLock);
  yezzey_endscan(scan);

  /* SELECT FROM yezzey.offload_tablespace_map WHERE reloid = i_reloid; */
  auto offload_tablespace_map_rel =
      yezzey_relation_open(yezzey_tablespace_map_oid, RowExclusiveLock);


  ScanKeyData offskey[1];

  ScanKeyInit(&offskey[0], Anum_pg_class_relname, BTEqualStrategyNumber,
              F_OIDEQ, ObjectIdGetDatum(i_reloid));

  auto scanoff = yezzey_beginscan(offload_tablespace_map_rel, snap, 1, offskey);
  auto offtuple = heap_getnext(scanoff, ForwardScanDirection);
  /* No map tuple created. Assume 'BASE' by default */
  if (!HeapTupleIsValid(offtuple)) {
      heap_close(offload_tablespace_map_rel, RowExclusiveLock);

      yezzey_endscan(scanoff);
      UnregisterSnapshot(snap);
      elog(ERROR, "failed to map relation %d to its origin tablespace", i_reloid);
  }

  auto rv = ((Form_offload_tablespace_map) GETSTRUCT(offtuple))->origin_tablespace_name;

  auto tablespace_val = std::string(text_to_cstring(&rv));

  heap_close(offload_tablespace_map_rel, RowExclusiveLock);

  yezzey_endscan(scanoff);
  UnregisterSnapshot(snap);

  return tablespace_val;
}

