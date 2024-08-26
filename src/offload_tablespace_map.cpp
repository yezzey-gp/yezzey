/*
 * file: src/offload_tablespace_map.cpp
 */

#include "pg.h"
#include "offload_policy.h"
#include "offload.h"
#include "yezzey_heap_api.h"
#include <unistd.h>

#include "offload_tablespace_map.h"

const std::string offload_metadata_relname = "offload_tablespace_map";


static Oid YezzeyResolveTablespaceMapOid() {
  auto snap = RegisterSnapshot(GetTransactionSnapshot());
  /**/
  ScanKeyData skey[2];

  auto classrel =
      yezzey_relation_open(RelationRelationId, RowExclusiveLock);

  ScanKeyInit(&skey[0], Anum_pg_class_relname, BTEqualStrategyNumber,
              F_TEXTEQ, PointerGetDatum(cstring_to_text(offload_metadata_relname.c_str())));


  ScanKeyInit(&skey[1], Anum_pg_class_relnamespace, BTEqualStrategyNumber,
              F_OIDEQ, ObjectIdGetDatum(YEZZEY_AUX_NAMESPACE));

  auto scan = yezzey_beginscan(classrel, snap, 2, skey);

  auto oldtuple = heap_getnext(scan, ForwardScanDirection);

  /* No map relation created. return invalid oid */
  if (!HeapTupleIsValid(oldtuple)) {
      heap_close(classrel, RowExclusiveLock);
      yezzey_endscan(scan);
      UnregisterSnapshot(snap);
      return InvalidOid;
  }

  Oid yezzey_tablespace_map_oid = HeapTupleGetOid(oldtuple);

  heap_close(classrel, RowExclusiveLock);
  yezzey_endscan(scan);
  UnregisterSnapshot(snap);

  return yezzey_tablespace_map_oid;
}

std::string YezzeyGetRelationOriginTablespace(Oid i_reloid) {
  auto yezzey_tablespace_map_oid = YezzeyResolveTablespaceMapOid();

  /* No map relation created. Assume pg_default by default */
  if (yezzey_tablespace_map_oid == InvalidOid) {
      return "pg_default";
  }

  auto snap = RegisterSnapshot(GetTransactionSnapshot());

  /* SELECT FROM yezzey.offload_tablespace_map WHERE reloid = i_reloid; */
  auto offload_tablespace_map_rel =
      yezzey_relation_open(yezzey_tablespace_map_oid, RowExclusiveLock);

  ScanKeyData offskey[1];

  ScanKeyInit(&offskey[0], Anum_pg_class_relname, BTEqualStrategyNumber,
              F_OIDEQ, ObjectIdGetDatum(i_reloid));

  auto scanoff = yezzey_beginscan(offload_tablespace_map_rel, snap, 1, offskey);
  auto offtuple = heap_getnext(scanoff, ForwardScanDirection);
  /* No map tuple created. Assume 'pg_default' by default */
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


void YezzeyRegisterRelationOriginTablespace(Oid i_reloid, Oid i_reltablespace) {

  auto yezzey_tablespace_map_oid = YezzeyResolveTablespaceMapOid();

  if (yezzey_tablespace_map_oid == InvalidOid) {
      /* no map relation created, NOOP */
      return;
  }

  auto snap = RegisterSnapshot(GetTransactionSnapshot());

  bool nulls[Natts_offload_tablespace_map];
  Datum values[Natts_offload_tablespace_map];

  memset(nulls, 0, sizeof(nulls));
  memset(values, 0, sizeof(values));

  /* SELECT FROM yezzey.offload_tablespace_map WHERE reloid = i_reloid; */
  auto offload_tablespace_map_rel =
      yezzey_relation_open(yezzey_tablespace_map_oid, RowExclusiveLock);

  ScanKeyData offskey[1];

  ScanKeyInit(&offskey[0], Anum_pg_class_relname, BTEqualStrategyNumber,
              F_OIDEQ, ObjectIdGetDatum(i_reloid));

  auto scanoff = yezzey_beginscan(offload_tablespace_map_rel, snap, 1, offskey);
  auto offtuple = heap_getnext(scanoff, ForwardScanDirection);
  /* No map tuple created. Assume 'pg_default' by default */
  if (!HeapTupleIsValid(offtuple)) {
      heap_close(offload_tablespace_map_rel, RowExclusiveLock);

      yezzey_endscan(scanoff);
      UnregisterSnapshot(snap);
      elog(ERROR, "failed to map relation %d to its origin tablespace: mapping already exists", i_reloid);
  }


  /* Search syscache for pg_tablespace */
  auto spctuple = SearchSysCache1(TABLESPACEOID, ObjectIdGetDatum(i_reltablespace));
  if (!HeapTupleIsValid(spctuple))
    ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("tablespace with OID %u does not exist", i_reltablespace)));

  auto spcname = ((Form_pg_tablespace) GETSTRUCT(spctuple))->spcname.data;
 
  values[Anum_offload_tablespace_map_reloid - 1] = ObjectIdGetDatum(i_reloid);
  values[Anum_offload_tablespace_map_origin_tablespace_name - 1] = CStringGetTextDatum(spcname);

  
  auto nofftuple = heap_form_tuple(RelationGetDescr(offload_tablespace_map_rel), values, nulls);

  simple_heap_insert(offload_tablespace_map_rel, nofftuple);

  heap_freetuple(nofftuple);

  ReleaseSysCache(spctuple);

  UnregisterSnapshot(snap);
}