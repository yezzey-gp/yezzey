
#include "virtual_index.h"

#include <algorithm>

const std::string aux_index_relname = "yezzey_virtual_index";
const std::string aux_index_relname_indx = "yezzey_virtual_indexindx";

void YezzeyCreateAuxIndex(void) {
  ObjectAddress baseobject;
  ObjectAddress yezzey_ao_auxiliaryobject;

  auto tupdesc = CreateTemplateTupleDesc(Natts_yezzey_virtual_index, false);

  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_virtual_index_blkno,
                     "blkno", INT4OID, -1, 0);
  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_virtual_index_filenode,
                     "filenode", OIDOID, -1, 0);
  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_virtual_start_off,
                     "offset_start", INT8OID, -1, 0);
  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_virtual_finish_off,
                     "offset_finish", INT8OID, -1, 0);
  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_virtual_modcount,
                     "modcount", INT8OID, -1, 0);
  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_virtual_lsn, "lsn",
                     LSNOID, -1, 0);

  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_virtual_status, "status",
                     INT4OID, -1, 0);

  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_virtual_x_path, "x_path",
                     TEXTOID, -1, 0);

  (void)heap_create_with_catalog(
      aux_index_relname.c_str() /* relname */,
      YEZZEY_AUX_NAMESPACE /* namespace */, 0 /* tablespace */,
      YEZZEY_VIRTUAL_INDEX_RELATION /* relid */,
      GetNewObjectId() /* reltype oid */, InvalidOid /* reloftypeid */,
      GetUserId() /* owner */, tupdesc /* rel tuple */, NIL,
      InvalidOid /* relam */, RELKIND_RELATION, RELPERSISTENCE_PERMANENT,
      RELSTORAGE_HEAP, false /* is shared */, false /* rel is mapped */, true,
      0, ONCOMMIT_NOOP, NULL /* GP Policy */, (Datum)0,
      false /* use_user_acl */, true, true, false /* valid_opts */,
      false /* is_part_child */, false /* is part parent */);

  /* Make this table visible, else yezzey virtual index creation will fail */
  CommandCounterIncrement();

  // #define YEZZEY_VIRT_INDEX_COLUMNS 2

  //   /* ShareLock is not really needed here, but take it anyway */
  //   auto yezzey_virt_indx_rel =
  //       heap_open(YEZZEY_VIRTUAL_INDEX_RELATION, ShareLock);
  //   char *colnameFirst = "blkno";
  //   char *colnameSecond = "filenode";
  //   auto indexColNames =
  //       list_make2(makeString(colnameFirst), makeString(colnameSecond));

  //   auto indexInfo = makeNode(IndexInfo);

  //   Oid collationObjectId[YEZZEY_VIRT_INDEX_COLUMNS];
  //   Oid classObjectId[YEZZEY_VIRT_INDEX_COLUMNS];
  //   int16 coloptions[YEZZEY_VIRT_INDEX_COLUMNS];

  //   indexInfo->ii_NumIndexAttrs = 2;
  //   indexInfo->ii_KeyAttrNumbers[0] = Anum_yezzey_virtual_index_blkno;
  //   indexInfo->ii_KeyAttrNumbers[1] = Anum_yezzey_virtual_index_filenode;
  //   indexInfo->ii_Expressions = NIL;
  //   indexInfo->ii_ExpressionsState = NIL;
  //   indexInfo->ii_Predicate = NIL;
  //   indexInfo->ii_PredicateState = NIL;
  //   indexInfo->ii_Unique = true;
  //   indexInfo->ii_Concurrent = false;

  //   collationObjectId[0] = InvalidOid;
  //   collationObjectId[1] = InvalidOid;

  //   classObjectId[0] = INT4_BTREE_OPS_OID;
  //   classObjectId[1] = OID_BTREE_OPS_OID;

  //   coloptions[0] = 0;
  //   coloptions[1] = 0;

  //   (void)index_create(yezzey_virt_indx_rel, aux_index_relname_indx.c_str(),
  //                      YEZZEY_VIRTUAL_INDEX_RELATION_INDX, InvalidOid,
  //                      InvalidOid, InvalidOid, indexInfo, indexColNames,
  //                      BTREE_AM_OID, 0 /* tablespace */, collationObjectId,
  //                      classObjectId, coloptions, (Datum)0, true, false,
  //                      false, false, true, false, false, true, NULL);

  //   /* Unlock target table -- no one can see it */
  //   heap_close(yezzey_virt_indx_rel, ShareLock);

  //   /* Unlock the index -- no one can see it anyway */
  //   UnlockRelationOid(YEZZEY_VIRTUAL_INDEX_RELATION_INDX,
  //   AccessExclusiveLock);

  //   CommandCounterIncrement();

  /*
   * Register dependency from the auxiliary table to the master, so that the
   * aoseg table will be deleted if the master is.
   */
  baseobject.classId = ExtensionRelationId;
  baseobject.objectId = get_extension_oid("yezzey", false);
  baseobject.objectSubId = 0;
  yezzey_ao_auxiliaryobject.classId = RelationRelationId;
  yezzey_ao_auxiliaryobject.objectId = YEZZEY_VIRTUAL_INDEX_RELATION;
  yezzey_ao_auxiliaryobject.objectSubId = 0;

  recordDependencyOn(&yezzey_ao_auxiliaryobject, &baseobject,
                     DEPENDENCY_INTERNAL);

  /*
   * Make changes visible
   */
  CommandCounterIncrement();
}

void emptyYezzeyIndex(Oid relfilenode) {
  HeapTuple tuple;

  ScanKeyData skey[1];

  /* DELETE FROM yezzey.yezzey_virtual_index WHERE relfilenode = <oid> */
  auto rel = heap_open(YEZZEY_VIRTUAL_INDEX_RELATION, RowExclusiveLock);

  auto snap = RegisterSnapshot(GetTransactionSnapshot());

  ScanKeyInit(&skey[0], Anum_yezzey_virtual_index_filenode,
              BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relfilenode));

  auto desc = heap_beginscan(rel, snap, 1, skey);

  while (HeapTupleIsValid(tuple = heap_getnext(desc, ForwardScanDirection))) {
    simple_heap_delete(rel, &tuple->t_self);
  }

  heap_endscan(desc);
  heap_close(rel, RowExclusiveLock);

  UnregisterSnapshot(snap);

  /* make changes visible*/
  CommandCounterIncrement();
} /* end emptyYezzeyIndex */

/* Update this settings if where clause expr changes */
#define YezzeyVirtualIndexScanCols 2

void emptyYezzeyIndexBlkno(int blkno, Oid relfilenode) {

  HeapTuple tuple;
  ScanKeyData skey[YezzeyVirtualIndexScanCols];

  /* DELETE FROM yezzey.yezzey_virtual_index_<oid> WHERE segno = <blkno> */
  auto rel = heap_open(YEZZEY_VIRTUAL_INDEX_RELATION, RowExclusiveLock);

  auto snap = RegisterSnapshot(GetTransactionSnapshot());

  ScanKeyInit(&skey[0], Anum_yezzey_virtual_index_blkno, BTEqualStrategyNumber,
              F_INT4EQ, Int32GetDatum(blkno));

  ScanKeyInit(&skey[1], Anum_yezzey_virtual_index_filenode,
              BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relfilenode));

  auto desc = heap_beginscan(rel, snap, YezzeyVirtualIndexScanCols, skey);

  while (HeapTupleIsValid(tuple = heap_getnext(desc, ForwardScanDirection))) {
    simple_heap_delete(rel, &tuple->t_self);
  }

  heap_endscan(desc);
  heap_close(rel, RowExclusiveLock);

  UnregisterSnapshot(snap);

  /* make changes visible*/
  CommandCounterIncrement();
} /* end emptyYezzeyIndexBlkno */

void YezzeyVirtualIndexInsert(int64_t blkno, Oid relfilenodeOid,
                              int64_t offset_start, int64_t offset_finish,
                              int64_t modcount, XLogRecPtr lsn,
                              const char *x_path /* external path */) {
  bool nulls[Natts_yezzey_virtual_index];
  Datum values[Natts_yezzey_virtual_index];

  memset(nulls, 0, sizeof(nulls));
  memset(values, 0, sizeof(values));

  /* INSERT INTO  yezzey.yezzey_virtual_index VALUES(segno, start_offset,
   * 0, modcount, external_path) */

  auto yandxrel = heap_open(YEZZEY_VIRTUAL_INDEX_RELATION, RowExclusiveLock);

  values[Anum_yezzey_virtual_index_blkno - 1] = Int64GetDatum(blkno);
  values[Anum_yezzey_virtual_index_filenode - 1] =
      ObjectIdGetDatum(relfilenodeOid);
  values[Anum_yezzey_virtual_start_off - 1] = Int64GetDatum(offset_start);
  values[Anum_yezzey_virtual_finish_off - 1] = Int64GetDatum(offset_finish);
  values[Anum_yezzey_virtual_modcount - 1] = Int64GetDatum(modcount);
  values[Anum_yezzey_virtual_lsn - 1] = LSNGetDatum(lsn);
  values[Anum_yezzey_virtual_status - 1] =
      Int32GetDatum(YEZZEY_CHUNK_STATUS_ACTIVE);
  values[Anum_yezzey_virtual_x_path - 1] =
      PointerGetDatum(cstring_to_text(x_path));

  auto yandxtuple = heap_form_tuple(RelationGetDescr(yandxrel), values, nulls);

  simple_heap_insert(yandxrel, yandxtuple);
  CatalogUpdateIndexes(yandxrel, yandxtuple);

  heap_freetuple(yandxtuple);
  heap_close(yandxrel, RowExclusiveLock);

  CommandCounterIncrement();
}

std::vector<ChunkInfo> YezzeyVirtualGetOrder(int blkno, Oid relfilenode) {

  /* SELECT external_path
   * FROM yezzey.yezzey_virtual_index_<oid>
   * WHERE segno = .. and filenode = ..
   * <>; */
  HeapTuple tuple;

  ScanKeyData skey[YezzeyVirtualIndexScanCols];

  std::vector<ChunkInfo> res;

  auto rel = heap_open(YEZZEY_VIRTUAL_INDEX_RELATION, RowExclusiveLock);

  auto snap = RegisterSnapshot(GetTransactionSnapshot());

  ScanKeyInit(&skey[0], Anum_yezzey_virtual_index_blkno, BTEqualStrategyNumber,
              F_INT4EQ, Int32GetDatum(blkno));

  ScanKeyInit(&skey[1], Anum_yezzey_virtual_index_filenode,
              BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relfilenode));

  /* TBD: Read index */
  auto desc = heap_beginscan(rel, snap, YezzeyVirtualIndexScanCols, skey);

  while (HeapTupleIsValid(tuple = heap_getnext(desc, ForwardScanDirection))) {
    auto ytup = ((FormData_yezzey_virtual_index *)GETSTRUCT(tuple));
    // unpack text to str
    res.push_back(
        ChunkInfo(ytup->lsn, ytup->modcount, text_to_cstring(&ytup->x_path)));
  }

  heap_endscan(desc);
  heap_close(rel, RowExclusiveLock);

  UnregisterSnapshot(snap);

  /* make changes visible*/
  CommandCounterIncrement();

  /* sort by modcount - they are unic */
  std::sort(res.begin(), res.end(),
            [](const ChunkInfo &lhs, const ChunkInfo &rhs) {
              return lhs.modcount < rhs.modcount;
            });

  return std::move(res);
}