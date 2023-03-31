
#include "virtual_index.h"



Oid YezzeyFindAuxIndex_internal(Oid reloid);

Oid YezzeyCreateAuxIndex(Relation aorel) {
  {
    auto tmp = YezzeyFindAuxIndex_internal(RelationGetRelid(aorel));
    if (OidIsValid(tmp)) {
      return tmp;
    }
  }

  Oid yezzey_ao_auxiliary_relid;
  char yezzey_ao_auxiliary_relname[NAMEDATALEN];
  TupleDesc tupdesc;

  ObjectAddress baseobject;
  ObjectAddress yezzey_ao_auxiliaryobject;

  tupdesc = CreateTemplateTupleDesc(Natts_yezzey_virtual_index, false);

  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_virtual_index, "segno",
                     INT4OID, -1, 0);
  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_virtual_start_off,
                     "offset_start", INT8OID, -1, 0);
  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_virtual_finish_off,
                     "offset_finish", INT8OID, -1, 0);
  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_virtual_modcount,
                     "modcount", INT8OID, -1, 0);
  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_virtual_ext_path,
                     "external_path", TEXTOID, -1, 0);

  snprintf(yezzey_ao_auxiliary_relname, sizeof(yezzey_ao_auxiliary_relname),
           "%s_%u", "yezzey_virtual_index", RelationGetRelid(aorel));

  yezzey_ao_auxiliary_relid = heap_create_with_catalog(
      yezzey_ao_auxiliary_relname /* relname */,
      YEZZEY_AUX_NAMESPACE /* namespace */, 0 /* tablespace */,
      GetNewObjectId() /* relid */, GetNewObjectId() /* reltype oid */,
      InvalidOid /* reloftypeid */, aorel->rd_rel->relowner /* owner */,
      tupdesc /* rel tuple */, NIL, InvalidOid /* relam */, RELKIND_YEZZEYINDEX,
      aorel->rd_rel->relpersistence, RELSTORAGE_HEAP,
      aorel->rd_rel->relisshared, RelationIsMapped(aorel), true, 0,
      ONCOMMIT_NOOP, NULL /* GP Policy */, (Datum)0, false /* use_user_acl */,
      true, true, false /* valid_opts */, false /* is_part_child */,
      false /* is part parent */);

  /* Make this table visible, else yezzey virtual index creation will fail */
  CommandCounterIncrement();

  /*
   * Register dependency from the auxiliary table to the master, so that the
   * aoseg table will be deleted if the master is.
   */
  baseobject.classId = RelationRelationId;
  baseobject.objectId = RelationGetRelid(aorel);
  baseobject.objectSubId = 0;
  yezzey_ao_auxiliaryobject.classId = RelationRelationId;
  yezzey_ao_auxiliaryobject.objectId = yezzey_ao_auxiliary_relid;
  yezzey_ao_auxiliaryobject.objectSubId = 0;

  recordDependencyOn(&yezzey_ao_auxiliaryobject, &baseobject,
                     DEPENDENCY_INTERNAL);

  /*
   * Make changes visible
   */
  CommandCounterIncrement();

  return yezzey_ao_auxiliary_relid;
}

Oid YezzeyFindAuxIndex_internal(Oid reloid) {
  HeapTuple tup;
  Oid operatorObjectId;
  char yezzey_ao_auxiliary_relname[NAMEDATALEN];
  SysScanDesc scan;
  ScanKeyData skey[2];
  Relation pg_class;
  Oid yezzey_virtual_index_oid;

  yezzey_virtual_index_oid = InvalidOid;

  snprintf(yezzey_ao_auxiliary_relname, sizeof(yezzey_ao_auxiliary_relname),
           "%s_%u", "yezzey_virtual_index", reloid);

  /*
   * Check the pg_appendonly relation to be certain the ao table
   * is there.
   */
  pg_class = heap_open(RelationRelationId, AccessShareLock);

  ScanKeyInit(&skey[0], Anum_pg_class_relname, BTEqualStrategyNumber, F_NAMEEQ,
              CStringGetDatum(yezzey_ao_auxiliary_relname));

  ScanKeyInit(&skey[1], Anum_pg_class_relnamespace, BTEqualStrategyNumber,
              F_OIDEQ, ObjectIdGetDatum(YEZZEY_AUX_NAMESPACE));

  scan = systable_beginscan(pg_class, ClassNameNspIndexId, true, NULL, 2, skey);

  if (HeapTupleIsValid(tup = systable_getnext(scan))) {
    yezzey_virtual_index_oid = HeapTupleGetOid(tup);
  }

  systable_endscan(scan);
  heap_close(pg_class, AccessShareLock);

  return yezzey_virtual_index_oid;
}

Oid YezzeyFindAuxIndex(Oid reloid) {
  Oid yezzey_virtual_index_oid = YezzeyFindAuxIndex_internal(reloid);
  if (OidIsValid(yezzey_virtual_index_oid)) {
    return yezzey_virtual_index_oid;
  } else {
    elog(ERROR, "could not find yezzey virtual index oid for relation \"%d\"",
         reloid);
  }
}

void emptyYezzeyIndex(Oid yezzey_index_oid) {
  HeapTuple tuple;
  HeapScanDesc desc;
  Relation rel;

  /* DELETE FROM yezzey.yezzey_virtual_index_<oid> */
  rel = heap_open(yezzey_index_oid, RowExclusiveLock);

  auto snap = RegisterSnapshot(GetTransactionSnapshot());

  desc = heap_beginscan(rel, snap, 0, NULL);

  while (HeapTupleIsValid(tuple = heap_getnext(desc, ForwardScanDirection))) {
    simple_heap_delete(rel, &tuple->t_self);
  }

  heap_endscan(desc);
  heap_close(rel, RowExclusiveLock);

  UnregisterSnapshot(snap);

  /* make changes visible*/
  CommandCounterIncrement();
} /* end emptyYezzeyIndex */

void YezzeyVirtualIndexInsert(Oid yandexoid /*yezzey auxiliary index oid*/,
                              int64_t segindx, int64_t modcount,
                              const std::string &ext_path) {
  HeapTuple yandxtuple;
  Relation yandxrel;

  bool nulls[Natts_yezzey_virtual_index];
  Datum values[Natts_yezzey_virtual_index];

  memset(nulls, 0, sizeof(nulls));
  memset(values, 0, sizeof(values));

  /* INSERT INTO  yezzey.yezzey_virtual_index_<oid> VALUES(segno, start_offset,
   * 0, modcount, external_path) */

  yandxrel = heap_open(yandexoid, RowExclusiveLock);

  values[Anum_yezzey_virtual_index - 1] = segindx;
  values[Anum_yezzey_virtual_start_off - 1] = Int64GetDatum(0);
  values[Anum_yezzey_virtual_finish_off - 1] = Int64GetDatum(0);
  values[Anum_yezzey_virtual_modcount - 1] = Int64GetDatum(modcount);
  values[Anum_yezzey_virtual_ext_path - 1] =
      CStringGetTextDatum(ext_path.c_str());

  yandxtuple = heap_form_tuple(RelationGetDescr(yandxrel), values, nulls);

  simple_heap_insert(yandxrel, yandxtuple);
  CatalogUpdateIndexes(yandxrel, yandxtuple);

  heap_freetuple(yandxtuple);
  heap_close(yandxrel, RowExclusiveLock);

  CommandCounterIncrement();
}

/*
static void
write_yezzey_index(Relation aorel, int32 segindex, int32 start_offset, int32
modcount, const char *path)
{
        HeapTuple	tuple;
        MemoryContext oldcxt;
        Datum	   *values = blockDirectory->values;
        bool	   *nulls = blockDirectory->nulls;
        TupleDesc	heapTupleDesc = RelationGetDescr(aorel);

        Assert(minipageInfo->numMinipageEntries > 0);

        oldcxt = MemoryContextSwitchTo(blockDirectory->memoryContext);

        Assert(blkdirRel != NULL);

        values[Anum_pg_aoblkdir_segno - 1] =
                Int32GetDatum(blockDirectory->currentSegmentFileNum);
        nulls[Anum_pg_aoblkdir_segno - 1] = false;

        values[Anum_pg_aoblkdir_columngroupno - 1] =
                Int32GetDatum(columnGroupNo);
        nulls[Anum_pg_aoblkdir_columngroupno - 1] = false;

        values[Anum_pg_aoblkdir_firstrownum - 1] =
                Int64GetDatum(minipageInfo->minipage->entry[0].firstRowNum);
        nulls[Anum_pg_aoblkdir_firstrownum - 1] = false;

        SET_VARSIZE(minipageInfo->minipage,
                                minipage_size(minipageInfo->numMinipageEntries));
        minipageInfo->minipage->nEntry = minipageInfo->numMinipageEntries;
        values[Anum_pg_aoblkdir_minipage - 1] =
                PointerGetDatum(minipageInfo->minipage);
        nulls[Anum_pg_aoblkdir_minipage - 1] = false;

        tuple = heaptuple_form_to(heapTupleDesc,
                                                          values,
                                                          nulls,
                                                          NULL,
                                                          NULL);

        *
         * Write out the minipage to the block directory relation. If this
         * minipage is already in the relation, we update the row. Otherwise, a
         * new row is inserted.
         *
        if (ItemPointerIsValid(&minipageInfo->tupleTid))
        {
                ereportif(Debug_appendonly_print_blockdirectory, LOG,
                                  (errmsg("Append-only block directory update a
minipage: "
                                                  "(segno, columnGroupNo,
nEntries, firstRowNum) = "
                                                  "(%d, %d, %u, " INT64_FORMAT
")", blockDirectory->currentSegmentFileNum, columnGroupNo,
minipageInfo->numMinipageEntries,
                                                  minipageInfo->minipage->entry[0].firstRowNum)));

                simple_heap_update(blkdirRel, &minipageInfo->tupleTid, tuple);
        }
        else
        {
                ereportif(Debug_appendonly_print_blockdirectory, LOG,
                                  (errmsg("Append-only block directory insert a
minipage: "
                                                  "(segno, columnGroupNo,
nEntries, firstRowNum) = "
                                                  "(%d, %d, %u, " INT64_FORMAT
")", blockDirectory->currentSegmentFileNum, columnGroupNo,
minipageInfo->numMinipageEntries,
                                                  minipageInfo->minipage->entry[0].firstRowNum)));

                simple_heap_insert(blkdirRel, tuple);
        }

        CatalogUpdateIndexes(blkdirRel, tuple);

        heap_freetuple(tuple);

        MemoryContextSwitchTo(oldcxt);
}
*/
