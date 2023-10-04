/*
 *
 * file: src/yezzey_expire.cpp
 */

#include "yezzey_expire.h"
#include "offload_policy.h"
#include "string"
#include "url.h"
#include "util.h"
#include "yezzey_heap_api.h"

void YezzeyRecordRelationExpireLsn(Relation rel) {
  if (Gp_role != GP_ROLE_EXECUTE) {
    return;
  }
  /* check that relation is yezzey-related. */
  if (rel->rd_node.spcNode != YEZZEYTABLESPACE_OID) {
    /* noop */
    return;
  }

  auto tp = SearchSysCache1(NAMESPACEOID,
                            ObjectIdGetDatum(RelationGetNamespace(rel)));

  if (!HeapTupleIsValid(tp)) {
    elog(ERROR, "yezzey: failed to get namescape name of relation %d",
         RelationGetNamespace(rel));
  }

  Form_pg_namespace nsptup = (Form_pg_namespace)GETSTRUCT(tp);
  auto namespaceName = std::string(nsptup->nspname.data);
  auto md = yezzey_fqrelname_md5(namespaceName, RelationGetRelationName(rel));

  ReleaseSysCache(tp);

  /*
   * It it important that we drop all chunks related to
   * relation filenode, not oid, since owner of particular
   * relifilenode may may with time thought operations
   * like vacuum, expand (the main case) etc...
   */
  /* clear yezzey index. */
  emptyYezzeyIndex(YezzeyFindAuxIndex(RelationGetRelid(rel)),
                   rel->rd_node.relNode);

#define YezzeySetExpireIndexCols 1
  ScanKeyData skey[YezzeySetExpireIndexCols];

  bool nulls[Natts_yezzey_expire_index];
  Datum values[Natts_yezzey_expire_index];

  memset(nulls, 0, sizeof(nulls));
  memset(values, 0, sizeof(values));

  /* upsert yezzey expire index */
  auto yexprel = heap_open(YEZZEY_EXPIRE_INDEX_RELATION, RowExclusiveLock);

  auto reloid = RelationGetRelid(rel);
  auto relfileoid = rel->rd_node.relNode;

  auto lsn = yezzeyGetXStorageInsertLsn();

  ScanKeyInit(&skey[0], Anum_yezzey_expire_index_reloid, BTEqualStrategyNumber,
              F_OIDEQ, ObjectIdGetDatum(reloid));

  auto snap = RegisterSnapshot(GetTransactionSnapshot());

  auto desc = yezzey_beginscan(yexprel, snap, YezzeySetExpireIndexCols, skey);

  HeapTuple tuple;

  values[Anum_yezzey_expire_index_reloid - 1] = Int64GetDatum(reloid);

  values[Anum_yezzey_expire_index_lsn - 1] = LSNGetDatum(lsn);

  while (HeapTupleIsValid(tuple = heap_getnext(desc, ForwardScanDirection))) {
    // update
    auto meta = (Form_yezzey_expire_index)GETSTRUCT(tuple);

    values[Anum_yezzey_expire_index_relfileoid - 1] =
        ObjectIdGetDatum(meta->yrelfileoid);

    values[Anum_yezzey_expire_index_last_use_lsn - 1] =
        LSNGetDatum(meta->last_use_lsn);

    values[Anum_yezzey_expire_index_fqnmd5 - 1] =
        PointerGetDatum(cstring_to_text(md.c_str()));
    ;

    auto yandxtuple = heap_form_tuple(RelationGetDescr(yexprel), values, nulls);

#if GP_VERSION_NUM < 70000
    simple_heap_update(yexprel, &tuple->t_self, yandxtuple);
    CatalogUpdateIndexes(yexprel, yandxtuple);
#else
    CatalogTupleUpdate(yexprel, &yandxtuple->t_self, yandxtuple);
#endif

    heap_freetuple(yandxtuple);
  }

  yezzey_endscan(desc);
  heap_close(yexprel, RowExclusiveLock);

  UnregisterSnapshot(snap);

  /* make changes visible*/
  CommandCounterIncrement();
} // YezzeyRecordRelationExpireLsn

const std::string yezzey_expire_index_relname = "yezzey_expire_index";
const std::string yezzey_expire_index_indx_relname = "yezzey_expire_index_indx";

void YezzeyCreateRelationExpireIndex(void) {
  { /* check existed, if no, return */
  }
  TupleDesc tupdesc;

  ObjectAddress baseobject;
  ObjectAddress yezzey_ao_auxiliaryobject;

#if GP_VERSION_NUM < 70000
  tupdesc = CreateTemplateTupleDesc(Natts_yezzey_expire_index, false);
#else
  tupdesc = CreateTemplateTupleDesc(Natts_yezzey_expire_index);
#endif

  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_expire_index_reloid,
                     "reloid", OIDOID, -1, 0);

  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_expire_index_relfileoid,
                     "relfileoid", OIDOID, -1, 0);

  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_expire_index_fqnmd5,
                     "fqnmd5", TEXTOID, -1, 0);

  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_expire_index_last_use_lsn,
                     "last_use_lsn", LSNOID, -1, 0);

  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_expire_index_lsn,
                     "expire_lsn", LSNOID, -1, 0);

#if GP_VERSION_NUM < 70000
  (void)heap_create_with_catalog(
      yezzey_expire_index_relname.c_str() /* relname */,
      YEZZEY_AUX_NAMESPACE /* namespace */, 0 /* tablespace */,
      YEZZEY_EXPIRE_INDEX_RELATION /* relid */,
      GetNewObjectId() /* reltype oid */, InvalidOid /* reloftypeid */,
      GetUserId() /* owner */, tupdesc /* rel tuple */, NIL,
      InvalidOid /* relam */, RELKIND_RELATION, RELPERSISTENCE_PERMANENT,
      RELSTORAGE_HEAP, false, false, true, 0, ONCOMMIT_NOOP,
      NULL /* GP Policy */, (Datum)0, false /* use_user_acl */, true, true,
      false /* valid_opts */, false /* is_part_child */,
      false /* is part parent */);

#else
  (void)heap_create_with_catalog(
      yezzey_expire_index_relname.c_str() /* relname */,
      YEZZEY_AUX_NAMESPACE /* namespace */, 0 /* tablespace */,
      YEZZEY_EXPIRE_INDEX_RELATION /* relid */,
      GetNewObjectId() /* reltype oid */, InvalidOid /* reloftypeid */,
      GetUserId() /* owner */, HEAP_TABLE_AM_OID /* access method*/,
      tupdesc /* rel tuple */, NIL, RELKIND_RELATION /*relkind*/,
      RELPERSISTENCE_PERMANENT, false /*shared*/, false /*mapped*/,
      ONCOMMIT_NOOP, NULL /* GP Policy */, (Datum)0, false /* use_user_acl */,
      true, true, InvalidOid /*relrewrite*/, NULL, false /* valid_opts */);
#endif

  /* Make this table visible, else yezzey virtual index creation will fail */
  CommandCounterIncrement();

  /* ShareLock is not really needed here, but take it anyway */
  auto yezzey_rel = heap_open(YEZZEY_EXPIRE_INDEX_RELATION, ShareLock);
  char *col1 = "reloid";
  char *col2 = "relfileoid";
  auto indexColNames = list_make2(col1, col2);

  auto indexInfo = makeNode(IndexInfo);

  Oid collationObjectId[2];
  Oid classObjectId[2];
  int16 coloptions[2];

  indexInfo->ii_NumIndexAttrs = 2;
#if GP_VERSION_NUM < 70000
  indexInfo->ii_KeyAttrNumbers[0] = Anum_yezzey_expire_index_reloid;
  indexInfo->ii_KeyAttrNumbers[1] = Anum_yezzey_expire_index_relfileoid;
#else
  indexInfo->ii_IndexAttrNumbers[0] = Anum_yezzey_expire_index_reloid;
  indexInfo->ii_IndexAttrNumbers[1] = Anum_yezzey_expire_index_relfileoid;
  indexInfo->ii_NumIndexKeyAttrs = indexInfo->ii_NumIndexAttrs;
#endif

  indexInfo->ii_Expressions = NIL;
  indexInfo->ii_ExpressionsState = NIL;
  indexInfo->ii_Predicate = NIL;
#if GP_VERSION_NUM < 70000
  indexInfo->ii_PredicateState = NIL;
#else
  indexInfo->ii_PredicateState = NULL;
#endif
  indexInfo->ii_ExclusionOps = NULL;
  indexInfo->ii_ExclusionProcs = NULL;
  indexInfo->ii_ExclusionStrats = NULL;
  indexInfo->ii_Unique = true;
  indexInfo->ii_ReadyForInserts = true;
  indexInfo->ii_Concurrent = false;
  indexInfo->ii_BrokenHotChain = false;

  collationObjectId[0] = InvalidOid;
  classObjectId[0] = OID_BTREE_OPS_OID;
  coloptions[0] = 0;

  collationObjectId[1] = InvalidOid;
  classObjectId[1] = OID_BTREE_OPS_OID;
  coloptions[1] = 0;

#if GP_VERSION_NUM < 70000
  (void)index_create(yezzey_rel, yezzey_expire_index_indx_relname.c_str(),
                     YEZZEY_EXPIRE_INDEX_RELATION_INDX, InvalidOid, InvalidOid,
                     InvalidOid, indexInfo, indexColNames, BTREE_AM_OID,
                     0 /* tablespace */, collationObjectId, classObjectId,
                     coloptions, (Datum)0, true, false, false, false, true,
                     false, false, true, NULL);
#else

  bits16 flags, constr_flags;
  flags = constr_flags = 0;
  (void)index_create(yezzey_rel, yezzey_expire_index_indx_relname.c_str(),
                     YEZZEY_EXPIRE_INDEX_RELATION_INDX, InvalidOid, InvalidOid,
                     InvalidOid, indexInfo, indexColNames, BTREE_AM_OID,
                     0 /* tablespace */, collationObjectId, classObjectId,
                     coloptions, (Datum)0, flags, constr_flags, true, true,
                     NULL);
#endif

  /* Unlock target table -- no one can see it */
  heap_close(yezzey_rel, ShareLock);

  /* Unlock the index -- no one can see it anyway */
  UnlockRelationOid(YEZZEY_EXPIRE_INDEX_RELATION_INDX, AccessExclusiveLock);

  CommandCounterIncrement();

  /*
   * Register dependency from the auxiliary table to the master, so that the
   * aoseg table will be deleted if the master is.
   */
  baseobject.classId = ExtensionRelationId;
  baseobject.objectId = get_extension_oid("yezzey", false);
  baseobject.objectSubId = 0;
  yezzey_ao_auxiliaryobject.classId = RelationRelationId;
  yezzey_ao_auxiliaryobject.objectId = YEZZEY_EXPIRE_INDEX_RELATION;
  yezzey_ao_auxiliaryobject.objectSubId = 0;

  recordDependencyOn(&yezzey_ao_auxiliaryobject, &baseobject,
                     DEPENDENCY_INTERNAL);

  /*
   * Make changes visible
   */
  CommandCounterIncrement();
}

#define YezzeyExpireIndexCols 3

void YezzeyUpsertLastUseLsn(Oid reloid, Oid relfileoid, const char *md5,
                            XLogRecPtr lsn) {

  ScanKeyData skey[YezzeyExpireIndexCols];

  bool nulls[Natts_yezzey_expire_index];
  Datum values[Natts_yezzey_expire_index];

  memset(nulls, 0, sizeof(nulls));
  memset(values, 0, sizeof(values));

  /* upsert yezzey expire index */
  auto rel = heap_open(YEZZEY_EXPIRE_INDEX_RELATION, RowExclusiveLock);

  ScanKeyInit(&skey[0], Anum_yezzey_expire_index_reloid, BTEqualStrategyNumber,
              F_OIDEQ, ObjectIdGetDatum(reloid));

  ScanKeyInit(&skey[1], Anum_yezzey_expire_index_relfileoid,
              BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relfileoid));

  ScanKeyInit(&skey[2], Anum_yezzey_expire_index_fqnmd5, BTEqualStrategyNumber,
              F_TEXTEQ, PointerGetDatum(cstring_to_text(md5)));

  auto snap = RegisterSnapshot(GetTransactionSnapshot());

  auto desc = yezzey_beginscan(rel, snap, YezzeyExpireIndexCols, skey);

  auto tuple = heap_getnext(desc, ForwardScanDirection);

  values[Anum_yezzey_expire_index_reloid - 1] = Int64GetDatum(reloid);
  values[Anum_yezzey_expire_index_relfileoid - 1] =
      ObjectIdGetDatum(relfileoid);

  values[Anum_yezzey_expire_index_last_use_lsn - 1] = LSNGetDatum(lsn);
  values[Anum_yezzey_expire_index_lsn - 1] = LSNGetDatum(0);
  values[Anum_yezzey_expire_index_fqnmd5 - 1] =
      PointerGetDatum(cstring_to_text(md5));

  if (HeapTupleIsValid(tuple)) {
    // update
    auto meta = (Form_yezzey_expire_index)GETSTRUCT(tuple);

    values[Anum_yezzey_expire_index_lsn - 1] = Int32GetDatum(meta->expire_lsn);

    auto yandxtuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);

#if GP_VERSION_NUM < 70000
    simple_heap_update(rel, &tuple->t_self, yandxtuple);
    CatalogUpdateIndexes(rel, yandxtuple);
#else
    CatalogTupleUpdate(rel, &yandxtuple->t_self, yandxtuple);
#endif

    heap_freetuple(yandxtuple);
  } else {
    // insert
    auto yandxtuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);

#if GP_VERSION_NUM < 70000
    simple_heap_insert(rel, yandxtuple);
    CatalogUpdateIndexes(rel, yandxtuple);
#else
    CatalogTupleInsert(rel, yandxtuple);
#endif

    heap_freetuple(yandxtuple);
  }

  yezzey_endscan(desc);
  heap_close(rel, RowExclusiveLock);

  UnregisterSnapshot(snap);

  /* make changes visible*/
  CommandCounterIncrement();
}
