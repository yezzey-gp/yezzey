#include "yezzey_expire.h"
#include "string"
#include "url.h"
#include "util.h"
#include "offload_policy.h"

void YezzeyRecordRelationExpireLsn(Relation rel) {
  /* check that relation is yezzey-related. */
  if (!YezzeyCheckRelationOffloaded(RelationGetRelid(rel))) {
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

  bool nulls[Natts_yezzey_expire_index];
  Datum values[Natts_yezzey_expire_index];

  memset(nulls, 0, sizeof(nulls));
  memset(values, 0, sizeof(values));

  /* INSERT INTO  yezzey.yezzey_virtual_index_<oid> VALUES(segno, start_offset,
   * 0, modcount, external_path) */

  auto yandxexprel = heap_open(YEZZEY_EXPIRE_INDEX_RELATION, RowExclusiveLock);

  values[Anum_yezzey_expire_index_reloid - 1] =
      ObjectIdGetDatum(RelationGetRelid(rel));
  values[Anum_yezzey_expire_index_relfileoid - 1] =
      ObjectIdGetDatum(rel->rd_node.relNode);

  values[Anum_yezzey_expire_index_fqnmd5 - 1] =
      PointerGetDatum(cstring_to_text(md.c_str()));
  values[Anum_yezzey_expire_index_lsn - 1] =
      LSNGetDatum(yezzeyGetXStorageInsertLsn());

  auto yandxtuple =
      heap_form_tuple(RelationGetDescr(yandxexprel), values, nulls);

  simple_heap_insert(yandxexprel, yandxtuple);
  CatalogUpdateIndexes(yandxexprel, yandxtuple);

  heap_freetuple(yandxtuple);
  heap_close(yandxexprel, RowExclusiveLock);

  CommandCounterIncrement();
} // YezzeyRecordRelationExpireLsn

/*

CREATE TABLE yezzey.yezzey_expire_index(
    reloid           OID,
    expire_lsn       LSN
)
DISTRIBUTED REPLICATED;
*/

const std::string yezzey_expire_index_relname = "yezzey_expire_index";
const std::string yezzey_expire_index_indx_relname = "yezzey_expire_index_indx";

void YezzeyCreateRelationExpireIndex(void) {
  { 
    /* check existed, if no, return */
  }
  TupleDesc tupdesc;

  ObjectAddress baseobject;
  ObjectAddress yezzey_ao_auxiliaryobject;

  tupdesc = CreateTemplateTupleDesc(Natts_yezzey_expire_index, false);

  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_expire_index_reloid,
                     "reloid", OIDOID, -1, 0);

  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_expire_index_relfileoid,
                     "relfileoid", OIDOID, -1, 0);

  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_expire_index_fqnmd5,
                     "fqnmd5", TEXTOID, -1, 0);

  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_yezzey_expire_index_lsn,
                     "expire_lsn", LSNOID, -1, 0);

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

  /* Make this table visible, else yezzey virtual index creation will fail */
  CommandCounterIncrement();

  /* ShareLock is not really needed here, but take it anyway */
  auto yezzey_rel = heap_open(YEZZEY_EXPIRE_INDEX_RELATION, ShareLock);
  char *colname = "reloid";
  auto indexColNames = list_make1(makeString(colname));

  auto indexInfo = makeNode(IndexInfo);

  Oid collationObjectId[1];
  Oid classObjectId[1];
  int16 coloptions[1];

  indexInfo->ii_NumIndexAttrs = 1;
  indexInfo->ii_KeyAttrNumbers[0] = Anum_yezzey_expire_index_reloid;
  indexInfo->ii_Expressions = NIL;
  indexInfo->ii_ExpressionsState = NIL;
  indexInfo->ii_Predicate = NIL;
  indexInfo->ii_PredicateState = NIL;
  indexInfo->ii_Unique = true;
  indexInfo->ii_Concurrent = false;

  collationObjectId[0] = InvalidOid;
  classObjectId[0] = OID_BTREE_OPS_OID;
  coloptions[0] = 0;

  (void)index_create(yezzey_rel, yezzey_expire_index_indx_relname.c_str(),
                     YEZZEY_EXPIRE_INDEX_RELATION_INDX, InvalidOid, InvalidOid,
                     InvalidOid, indexInfo, indexColNames, BTREE_AM_OID,
                     0 /* tablespace */, collationObjectId, classObjectId,
                     coloptions, (Datum)0, true, false, false, false, true,
                     false, false, true, NULL);

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