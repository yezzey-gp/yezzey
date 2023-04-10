#include "offload_policy.h"
#include "offload.h"

/*

CREATE TABLE yezzey.offload_metadata(
    reloid           OID,
    relpolicy        offload_policy INT --NOT NULL,
    relext_time      TIMESTAMP,
    rellast_archived TIMESTAMP
)
DISTRIBUTED REPLICATED;
*/

const std::string offload_metadata_relname = "offload_metadata";
const std::string offload_metadata_relname_indx = "offload_metadata_indx";

void YezzeyOffloadPolicyRelation() {
  { /* check existed, if no, return */
  }
  TupleDesc tupdesc;

  ObjectAddress baseobject;
  ObjectAddress yezzey_ao_auxiliaryobject;

  tupdesc = CreateTemplateTupleDesc(Natts_offload_metadata, false);

  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_offload_metadata_reloid,
                     "reloid", OIDOID, -1, 0);
  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_offload_metadata_relpolicy,
                     "relpolicy", INT8OID, -1, 0);
  TupleDescInitEntry(tupdesc, (AttrNumber)Anum_offload_metadata_relext_time,
                     "relext_time", TIMESTAMPOID, -1, 0);
  TupleDescInitEntry(tupdesc,
                     (AttrNumber)Anum_offload_metadata_rellast_archived,
                     "rellast_archived", TIMESTAMPOID, -1, 0);

  (void)heap_create_with_catalog(
      offload_metadata_relname.c_str() /* relname */,
      YEZZEY_AUX_NAMESPACE /* namespace */, 0 /* tablespace */,
      YEZZEY_OFFLOAD_POLICY_RELATION /* relid */,
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
  auto yezzey_rel = heap_open(YEZZEY_OFFLOAD_POLICY_RELATION, ShareLock);
  char * colname = "reloid";
  auto indexColNames = list_make1(makeString(colname));

  auto indexInfo = makeNode(IndexInfo);

  Oid collationObjectId[1];
  Oid classObjectId[1];
  int16 coloptions[1];

  indexInfo->ii_NumIndexAttrs = 1;
  indexInfo->ii_KeyAttrNumbers[0] = Anum_offload_metadata_reloid;
  indexInfo->ii_Expressions = NIL;
  indexInfo->ii_ExpressionsState = NIL;
  indexInfo->ii_Predicate = NIL;
  indexInfo->ii_PredicateState = NIL;
  indexInfo->ii_Unique = true;
  indexInfo->ii_Concurrent = false;

  collationObjectId[0] = InvalidOid;
  classObjectId[0] = OID_BTREE_OPS_OID;
  coloptions[0] = 0;

  (void)index_create(yezzey_rel, offload_metadata_relname_indx.c_str(),
                     YEZZEY_OFFLOAD_POLICY_RELATION_INDX, InvalidOid,
                     InvalidOid, InvalidOid, indexInfo, indexColNames,
                     BTREE_AM_OID, 0 /* tablespace */, collationObjectId,
                     classObjectId, coloptions, (Datum)0, true, false, false,
                     false, true, false, false, true, NULL);

  /* Unlock target table -- no one can see it */
  heap_close(yezzey_rel, ShareLock);

  /* Unlock the index -- no one can see it anyway */
  UnlockRelationOid(YEZZEY_OFFLOAD_POLICY_RELATION_INDX, AccessExclusiveLock);

  CommandCounterIncrement();

  /*
   * Register dependency from the auxiliary table to the master, so that the
   * aoseg table will be deleted if the master is.
   */
  baseobject.classId = ExtensionRelationId;
  baseobject.objectId = get_extension_oid("yezzey", false);
  baseobject.objectSubId = 0;
  yezzey_ao_auxiliaryobject.classId = RelationRelationId;
  yezzey_ao_auxiliaryobject.objectId = YEZZEY_OFFLOAD_POLICY_RELATION;
  yezzey_ao_auxiliaryobject.objectSubId = 0;

  recordDependencyOn(&yezzey_ao_auxiliaryobject, &baseobject,
                     DEPENDENCY_INTERNAL);

  /*
   * Make changes visible
   */
  CommandCounterIncrement();
}

void YezzeySetRelationExpiritySeg(Oid i_reloid, int i_relpolicy,
                                  Timestamp i_relexp) {
  /**/
  ScanKeyData skey[1];

  bool nulls[Natts_offload_metadata];
  Datum values[Natts_offload_metadata];

  memset(nulls, 0, sizeof(nulls));
  memset(values, 0, sizeof(values));

  auto snap = RegisterSnapshot(GetTransactionSnapshot());

  auto offrel = heap_open(YEZZEY_OFFLOAD_POLICY_RELATION, RowExclusiveLock);

  /* INSERT INTO yezzey.offload_metadata VALUES(v_reloid, 1, NULL, NOW()); */

  ScanKeyInit(&skey[0], Anum_offload_metadata_reloid, BTEqualStrategyNumber,
              F_OIDEQ, ObjectIdGetDatum(i_reloid));

  auto scan = heap_beginscan(offrel, snap, 0, NULL);

  auto oldtuple = heap_getnext(scan, ForwardScanDirection);

  values[Anum_offload_metadata_reloid - 1] = i_reloid;
  values[Anum_offload_metadata_relpolicy - 1] = i_relpolicy;
  values[Anum_offload_metadata_relext_time - 1] = i_relexp;
  values[Anum_offload_metadata_rellast_archived - 1] =
      TimestampTzGetDatum(GetCurrentTimestamp());

  if (HeapTupleIsValid(oldtuple)) {
    auto meta = (Form_yezzey_offload_metadata)GETSTRUCT(oldtuple);

    values[Anum_offload_metadata_relpolicy - 1] = meta->relpolicy;
    values[Anum_offload_metadata_relext_time - 1] = meta->relext_time;

    auto offtuple = heap_form_tuple(RelationGetDescr(offrel), values, nulls);

    simple_heap_update(offrel, &oldtuple->t_self, offtuple);

  heap_freetuple(offtuple);
  } else {
    auto offtuple = heap_form_tuple(RelationGetDescr(offrel), values, nulls);

    simple_heap_insert(offrel, offtuple);

  heap_freetuple(offtuple);
  }

  heap_close(offrel, RowExclusiveLock);

  heap_endscan(scan);
  UnregisterSnapshot(snap);

  /* make changes visible */
  CommandCounterIncrement();
}

/*
 * YezzeyDefineOffloadPolicy:
 * do all the work for relation offloading:
 * 1) Open relation in exclusive mode
 * 2) Create yezzey aux index relation for offload relation
 * 3)
 *   3.1) Do main offloading job on segments
 *   3.2) On dispather, clear pre-assigned oids.
 * 4) change relation tablespace to virtual tablespace
 * 5) record entry in offload metadata to track and process
 * in bgworker routines
 * 6) add the dependency in pg_depend
 */
void YezzeyDefineOffloadPolicy(Oid reloid) {
  ObjectAddress relationAddr, extensionAddr;
  int rc;

  relationAddr.classId = RelationRelationId;
  relationAddr.objectId = reloid;
  relationAddr.objectSubId = 0;

  auto yezzey_ext_oid = get_extension_oid("yezzey", false);

  if (!yezzey_ext_oid) {
    elog(ERROR, "failed to get yezzey extnsion oid");
  }

  extensionAddr.classId = ExtensionRelationId;
  extensionAddr.objectId = yezzey_ext_oid;
  extensionAddr.objectSubId = 0;

  //   elog(yezzey_log_level, "recording dependency on yezzey for relation %d",
  //        reloid);

  /*
   * 2) Create auxularry yezzey table to track external storage
   * chunks
   */
  auto aorel = relation_open(reloid, AccessExclusiveLock);
  RelationOpenSmgr(aorel);

  (void)YezzeyCreateAuxIndex(aorel);

  /*
   * @brief do main offload job on segments
   *
   */
  if (Gp_role != GP_ROLE_DISPATCH) {
    /*  */
    if ((rc = yezzey_offload_relation_internal_rel(aorel, true, NULL)) < 0) {
      elog(ERROR,
           "failed to offload relation (oid=%d) to external storage: return "
           "code "
           "%d",
           reloid, rc);
    }
  } else {
    /* clear all pre-assigned oids
     * for auxularry yezzey table relation
     *
     * We need to do it, else we will get error
     * about assigned, but not dispatched oids
     */
    GetAssignedOidsForDispatch();
  }

  /* change relation tablespace */
  (void)YezzeyATExecSetTableSpace(aorel, reloid, YEZZEYTABLESPACE_OID);

  /* record entry in offload metadata */

  (void)YezzeySetRelationExpiritySeg(reloid, 1 /* always external */,
                                     GetCurrentTimestamp());

  /*
   * OK, add the dependency.
   */
  // recordDependencyOn(&relationAddr, &extensionAddr, DEPENDENCY_EXTENSION);
  // recordDependencyOn(&extensionAddr, &relationAddr, DEPENDENCY_NORMAL);
  recordDependencyOn(&relationAddr, &extensionAddr, DEPENDENCY_NORMAL);
  // recordDependencyOn(&extensionAddr, &relationAddr, DEPENDENCY_INTERNAL);
  relation_close(aorel, AccessExclusiveLock);
}