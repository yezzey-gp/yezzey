
#include "url.h"

std::string craftStoragePath(const std::shared_ptr<IOadv> &adv, ssize_t segindx,
                             ssize_t modcount,
                             const std::string &storage_prefix,
                             XLogRecPtr current_recptr) {

  /* Craft path in format
   *  handler->external_storage_prefix + seg_{segid} + ...
   * example:           (prefix in extrenal storage) / tablespace oid /
   * reloid / md5(schema + relname) / relfilenode / modcount
   * wal-e/<cid>/6/segments_005/seg0/basebackups_005/aosegments/1663_98304_527e1c67fae2e4f3e5caf632d5473cf5_73728_1_1_D_1_D_1_D_1_aoseg_yezzey
   *
   */

  auto prefix = getYezzeyRelationUrl_internal(
      adv->nspname, adv->relname, storage_prefix, adv->coords_, segindx);

  return make_yezzey_url(prefix, modcount, current_recptr);
}

std::string craftWalgStoragePath(const std::shared_ptr<IOadv> &adv,
                                 ssize_t segindx, ssize_t modcount) {

  /* Craft path in format
   *  handler->external_storage_prefix + seg_{segid} + ...
   * example:           (prefix in wal-g compatable form) / tablespace oid /
   * reloid / md5(schema + relname) / relfilenode / modcount
   * segments_005/seg0/basebackups_005/aosegments/1663_98304_527e1c67fae2e4f3e5caf632d5473cf5_73728_1_1_D_1_D_1_D_1_aoseg_yezzey
   *
   */

  auto prefix =
      yezzey_block_file_path(adv->nspname, adv->relname, adv->coords_, segindx);

  return make_yezzey_url(prefix, modcount, InvalidXLogRecPtr);
}

std::string craftUrlXpath(const std::shared_ptr<IOadv> &adv, ssize_t segindx,
                     ssize_t modcount) {
                    
  XLogRecPtr current_recptr;

  if (RecoveryInProgress())
    ereport(
        ERROR,
        (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
         errmsg("recovery is in progress"),
         errhint("WAL control functions cannot be executed during recovery.")));

  current_recptr = GetXLogWriteRecPtr();

 return craftStoragePath(adv, segindx, modcount,
                               adv->external_storage_prefix, current_recptr);

}

std::string craftUrl(const std::shared_ptr<IOadv> &adv, ssize_t segindx,
                     ssize_t modcount) {


                         auto path = craftUrlXpath(adv, segindx, modcount);

  auto url =
      getYezzeyExtrenalStorageBucket(adv->host.c_str(), adv->bucket.c_str()) +
      path;

  // add config path
  return storage_url_add_options(url, adv->config_path.c_str());
  ;
}