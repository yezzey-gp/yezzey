
#include "url.h"

std::string craftStoragePath(const std::shared_ptr<YLister> &lister,
                             const std::shared_ptr<IOadv> &adv, ssize_t segindx,
                             ssize_t modcount,
                             const std::string &storage_prefix) {

  /* Craft path in format
   *  handler->external_storage_prefix + seg_{segid} + ...
   * example:           (prefix in wal-g compatable form) / tablespace oid /
   * reloid / md5(schema + relname) / relfilenode / modcount
   * wal-e/<cid>/6/segments_005/seg0/basebackups_005/aosegments/1663_98304_527e1c67fae2e4f3e5caf632d5473cf5_73728_1_1_D_1_D_1_D_1_aoseg_yezzey
   *
   */

  auto prefix = getYezzeyRelationUrl_internal(
      adv->nspname, adv->relname, storage_prefix, adv->coords_, segindx);

  auto content = lister->list_chunk_names();
  if (content.size() == 0) {
    return make_yezzey_url(prefix, {modcount, modcount, modcount, modcount});
    ;
  }
  /* skip first len(prefix) bytes and parse modcounts */
  auto modcounts = parseModcounts(prefix, content.back());
  if (modcounts.size() <= 3) {
    modcounts.push_back(modcount);
  } else if (modcounts.size() == 4) {
    modcounts.back() = modcount;
  } else {
    // else error
  }

  return make_yezzey_url(prefix, modcounts);
  ;
}

std::string craftUrl(const std::shared_ptr<YLister> &lister,
                     const std::shared_ptr<IOadv> &adv, ssize_t segindx,
                     ssize_t modcount) {
  auto path = craftStoragePath(lister, adv, segindx, modcount,
                               adv->external_storage_prefix);

  auto url =
      getYezzeyExtrenalStorageBucket(adv->host.c_str(), adv->bucket.c_str()) +
      path;

  // add config path
  return storage_url_add_options(url, adv->config_path.c_str());
  ;
}