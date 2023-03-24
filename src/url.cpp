
#include "url.h"

std::string craftStoragePath(const std::shared_ptr<YLister> &lister,
                             const std::shared_ptr<IOadv> &adv, ssize_t segindx,
                             ssize_t modcount,
                             const std::string &storage_prefix) {

  auto prefix = getYezzeyRelationUrl_internal(
      adv->nspname, adv->relname, storage_prefix, adv->coords_, segindx);

  auto content = lister->list_chunk_names();
  if (content.size() == 0) {
    auto url =
        make_yezzey_url(prefix, {modcount, modcount, modcount, modcount});
    return url;
  }
  /* skip first len(prefix) bytes and parse modcounts */
  auto largest = parseModcounts(prefix, content.back());
  if (largest.size() <= 3) {
    largest.push_back(modcount);
  } else if (largest.size() == 4) {
    largest.back() = modcount;
  } else {
    // else error
  }

  auto url = make_yezzey_url(

      prefix, largest);

  return url;
}

std::string craftUrl(const std::shared_ptr<YLister> &lister,
                     const std::shared_ptr<IOadv> &adv, ssize_t segindx,
                     ssize_t modcount) {

  /* Craft url in format
   *  handler->external_storage_prefix + seg_{segid} + ...
   * example:
   * wal-e/mdbrhqjnl6k5duk7loi2/6/segments_005/seg0/basebackups_005/aosegments/1663_98304_527e1c67fae2e4f3e5caf632d5473cf5_73728_1_1_D_1_D_1_D_1_aoseg_yezzey
   *
   */

  auto prefix = getYezzeyRelationUrl_internal(adv->nspname, adv->relname,
                                              adv->external_storage_prefix,
                                              adv->coords_, segindx);

  auto content = lister->list_chunk_names();
  if (content.size() == 0) {
    auto url = make_yezzey_url(
        getYezzeyExtrenalStorageBucket(adv->host.c_str(), adv->bucket.c_str()) +
            prefix,
        {modcount, modcount, modcount, modcount});

    url = storage_url_add_options(url, adv->config_path.c_str());
    return url;
  }
  /* skip first len(prefix) bytes and parse modcounts */
  auto largest = parseModcounts(prefix, content.back());
  if (largest.size() <= 3) {
    largest.push_back(modcount);
  } else if (largest.size() == 4) {
    largest.back() = modcount;
  } else {
    // else error
  }

  auto url = make_yezzey_url(
      getYezzeyExtrenalStorageBucket(adv->host.c_str(), adv->bucket.c_str()) +
          prefix,
      largest);

  // config path
  url = storage_url_add_options(url, adv->config_path.c_str());

  return url;
}