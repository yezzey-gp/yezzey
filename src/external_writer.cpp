
/**
 * @file smgr_s3.cpp
 *
 * @author your name (you@domain.com)
 * @brief
 * @version 0.1
 * @date 2023-02-20
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "external_writer.h"

#include "util.h"
#include <map>
#include <string>
#include <vector>

#include "io_adv.h"

ExternalWriter::ExternalWriter(std::shared_ptr<IOadv> adv, ssize_t segindx,
                               ssize_t modcount,
                               const std::string &storage_path,
                               std::shared_ptr<YLister> lister)
    : adv_(adv), segindx_(segindx), modcount_(modcount),
      storage_path_(storage_path), lister_(lister) {

  if (storage_path.size()) {
    createWriterHandleToPath();
  } else {
    createWriterHandle();
  }
}

// pass yreader
void ExternalWriter::createWriterHandle() {

  /* Craft url in format
   *  handler->external_storage_prefix + seg_{segid} + ...
   * example:
   * wal-e/mdbrhqjnl6k5duk7loi2/6/segments_005/seg0/basebackups_005/aosegments/1663_98304_527e1c67fae2e4f3e5caf632d5473cf5_73728_1_1_D_1_D_1_D_1_aoseg_yezzey
   *
   */

  auto prefix = getYezzeyRelationUrl_internal(adv_->nspname, adv_->relname,
                                              adv_->external_storage_prefix,
                                              adv_->coords_, segindx_);

  auto content = lister_->list_chunk_names();
  if (content.size() == 0) {
    auto url = make_yezzey_url(getYezzeyExtrenalStorageBucket(
                                   adv_->host.c_str(), adv_->bucket.c_str()) +
                                   prefix,
                               {modcount_, modcount_, modcount_, modcount_});

    url = storage_url_add_options(url, adv_->config_path.c_str());

    writer_ = writer_init(url.c_str());
    return;
  }
  /* skip first len(prefix) bytes and parse modcounts */
  auto largest = parseModcounts(prefix, content.back());
  if (largest.size() <= 3) {
    largest.push_back(modcount_);
  } else if (largest.size() == 4) {
    largest.back() = modcount_;
  } else {
    // else error
  }

  auto url = make_yezzey_url(
      getYezzeyExtrenalStorageBucket(adv_->host.c_str(), adv_->bucket.c_str()) +
          prefix,
      largest);

  // config path
  url = storage_url_add_options(url, adv_->config_path.c_str());

  writer_ = writer_init(url.c_str());
}

void ExternalWriter::createWriterHandleToPath() {
  std::string url =
      getYezzeyExtrenalStorageBucket(adv_->host.c_str(), adv_->bucket.c_str());

  url += adv_->external_storage_prefix;
  url += "/seg" + std::to_string(segindx_) + basebackupsPath + storage_path_;

  // config path
  url = storage_url_add_options(url, adv_->config_path.c_str());
  writer_ = writer_init(url.c_str());
}

bool ExternalWriter::write(const char *buffer, size_t *amount) {
  auto inner_amount = *amount;
  auto res = writer_transfer_data(writer_, (char *)buffer, inner_amount);
  *amount = inner_amount;
  return res;
}

/*XXX: fix cleanup*/

bool ExternalWriter::close() {
  auto ptr = &writer_;
  auto res = writer_cleanup(ptr);
  return res;
}

ExternalWriter::~ExternalWriter() { close(); }