
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

#include "x_writer.h"

#include "url.h"
#include "util.h"
#include <map>
#include <string>
#include <vector>

#include "io_adv.h"

ExternalWriter::ExternalWriter(std::shared_ptr<IOadv> adv, ssize_t segindx,
                               ssize_t modcount,
                               const std::string &storage_path)
    : adv_(adv), segindx_(segindx), modcount_(modcount),
      storage_path_(storage_path),
      insertion_rec_ptr_(yezzeyGetXStorageInsertLsn()),
      storage_offload_path_(
          craftUrlXpath(adv, segindx, modcount, insertion_rec_ptr_)) {
  InitWriter();
}

// pass yreader
std::string ExternalWriter::InitWriter() {
  /* this sets storage_offload_path_ */
  if (storage_path_.size()) {
    return createWriterHandleToPath();
  }
  return createWriterHandle();
}

// pass yreader
std::string ExternalWriter::createWriterHandle() {
  auto url = craftUrl(adv_, segindx_, modcount_, insertion_rec_ptr_);
  writer_ = writer_init(url.c_str());
  return url;
}

std::string ExternalWriter::createWriterHandleToPath() {
  std::string url =
      getYezzeyExtrenalStorageBucket(adv_->host.c_str(), adv_->bucket.c_str());

  url += adv_->external_storage_prefix;
  url += "/seg" + std::to_string(segindx_) + basebackupsPath + storage_path_;

  // config path
  url = storage_url_add_options(url, adv_->config_path.c_str());
  writer_ = writer_init(url.c_str());
  return url;
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