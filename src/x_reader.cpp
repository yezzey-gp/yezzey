
/**
 *
 *
 * @author your name (you@domain.com)
 * @brief
 * @version 0.1
 * @date 2023-02-20
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "x_reader.h"

#include <map>
#include <string>
#include <vector>

#include "util.h"

#include "io_adv.h"

ExternalReader::ExternalReader(std::shared_ptr<IOadv> adv,
                               const std::vector<ChunkInfo> &order,
                               ssize_t segindx)
    : adv_(adv), order_(order), segindx_(segindx), order_ptr_(0) {
  if (order_.size()) {
    reader_ = createReaderHandle(order_[0].x_path);
  }
}

/*
 * fileName is in form 'base=DEFAULTTABLESPACE_OID/<dboid>/<tableoid>.<seg>'
 */

GPReader *ExternalReader::createReaderHandle(std::string x_path) {
  // throw if initialization error?
  try {
    return reader_init_unsafe(
        (getYezzeyExtrenalStorageBucket(adv_->host.c_str(),
                                        adv_->bucket.c_str()) +
         storage_url_add_options(x_path, adv_->config_path.c_str()))
            .c_str());
  } catch (...) {
    elog(ERROR, "failed to prepare x-storage reader for chunk");
  }
}

GPReader *ExternalReader::recreateReaderHandle(
    std::string x_path, std::shared_ptr<PreAllocatedMemory> prealloc) {
  // throw if initialization error?
  try {
    return reader_reinit_unsafe(
        (getYezzeyExtrenalStorageBucket(adv_->host.c_str(),
                                        adv_->bucket.c_str()) +
         storage_url_add_options(x_path, adv_->config_path.c_str()))
            .c_str(),
        prealloc);
  } catch (...) {
    elog(ERROR, "failed to prepare x-storage reader for chunk");
  }
}

bool ExternalReader::read(char *buffer, size_t *amount) {
  if (order_ptr_ == order_.size()) {
    *amount = 0;
    return false;
  }
  if (reader_empty(reader_)) {
    /**/
    auto reader = &reader_;
    auto prealloc = reader_->getParams().getMemoryContext().prealloc;
    auto res = reader_cleanup_unsafe(reader);
    if (!res) {
      *amount = 0;
      return res;
    }
    ++order_ptr_;
    if (order_ptr_ == order_.size()) {
      *amount = 0;
      return true;
    }

    reader_ = recreateReaderHandle(order_[order_ptr_].x_path, prealloc);
    /* return zero byte to indocate file end */
    *amount = 0;
    return true;
  }
  int inner_amount = *amount;
  auto res = reader_transfer_data(reader_, buffer, inner_amount);
  *amount = inner_amount;
  return res;
}

bool ExternalReader::empty() {
  return order_ptr_ + 1 == order_.size() && reader_empty(reader_);
}

/*XXX: fix cleanup*/

bool ExternalReader::close() {
  if (!reader_) {
    return true;
  }
  auto reader = &reader_;
  auto res = reader_cleanup_unsafe(reader);
  return res;
}

ExternalReader::~ExternalReader() { close(); }

std::vector<storageChunkMeta> ExternalReader::list_relation_chunks() {
  std::vector<storageChunkMeta> rv;
  auto content = reader_->getKeyList().contents;
  for (size_t i = 0; i < content.size(); ++i) {
    rv.emplace_back();
    rv.back().chunkName = content[i].getName();
    rv.back().chunkSize = content[i].getSize();
  }
  return rv;
}

std::vector<std::string> ExternalReader::list_chunk_names() {
  std::vector<std::string> rv;
  auto content = reader_->getKeyList().contents;
  for (size_t i = 0; i < content.size(); ++i) {
    rv.emplace_back(content[i].getName());
  }
  return rv;
}