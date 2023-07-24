
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

GPReader *ExternalReader::createReaderHandle(const char *x_path) {
  // throw if initialization error?
  return reader_init_unsafe(
      storage_url_add_options(x_path, adv_->config_path.c_str()).c_str());
}

bool ExternalReader::read(char *buffer, size_t *amount) {
  if (order_ptr_ == order_.size()) {
    *amount = 0;
    return false;
  }
  if (reader_empty(reader_)) {
    ++order_ptr_;
    if (order_ptr_ == order_.size()) {
      *amount = 0;
      return false;
    }

    reader_ = createReaderHandle(order_[order_ptr_].x_path);
  }
  int inner_amount = *amount;
  auto res = reader_transfer_data(reader_, buffer, inner_amount);
  *amount = inner_amount;
  return res;
}

bool ExternalReader::empty() { return reader_empty(reader_); }

/*XXX: fix cleanup*/

bool ExternalReader::close() {
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

void ExternalReader::BumpArenda(size_t count) {
  return reader_->BumpArenda(count);
};