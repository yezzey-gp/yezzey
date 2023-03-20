
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

#include "external_reader.h"

#include <map>
#include <string>
#include <vector>

#include "util.h"

#include "io_adv.h"

ExternalReader::ExternalReader(std::shared_ptr<IOadv> adv, ssize_t segindx)
    : adv_(adv), segindx_(segindx) {

  createReaderHandle();
}

/*
 * fileName is in form 'base=DEFAULTTABLESPACE_OID/<dboid>/<tableoid>.<seg>'
 */

void ExternalReader::createReaderHandle() {

  auto prefix = getYezzeyRelationUrl_internal(adv_->nspname, adv_->relname,
                                              adv_->external_storage_prefix,
                                              adv_->coords_, segindx_);

  // add config path FIXME
  auto reader = reader_init(
      storage_url_add_options((getYezzeyExtrenalStorageBucket(
                                   adv_->host.c_str(), adv_->bucket.c_str()) +
                               prefix),
                              adv_->config_path.c_str())
          .c_str());

  if (reader == NULL) {
    /* error while external storage initialization */
    // throw here
    return;
  }

  auto content = reader->getKeyList().contents;
  std::map<std::string, std::vector<int64_t>> cache;

  std::sort(
      content.begin(), content.end(),
      [&cache, &prefix](const BucketContent &c1, const BucketContent &c2) {
        auto v1 = cache[c1.getName()] =
            cache.count(c1.getName()) ? cache[c1.getName()]
                                      : parseModcounts(prefix, c1.getName());
        auto v2 = cache[c2.getName()] =
            cache.count(c2.getName()) ? cache[c2.getName()]
                                      : parseModcounts(prefix, c2.getName());
        return v1 < v2;
      });

  reader_ = reader;
}

bool ExternalReader::read(char *buffer, size_t *amount) {
  int inner_amount = *amount;
  auto res = reader_transfer_data(reader_, buffer, inner_amount);
  *amount = inner_amount;
  return res;
}

bool ExternalReader::empty() { return reader_empty(reader_); }

/*XXX: fix cleanup*/

bool ExternalReader::close() {
  auto reader = &reader_;
  auto res = reader_cleanup(reader);
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