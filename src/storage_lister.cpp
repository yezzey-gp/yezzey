#include "storage_lister.h"
#include "util.h"

StorageLister::StorageLister(std::shared_ptr<IOadv> adv, ssize_t segindx)
    : adv_(adv), segindx_(segindx) {
  reader_ = std::shared_ptr<GPReader>(createReaderHandle());
}

/*
 * list all prefix in form 'base=DEFAULTTABLESPACE_OID/<dboid>/<tableoid>.<seg>'
 */

GPReader *StorageLister::createReaderHandle() {
  // throw if initialization error?
  try {
    auto x_path = yezzey_block_file_path(adv_->nspname, adv_->relname,
                                         adv_->coords_, segindx_);
    return reader_init_unsafe(
        (getYezzeyExtrenalStorageBucket(adv_->host.c_str(),
                                        adv_->bucket.c_str()) +
         adv_->external_storage_prefix +
         storage_url_add_options(x_path, adv_->config_path.c_str()))
            .c_str());
  } catch (...) {
    elog(ERROR, "failed to prepare x-storage reader for chunk");
  }
}

bool StorageLister::close() {
  reader_->close();
  return true;
}

StorageLister::~StorageLister() { close(); }

std::vector<storageChunkMeta> StorageLister::list_relation_chunks() {
  std::vector<storageChunkMeta> rv;
  auto content = reader_->getKeyList().contents;
  for (size_t i = 0; i < content.size(); ++i) {
    rv.emplace_back();
    rv.back().chunkName = content[i].getName();
    rv.back().chunkSize = content[i].getSize();
  }
  return rv;
}

std::vector<std::string> StorageLister::list_chunk_names() {
  std::vector<std::string> rv;
  auto content = reader_->getKeyList().contents;
  for (size_t i = 0; i < content.size(); ++i) {
    rv.emplace_back(content[i].getName());
  }
  return rv;
}