#include "storage_lister.h"
#include "virtual_index.h"

StorageLister::StorageLister(std::shared_ptr<IOadv> adv, ssize_t segindx)
    : adv_(adv), segindx_(segindx) {
  reader_ = std::make_shared<ExternalReader>(
      adv_,
      YezzeyVirtualGetOrder(YezzeyFindAuxIndex(adv->reloid),
                            std::get<2>(adv->coords_)),
      segindx_);
}

StorageLister::~StorageLister() { close(); }

bool StorageLister::close() { return reader_->close(); }

std::vector<storageChunkMeta> StorageLister::list_relation_chunks() {
  return reader_->list_relation_chunks();
}

std::vector<std::string> StorageLister::list_chunk_names() {
  return reader_->list_chunk_names();
};
