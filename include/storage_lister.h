#pragma once

#include "external_reader.h"
#include "io_adv.h"
#include "ylister.h"
#include <memory>
#include <vector>

// read from external storage, using gpreader.
// decrypt all data with gpg
class StorageLister : public YLister {
public:
  explicit StorageLister(std::shared_ptr<IOadv> adv, ssize_t segindx);

  virtual ~StorageLister();

  virtual bool close();

  virtual std::vector<storageChunkMeta> list_relation_chunks();
  virtual std::vector<std::string> list_chunk_names();

protected:
  std::shared_ptr<ExternalReader> reader_{nullptr};
  bool read_initialized_{false};

private:
  std::shared_ptr<IOadv> adv_;
  ssize_t segindx_;
};