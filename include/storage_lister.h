#pragma once

#include "io_adv.h"
/* should go after ioadv */
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

public:

protected:
  bool read_initialized_{false};

private:
  std::shared_ptr<IOadv> adv_;
  ssize_t segindx_;
};