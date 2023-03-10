#pragma once

#include "crypto.h"
#include "io_adv.h"
#include "yreader.h"

// read from external storage, using gpreader.
// decrypt all data with gpg
class EncryptedStorageReader : public YReader {
public:
  explicit EncryptedStorageReader(std::shared_ptr<IOadv> adv, ssize_t segindx);

  virtual ~EncryptedStorageReader();

  virtual bool close();
  virtual bool read(char *buffer, size_t *amount);
  virtual bool empty();

  virtual std::vector<storageChunkMeta> list_relation_chunks();
  virtual std::vector<std::string> list_chunk_names();

  virtual void BumpArenda(size_t count);

  int prepare();

protected:
  std::unique_ptr<Crypter> crypter_;
  std::shared_ptr<YReader> reader_{nullptr};
  bool read_initialized_{false};

private:
  std::shared_ptr<IOadv> adv_;
  std::shared_ptr<BlockingBuffer> buf_;
  ssize_t segindx_;
};