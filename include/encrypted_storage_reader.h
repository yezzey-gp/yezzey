#pragma once

#include "crypto.h"
#include "io_adv.h"
#include "yreader.h"

#include "chunkinfo.h"

// read from external storage, using gpreader.
// decrypt all data with gpg
class EncryptedStorageReader : public YReader {
public:
  explicit EncryptedStorageReader(std::shared_ptr<IOadv> adv,
                                  const std::vector<ChunkInfo> &order,
                                  ssize_t segindx);

  virtual ~EncryptedStorageReader();

  virtual bool close();
  virtual bool read(char *buffer, size_t *amount);
  virtual bool empty();

  int prepare();

protected:
  std::unique_ptr<Crypter> crypter_;
  std::shared_ptr<YReader> reader_{nullptr};
  bool read_initialized_{false};

private:
  std::shared_ptr<IOadv> adv_;

protected:
  std::vector<ChunkInfo> order_;

private:
  ssize_t segindx_;
  std::shared_ptr<BlockingBuffer> buf_;
};