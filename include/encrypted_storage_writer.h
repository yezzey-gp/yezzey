#pragma once

#include "crypto.h"
#include "io_adv.h"
#include "yreader.h"
#include "ywriter.h"

// write to external storage, using gpwriter.
// encrypt all data with gpg
class EncryptedStorageWriter : public YWriter {
public:
  explicit EncryptedStorageWriter(std::shared_ptr<IOadv> adv, ssize_t segindx,
                                  ssize_t modcount,
                                  const std::string &storage_path,
                                  std::shared_ptr<YReader> reader_);

  virtual ~EncryptedStorageWriter();

  virtual bool write(const char *buffer, size_t *amount);

  // This should be reentrant, has no side effects when called multiple times.
  virtual bool close() override;

  int prepare();

protected:
  std::unique_ptr<Crypter> crypter_;
  std::shared_ptr<YReader> reader_{nullptr};
  std::shared_ptr<YWriter> writer_{nullptr};
  bool write_initialized_{false};

private:
  std::shared_ptr<IOadv> adv_;
  std::shared_ptr<BlockingBuffer> buf_;
  const std::string storage_path_;
  ssize_t segindx_;
  ssize_t modcount_;
};