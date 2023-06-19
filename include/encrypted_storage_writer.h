#pragma once

#include "blocking_buf.h"
#include "crypto.h"
#include "io_adv.h"
#include "ylister.h"
#include "ywriter.h"

// write to external storage, using gpwriter.
// encrypt all data with gpg
class EncryptedStorageWriter : public YWriter {
public:
  explicit EncryptedStorageWriter(std::shared_ptr<IOadv> adv, ssize_t segindx,
                                  ssize_t modcount,
                                  const std::string &storage_path);

  virtual ~EncryptedStorageWriter();

  virtual bool write(const char *buffer, size_t *amount);

  // This should be reentrant, has no side effects when called multiple times.
  virtual bool close() override;

  int prepare();

  std::string getExternalStoragePath() {
    return writer_->getExternalStoragePath();
  }

private:
  std::shared_ptr<IOadv> adv_;
  std::shared_ptr<BlockingBuffer> buf_;
  ssize_t segindx_;
  ssize_t modcount_;
  const std::string storage_path_;

protected:
  std::unique_ptr<Crypter> crypter_{nullptr};
  std::shared_ptr<YWriter> writer_{nullptr};
  bool write_initialized_{false};
};