#ifdef HAVE_CRYPTO
#include "encrypted_storage_writer.h"
#include "meta.h"
#include "x_writer.h"

EncryptedStorageWriter::EncryptedStorageWriter(std::shared_ptr<IOadv> adv,
                                               ssize_t segindx,
                                               ssize_t modcount,
                                               const std::string &storage_path)
    : adv_(adv), segindx_(segindx), modcount_(modcount),
      storage_path_(storage_path) {
  writer_ = std::make_shared<ExternalWriter>(adv_, segindx_, modcount_,
                                             storage_path_);
  buf_ = std::make_shared<BlockingBuffer>(1 << 24);
  crypter_ = make_unique<Crypter>(adv_, writer_, buf_);
}

EncryptedStorageWriter::~EncryptedStorageWriter() { close(); }

int EncryptedStorageWriter::prepare() {
  auto rc = crypter_->io_prepare_crypt(false);
  if (rc) {
    return rc;
  }
  crypter_->io_dispatch_encrypt();
  return 0;
}

bool EncryptedStorageWriter::write(const char *buffer, size_t *amount) {
  if (adv_->use_gpg_crypto) {
    //
    if (!write_initialized_) {
      if (prepare()) {
        // throw can cath ex here
        // elog(ERROR, "failed to prepare write");
        return false;
      }
      write_initialized_ = true;
    }
    auto ret = buf_->write((const char *)buffer, *amount);
    *amount = ret;
    return ret > 0;
  }

  return writer_->write(buffer, amount);
}

bool EncryptedStorageWriter::close() {
  buf_->close();
  crypter_->waitio();
  return writer_->close();
}
#endif