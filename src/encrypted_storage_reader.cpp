#include "encrypted_storage_reader.h"
#include "meta.h"

EncryptedStorageReader::EncryptedStorageReader(
    std::shared_ptr<IOadv> adv, const std::vector<ChunkInfo> &order,
    ssize_t segindx)
    : adv_(adv), order_(order), segindx_(segindx) {
  buf_ = std::make_shared<BlockingBuffer>(1 << 12);

  /* TODO: with order */
  reader_ = std::make_shared<ExternalReader>(adv_, order_, segindx_);
  crypter_ = make_unique<Crypter>(adv_, reader_, buf_);
}

EncryptedStorageReader::~EncryptedStorageReader() { close(); }

int EncryptedStorageReader::prepare() {
  auto rc = crypter_->io_prepare_crypt(true);
  if (rc) {
    return rc;
  }
  crypter_->io_dispatch_decrypt();
  return 0;
}

bool EncryptedStorageReader::read(char *buffer, size_t *amount) {
  if (adv_->use_gpg_crypto) {
    //
    if (!read_initialized_) {
      if (prepare()) {
        // throw and catch exception
        return false;
      }
      read_initialized_ = true;
    }

    auto ret = buf_->read((char *)buffer, *amount);
    *amount = ret;
    return ret > 0;
  }

  return reader_->read(buffer, amount);
}

bool EncryptedStorageReader::close() {
  buf_->close();
  crypter_->waitio();
  buf_->reset();
  return reader_->close();
}

bool EncryptedStorageReader::empty() { return reader_->empty(); }

void EncryptedStorageReader::BumpArenda(size_t count) {
  return reader_->BumpArenda(count);
};