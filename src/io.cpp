
#include "io.h"

#include "io_adv.h"
#include "util.h"

#include "encrypted_storage_reader.h"
#include "encrypted_storage_writer.h"
#include "walg_reader.h"

#define USE_WLG_READER 1

YIO::YIO(std::shared_ptr<IOadv> adv, ssize_t segindx, ssize_t modcount,
         const std::string &storage_path)
    : adv_(adv), segindx_(segindx), modcount_(modcount) {
#if USE_WLG_READER
  reader_ = std::make_shared<WALGReader>(adv_, segindx_);
#else
  reader_ = std::make_shared<EncryptedStorageReader>(adv_, segindx_);
#endif

  writer_ = std::make_shared<EncryptedStorageWriter>(adv_, segindx_, modcount,
                                                     storage_path, reader_);
}

YIO::YIO(std::shared_ptr<IOadv> adv, ssize_t segindx) {
#if USE_WLG_READER
  reader_ = std::make_shared<WALGReader>(adv_, segindx_);
#else
  reader_ = std::make_shared<EncryptedStorageReader>(adv_, segindx_);
#endif
}

bool YIO::io_read(char *buffer, size_t *amount) {
  if (reader_.get() == nullptr) {
    *amount = -1;
    return false;
  }
  return reader_->read(buffer, amount);
}

bool YIO::io_write(char *buffer, size_t *amount) {
  if (writer_.get() == nullptr) {
    *amount = -1;
    return false;
  }
  return writer_->write(buffer, amount);
}

bool YIO::io_close() {
  auto rrs = reader_->close();
  auto wrs = writer_->close();
  return rrs && wrs;
}

YIO::~YIO() { io_close(); }

bool YIO::reader_empty() {
  return reader_.get() == nullptr ? true : reader_->empty();
}

std::vector<storageChunkMeta> YIO::list_relation_chunks() {
  return reader_->list_relation_chunks();
}