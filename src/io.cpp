
#include "io.h"

#include "io_adv.h"
#include "url.h"
#include "util.h"

#include "storage_lister.h"
#include "virtual_index.h"

#define USE_YPX_READER 1
#define USE_YPX_WRITER 1

#define USE_WLG_WRITER 0
#define USE_WLG_READER 0

#if USE_YPX_READER
#include "yproxy.h"
#endif

#if USE_WLG_WRITER
#include "walg_reader.h"
#include "walg_writer.h"
#endif

#if HAVE_CRYPTO
#include "encrypted_storage_reader.h"
#include "encrypted_storage_writer.h"
#else
#endif

YIO::YIO(std::shared_ptr<IOadv> adv, ssize_t segindx, ssize_t modcount,
         const std::string &storage_path)
    : adv_(adv), segindx_(segindx), modcount_(modcount),
      order_(YezzeyVirtualGetOrder(YezzeyFindAuxIndex(adv->reloid), adv->reloid,
                                   adv->coords_.filenode, adv->coords_.blkno)) {
#if USE_YPX_READER
  reader_ = std::make_shared<YProxyReader>(adv_, segindx_, order_);

#else
#error "Unsupported storage read configuration"
#endif

#if USE_YPX_WRITER
  writer_ =
      std::make_shared<YProxyWriter>(adv_, segindx_, modcount, storage_path);
#elif USE_WLG_WRITER

#error "Unsupported storage write configuration"
#endif
}

YIO::YIO(std::shared_ptr<IOadv> adv, ssize_t segindx)
    : adv_(adv), segindx_(segindx),
      order_(YezzeyVirtualGetOrder(YezzeyFindAuxIndex(adv->reloid), adv->reloid,
                                   adv->coords_.filenode, adv->coords_.blkno)) {
#if USE_YPX_READER
  reader_ = std::make_shared<YProxyReader>(adv_, segindx_, order_);

#else
#error "Unsupported storage read configuration"
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

  /* insert new chuck in yezzey virtual index */
  return writer_->write(buffer, amount);
}

bool YIO::io_close() {
  bool rrs = true;
  bool wrs = true;
  if (reader_.get()) {
    rrs = reader_->close();
  }
  if (writer_.get()) {
    wrs = writer_->close();
  }
  return rrs && wrs;
}

YIO::~YIO() { io_close(); }

bool YIO::reader_empty() {
  return reader_.get() == nullptr ? true : reader_->empty();
}