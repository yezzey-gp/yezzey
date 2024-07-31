
#include "io.h"

#include "io_adv.h"
#include "url.h"
#include "util.h"

#include "virtual_index.h"

#define USE_YPX_READER 1
#define USE_YPX_WRITER 1

#include "yproxy.h"


YIO::YIO(std::shared_ptr<IOadv> adv, ssize_t segindx, ssize_t modcount,
         const std::string &storage_path)
    : adv_(adv), segindx_(segindx), modcount_(modcount),
      order_(YezzeyVirtualGetOrder(YezzeyFindAuxIndex(adv->reloid), adv->reloid,
                                   adv->coords_.filenode, adv->coords_.blkno)) {
  reader_ = std::make_shared<YProxyReader>(adv_, segindx_, order_);

  writer_ =
      std::make_shared<YProxyWriter>(adv_, segindx_, modcount, storage_path);
}

YIO::YIO(std::shared_ptr<IOadv> adv, ssize_t segindx)
    : adv_(adv), segindx_(segindx),
      order_(YezzeyVirtualGetOrder(YezzeyFindAuxIndex(adv->reloid), adv->reloid,
                                   adv->coords_.filenode, adv->coords_.blkno)) {
  reader_ = std::make_shared<YProxyReader>(adv_, segindx_, order_);
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