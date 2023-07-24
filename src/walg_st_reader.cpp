#include "meta.h"
#include "url.h"
#include "util.h"
#include "walg_reader.h"

/*
wal-g
    --config=./conf.yaml aosegfile-stream
    --segno=1
    --relationName='aot_sample'
    --schemaName='public'
    --tablespaceOid=1663 (base/ tablespace)
    --dbOid=x
    --relOid=y
    --blockno=z
*/

std::string craftString(std::shared_ptr<IOadv> adv, int64_t modcount,
                        size_t segindx) {
  std::string cmd = adv->walg_bin_path;

  cmd += " --config=" + adv->walg_config_path;
  cmd += " st cat ";

  cmd += craftWalgStoragePath(adv, segindx, modcount);
  return cmd;
}

WALGSTReader::WALGSTReader(std::shared_ptr<IOadv> adv, ssize_t segindx,
                           const std::vector<ChunkInfo> order)
    : adv_(adv), segindx_(segindx), order_ptr_(0), order_(order) {}

WALGSTReader::~WALGSTReader() { close(); }

bool WALGSTReader::close() {
  if (wal_g_ != nullptr) {
    auto *pbuf = wal_g_->rdbuf();
    pbuf->kill(SIGTERM);
    wal_g_->close();
  }
  return true;
}
bool WALGSTReader::read(char *buffer, size_t *amount) {
  if (wal_g_ == nullptr || wal_g_->eof()) {
    if (order_ptr_ == order_.size()) {
      return false;
    }
    cmd_ = craftString(adv_, order_[order_ptr_].modcount, GpIdentity.segindex);
    wal_g_ = make_unique<redi::ipstream>(cmd_);
    ++order_ptr_;
  }

  wal_g_->read(buffer, *amount);
  *amount = wal_g_->gcount();
  return *amount > 0;
}
int WALGSTReader::prepare() { return 0; }

bool WALGSTReader::empty() {
  if (wal_g_ == nullptr)
    return false;
  else
    return order_ptr_ == order_.size() && wal_g_->eof();
};