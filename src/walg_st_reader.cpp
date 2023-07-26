#include "meta.h"
#include "url.h"
#include "util.h"
#include "walg_reader.h"
#include <iostream>

/*
sample:
wal-g
--config conf.yaml
st cat --decrypt
segments_005/seg1/basebackups_005/aosegments/1663_16384_41a9e377c1b0698523e79969eecc4998_16384_1__DY_1_aoseg_yezzey
*/

std::string WALGSTReader::craftString(std::string x_path, size_t segindx) {
  std::string cmd = adv_->walg_bin_path;

  cmd += " --config=" + adv_->walg_config_path;
  cmd += " st cat --decrypt  ";
  std::cerr << x_path << "\n";

  auto modified_x_path = x_path;
  modified_x_path.erase(modified_x_path.begin(),
                        modified_x_path.begin() +
                            adv_->external_storage_prefix.size());

  cmd += modified_x_path;
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
    cmd_ = craftString(order_[order_ptr_].x_path, GpIdentity.segindex);
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