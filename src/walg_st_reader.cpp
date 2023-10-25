#include "meta.h"
#include "url.h"
#include "util.h"
#include "gucs.h"
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

  cmd += x_path;
  return cmd;
}

WALGSTReader::WALGSTReader(std::shared_ptr<IOadv> adv, ssize_t segindx,
                           const std::vector<ChunkInfo> order)
    : adv_(adv), segindx_(segindx), order_ptr_(0), order_(order) {}

WALGSTReader::~WALGSTReader() { close(); }

bool WALGSTReader::close() {
  if (wal_g_ != nullptr) {
    pclose(wal_g_);
    wal_g_ = nullptr;
  }
  return true;
}
bool WALGSTReader::read(char *buffer, size_t *amount) {
  while (1) {
    if (wal_g_ == nullptr) {
      if (order_ptr_ == order_.size()) {
        return false;
      }
      cmd_ = craftString(order_[order_ptr_].x_path, GpIdentity.segindex);
      if (wal_g_ != nullptr) {
        pclose(wal_g_);
        wal_g_ = nullptr;
      }
      wal_g_ = popen(cmd_.c_str(), "r");
      if (wal_g_ == nullptr) {
        elog(WARNING, "failed to read wal-g for %s, %d, %m", order_[order_ptr_].x_path.c_str(), ferror(wal_g_)); 
        return false;
      }

      ++order_ptr_;
    }

    auto rc = fread(buffer, sizeof(char), *amount, wal_g_);
    if (rc < 0) {
        elog(WARNING, "failed to read wal-g for %s, %d, %m", order_[order_ptr_].x_path.c_str(), ferror(wal_g_)); 
        return rc;
    }

    if (rc == 0) {
      if (feof(wal_g_)) {
        wal_g_ = nullptr;
        continue;
      }
      elog(WARNING, "failed to read wal-g for %s, %d, %m", order_[order_ptr_].x_path.c_str(), ferror(wal_g_)); 
      return -1;
    }

    *amount = rc;

    if (feof(wal_g_)) {
      wal_g_ = nullptr;
      return true;
    }
    return rc > 0;
  }
}

int WALGSTReader::prepare() { return 0; }

bool WALGSTReader::empty() {
  if (wal_g_ == nullptr)
    return order_ptr_ == order_.size();
  else
    return order_ptr_ == order_.size() && feof(wal_g_);
};