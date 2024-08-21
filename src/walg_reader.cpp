#if USE_WLG_READER
#include "walg_reader.h"
#include "meta.h"
#include "util.h"

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

std::string craftString(std::shared_ptr<IOadv> adv, size_t segindx) {
  std::string cmd = adv->walg_bin_path;

  cmd += " --config=" + adv->walg_config_path;
  cmd += " aosegfile-stream ";
  auto coords = adv->coords_;
  auto dbOid = coords.dboid;
  auto relOid = coords.filenode;
  auto relBlck = coords.blkno;
  cmd += " --segno=" + std::to_string(segindx);
  cmd += " --relationName=" + adv->relname;
  cmd += " --schemaName=" + adv->nspname;
  cmd += " --tablespaceOid=1663";
  cmd += " --dbOid=" + std::to_string(dbOid);
  cmd += " --relOid=" + std::to_string(relOid);
  cmd += " --blockno=" + std::to_string(relBlck);
  return cmd;
}

WALGReader::WALGReader(std::shared_ptr<IOadv> adv, ssize_t segindx)
    : adv_(adv), segindx_(segindx), cmd_(craftString(adv, segindx)) {}

WALGReader::~WALGReader() { close(); }

bool WALGReader::close() {
  if (initialized_) {
    auto *pbuf = wal_g_->rdbuf();
    pbuf->kill(SIGTERM);
    wal_g_->close();
  }
  return true;
}
bool WALGReader::read(char *buffer, size_t *amount) {
  if (!initialized_) {
    wal_g_ = make_unique<redi::ipstream>(cmd_);
    initialized_ = true;
  }

  wal_g_->read(buffer, *amount);
  *amount = wal_g_->gcount();
  return *amount > 0;
}
int WALGReader::prepare() { return 0; }

bool WALGReader::empty() {
  if (!initialized_)
    return false;
  else
    return wal_g_->eof();
};
#endif