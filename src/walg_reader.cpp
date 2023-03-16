#include "walg_reader.h"

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

  std::string cmd =
      adv->walg_bin_path;
    
  cmd += " --config=" + adv->walg_config_path;
  cmd += "aosegfile-stream";
  auto coords = adv->coords_;
  auto dbOid = std::get<0>(coords);
  auto relOid = std::get<1>(coords);
  auto relBlck = std::get<2>(coords);
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
  if (initialized_)
    wal_g_->close();
  return true;
}
bool WALGReader::read(char *buffer, size_t *amount) {
  if (!initialized_) {
    wal_g_ = std::make_unique<redi::ipstream>(cmd_);
    initialized_ = true;
  }

  wal_g_->read(buffer, *amount);
  *amount = wal_g_->gcount();
  return *amount > 0;
}
std::vector<storageChunkMeta> WALGReader::list_relation_chunks() { return {}; }
std::vector<std::string> WALGReader::list_chunk_names() { return {}; }

int WALGReader::prepare() { return 0; }

void WALGReader::BumpArenda(size_t /*count*/) {}

bool WALGReader::empty() {
  if (!initialized_)
    return false;
  else
    return wal_g_->eof();
};