#pragma once

#include "pg.h"

#include <string>

struct ChunkInfo {
  XLogRecPtr lsn;
  int64_t modcount;
  std::string x_path;
  uint64_t size;
  uint64_t start_off;
  bool enc;

  ChunkInfo() {}

  ChunkInfo(XLogRecPtr lsn, int64_t modcount, const char *x_path, uint64_t size, uint64_t start_off, bool enc)
      : lsn(lsn), modcount(modcount), x_path(x_path), size(size), start_off(start_off), enc(enc) {}
};