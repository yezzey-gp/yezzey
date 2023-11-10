#pragma once

#include "pg.h"

#include <string>

struct ChunkInfo {
  XLogRecPtr lsn;
  int64_t modcount;
  int64_t size;
  std::string x_path;

  ChunkInfo() {}

  ChunkInfo(XLogRecPtr lsn, int64_t modcount, const char *x_path, uint64_t size)
      : lsn(lsn), modcount(modcount), x_path(x_path), size(size) {}
};