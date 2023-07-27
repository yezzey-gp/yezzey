#pragma once

#include "pg.h"

#include <string>

struct ChunkInfo {
  XLogRecPtr lsn;
  int64_t modcount;
  std::string x_path;

  ChunkInfo() {}

  ChunkInfo(XLogRecPtr lsn, int64_t modcount, const char *x_path)
      : lsn(lsn), modcount(modcount), x_path(x_path) {}
};