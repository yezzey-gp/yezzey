#pragma once
#include <string>
#include <tuple>

struct relnodeCoord {
  Oid dboid;
  Oid filenode;
  int64_t blkno;
  relnodeCoord() {}
  relnodeCoord(Oid dboid, Oid filenode, int64_t blkno)
      : dboid(dboid), filenode(filenode), blkno(blkno) {}
};

relnodeCoord getRelnodeCoordinate(const std::string &fileName);
