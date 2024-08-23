#pragma once
#include <string>
#include <tuple>

struct relnodeCoord {
  Oid spcNode;
  Oid dboid;
  Oid filenode;
  int64_t blkno;
  relnodeCoord() {}
  relnodeCoord(Oid spcNode, Oid dboid, Oid filenode, int64_t blkno)
      : spcNode(spcNode), dboid(dboid), filenode(filenode), blkno(blkno) {}
};

relnodeCoord getRelnodeCoordinate(Oid spcNode, const std::string &fileName);
