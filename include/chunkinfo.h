#pragma once

#include "pg.h"

#include <string>

struct ChunkInfo {
    XLogRecPtr lsn;
    int64_t modcount;

    ChunkInfo() {}

    ChunkInfo(XLogRecPtr lsn, int64_t modcount)
    : lsn(lsn), modcount(modcount) {} 
};