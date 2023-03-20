#pragma once

#include <string>
#include <vector>

struct storageChunkMeta {
  int64_t chunkSize;
  std::string chunkName;
};

class YLister {
public:
  virtual ~YLister(){

  };

  virtual std::vector<storageChunkMeta> list_relation_chunks() = 0;
  virtual std::vector<std::string> list_chunk_names() = 0;
  // limit number of relation external storage chunks to revieve
  virtual bool close() = 0;
};
