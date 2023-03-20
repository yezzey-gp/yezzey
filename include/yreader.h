#pragma once

#include <string>
#include <vector>

class YReader {
public:
  virtual ~YReader(){

  };

  virtual bool read(char *buffer, size_t *amount) = 0;
  virtual bool empty() = 0;

  // limit number of relation external storage chunks to revieve
  virtual void BumpArenda(size_t count) = 0;
  virtual bool close() = 0;
};
