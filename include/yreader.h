#pragma once

#include <string>
#include <vector>

class YReader {
public:
  virtual ~YReader(){

  };

  virtual bool read(char *buffer, size_t *amount) = 0;
  virtual bool empty() = 0;

  virtual bool close() = 0;
};
