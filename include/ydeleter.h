#pragma once

#include <string>

class YDeleter {
public:
  virtual ~YDeleter(){

  };

  // write() attempts to write up to count bytes from the buffer.
  // Always return 0 if EOF, no matter how many times it's invoked. Throw
  // exception if encounters errors.
  virtual bool deleteChunk(const std::string &fn) = 0;

  // This should be reentrant, has no side effects when called multiple times.
  virtual bool close() = 0;
};