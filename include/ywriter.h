#pragma once

#include <string>

class YWriter {
public:
  virtual ~YWriter(){

  };

  // write() attempts to write up to count bytes from the buffer.
  // Always return 0 if EOF, no matter how many times it's invoked. Throw
  // exception if encounters errors.
  virtual bool write(const char *buffer, size_t *amount) = 0;

  // This should be reentrant, has no side effects when called multiple times.
  virtual bool close() = 0;

  virtual std::string getExternalStoragePath() = 0;
};