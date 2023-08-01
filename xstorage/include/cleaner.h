#ifndef __S3_CLEANER_H__
#define __S3_CLEANER_H__

#include "s3common_headers.h"
#include "s3params.h"

class Cleaner {
   public:
    virtual ~Cleaner() {
    }

    virtual void open(const S3Params &params) = 0;

    // clean() attempts to delete yezzey chunk from external storage
    // Always return 0 if EOF, no matter how many times it's invoked. Throw exception if encounters
    // errors.
    virtual uint64_t clean() = 0;

    // This should be reentrant, has no side effects when called multiple times.
    virtual void close() = 0;
};

#endif
