#ifndef INCLUDE_GPCLEANER_H_
#define INCLUDE_GPCLEANER_H_

#include "s3common_headers.h"
#include "s3common_writer.h"
#include "s3interface.h"
#include "s3log.h"
#include "s3macros.h"
#include "s3utils.h"
#include "cleaner.h"

#define S3_DEFAULT_FORMAT "data"

class GPCleaner : public Cleaner {
   public:
    GPCleaner(const S3Params &params);
    virtual ~GPCleaner() {
        this->close();
    }

    virtual void open(const S3Params &params);

    // clean() attempts to delete yezzey chunk from external storage
    // Always return 0 if EOF, no matter how many times it's invoked. Throw exception if encounters
    // errors.
    virtual uint64_t clean();

    // This should be reentrant, has no side effects when called multiple times.
    virtual void close();

   protected:
    string format;
    S3Params params;
    S3RESTfulService restfulService;
    S3InterfaceService s3InterfaceService;
    S3CommonWriter commonWriter;

    // it links to itself by default
    // but the pointer here leaves a chance to mock it in unit test
    S3RESTfulService *restfulServicePtr;
};

GPCleaner *cleaner_init(const char *url_with_options);

#endif /* INCLUDE_GPCLEANER_H_ */
