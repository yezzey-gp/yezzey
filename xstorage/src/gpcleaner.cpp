#include "gpcleaner.h"
#include "s3memory_mgmt.h"

GPCleaner::GPCleaner(const S3Params& params)
    : params(params), restfulService(this->params), s3InterfaceService(this->params) {
    restfulServicePtr = &restfulService;
}

void GPCleaner::open(const S3Params& params) {
    this->s3InterfaceService.setRESTfulService(this->restfulServicePtr);
    this->commonWriter.setS3InterfaceService(&this->s3InterfaceService);
    this->commonWriter.open(this->params);
}

uint64_t GPCleaner::clean() {
    this->s3InterfaceService.deleteChunk(this->params.getS3Url());
    return 0;
}

void GPCleaner::close() {
    this->commonWriter.close();
}

GPCleaner *cleaner_init(const char *url_with_options) {

    if (!url_with_options) {
        return NULL;
    }

    string urlWithOptions(url_with_options);

    S3Params params = InitConfig(urlWithOptions);

    InitRemoteLog();

    auto cleaner = new GPCleaner(params);
    if (cleaner == NULL) {
        return NULL;
    }

    cleaner->open(params);
    return cleaner;
}

