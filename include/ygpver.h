#ifndef YEZZEY_GPVER_H
#define YEZZEY_GPVER_H

#include "pg_config.h"

#define IsCloudBerry (GP_VERSION_NUM < 60000 && GP_VERSION_NUM >= 10000)
#define IsGreenplum7 (GP_VERSION_NUM >= 70000)
#define IsGreenplum6 (GP_VERSION_NUM >= 60000 && GP_VERSION_NUM < 70000)

#define IsModernYezzey (IsCloudBerry || IsGreenplum7)


#endif /* YEZZEY_GPVER_H */