#include "yezzey_meta.h"

void YezzeyUpdateMetadataRelations(
    Oid yandexoid /*yezzey auxiliary index oid*/, Oid reloid,
    Oid relfilenodeOid, int64_t blkno, int64_t offset_start,
    int64_t offset_finish, int32_t encrypted, int32_t reused, int64_t modcount,
    XLogRecPtr lsn, const char *x_path /* external path */, const char *md5) {

  YezzeyVirtualIndexInsert(yandexoid, reloid, relfilenodeOid, blkno,
                           offset_start, offset_finish, encrypted, reused,
                           modcount, lsn, x_path);
  YezzeyUpsertLastUseLsn(reloid, relfilenodeOid, md5, lsn);
}