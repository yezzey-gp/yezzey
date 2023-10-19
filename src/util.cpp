
/**
 * @file smgr_s3.cpp
 *
 * @author your name (you@domain.com)
 * @brief
 * @version 0.1
 * @date 2023-02-20
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "util.h"

#include <map>
#include <string>
#include <vector>

#include "gpreader.h"
#include "gpwriter.h"

#include "virtual_index.h"

#include "io.h"
#include "io_adv.h"
#include "storage_lister.h"

#include "url.h"

#define DEFAULTTABLESPACE_OID 1663 /* FIXME */

const char *baseYezzeyPath = "/basebackups_005/yezzey/";

std::string getYezzeyExtrenalStorageBucket(const char *host,
                                           const char *bucket) {
  std::string url = "s3://";
  std::string toErase = "https://";
  std::string hostStr = host;
  size_t pos = hostStr.find(toErase);
  if (pos != std::string::npos) {
    hostStr.erase(pos, toErase.length());
  }
  url += hostStr;
  url += "/";
  url += bucket;
  url += "/";

  return url;
}

std::string storage_url_add_options(const std::string &s3path,
                                    const char *config_path) {
  auto ret = s3path;

  ret += " config=";
  ret += config_path;
  ret += " region=us-east-1";

  return ret;
}

relnodeCoord getRelnodeCoordinate(const std::string &fileName) {

  Oid dbOid = 0, relfilenodeOid = 0;
  int64_t blkno = 0;

  auto len = fileName.size();

  for (size_t it = 0; it < len;) {
    if (!isdigit(fileName[it])) {
      ++it;
      continue;
    }
    if (dbOid && relfilenodeOid && blkno) {
      break; // seg num follows
    }
    if (dbOid == 0) {
      while (it < len && isdigit(fileName[it])) {
        dbOid *= 10;
        dbOid += fileName[it++] - '0';
      }
    } else if (relfilenodeOid == 0) {
      while (it < len && isdigit(fileName[it])) {
        relfilenodeOid *= 10;
        relfilenodeOid += fileName[it++] - '0';
      }
    } else if (blkno == 0) {
      while (it < len && isdigit(fileName[it])) {
        blkno *= 10;
        blkno += fileName[it++] - '0';
      }
    }
  }

  return relnodeCoord(dbOid, relfilenodeOid, blkno);
}

void getYezzeyExternalStoragePath(const char *nspname, const char *relname,
                                  const char *host, const char *bucket,
                                  const char *storage_prefix,
                                  const char *filename, int32_t segid,
                                  char **dest) {
  auto prefix =
      getYezzeyRelationUrl(nspname, relname, storage_prefix, filename, segid);
  auto path = getYezzeyExtrenalStorageBucket(host, bucket) + prefix;

  *dest = (char *)malloc(sizeof(char) * path.size());
  strcpy(*dest, path.c_str());
  return;
}

void getYezzeyExternalStoragePathByCoords(
    const char *nspname, const char *relname, const char *host,
    const char *bucket, const char *storage_prefix, Oid dbNode, Oid relNode,
    int32_t segblockno /* segment no*/, int32_t segid, char **dest) {
  auto coords = relnodeCoord({dbNode, relNode, segblockno});
  auto prefix = getYezzeyRelationUrl_internal(nspname, relname, storage_prefix,
                                              coords, segid);
  auto path = getYezzeyExtrenalStorageBucket(host, bucket) + prefix;

  *dest = (char *)malloc(sizeof(char) * path.size());
  strcpy(*dest, path.c_str());
  return;
}

/*
 * fileName is in form 'base=DEFAULTTABLESPACE_OID/<dboid>/<tableoid>.<seg>'
 */

std::vector<int64_t> parseModcounts(const std::string &prefix,
                                    std::string name) {
  std::vector<int64_t> res;
  auto indx = name.find(prefix);
  if (indx == std::string::npos) {
    return res;
  }
  indx += prefix.size();
  auto endindx = name.find("_aoseg", indx);

  size_t prev = 0;

  /* name[endindx] -> not digit */
  /* mc1_D_mc2_D_mc3_D_mc4 */
  for (size_t it = indx; it <= endindx; ++it) {
    if (!isdigit(name[it])) {
      if (prev) {
        res.push_back(prev);
      }
      prev = 0;
      continue;
    }
    prev *= 10;
    prev += name[it] - '0';
  }

  return res;
}

std::string make_yezzey_url(const std::string &prefix, int64_t modcount,
                            XLogRecPtr current_recptr) {
  auto rv = prefix + ("_DY_" + std::to_string(modcount));
  if (current_recptr != InvalidXLogRecPtr) {
    rv += "_xlog_" + std::to_string(current_recptr);
  }
  return rv;
}

/* calc size of external files */
int64_t yezzey_virtual_relation_size(std::shared_ptr<IOadv> adv,
                                     int32_t segid) {
  try {
    auto lister = StorageLister(adv, GpIdentity.segindex);
    int64_t sz = 0;
    for (auto key : lister.reader_->getKeyList().contents) {
      sz +=
          lister.reader_->bucketReader.constructReaderParams(key).getKeySize();
    }
    /* external reader destruct */
    return sz;
  } catch (...) {
    return -1;
  }
}

/* calc total offset of external files */
int64_t yezzey_calc_virtual_relation_size(std::shared_ptr<IOadv> adv,
                                          ssize_t segindx, ssize_t modcount,
                                          const std::string &storage_path) {

#if USE_WALG_BACKUPS
  try {
    auto ioh = YIO(adv, segindx, modcount, storage_path);
    int64_t sz = 0;
    auto buf = std::vector<char>(1 << 20);
    /* fix this */
    for (;;) {
      auto rc = buf.size();
      if (!ioh.io_read(buf.data(), &rc)) {
        break;
      }
      sz += rc;
    }

    ioh.io_close();
    return sz;
  } catch (...) {
    return -1;
  }
#else
  /* TODO: better import logic */
  return 0;
#endif
}
/*XXX: fix cleanup*/

XLogRecPtr yezzeyGetXStorageInsertLsn(void) {
  if (RecoveryInProgress())
    ereport(
        ERROR,
        (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
         errmsg("recovery is in progress"),
         errhint("WAL control functions cannot be executed during recovery.")));

  return GetXLogWriteRecPtr();
}
