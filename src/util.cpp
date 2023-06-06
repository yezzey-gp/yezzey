
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

#include "external_reader.h"
#include "io.h"
#include "io_adv.h"

#define DEFAULTTABLESPACE_OID 1663 /* FIXME */

const char *basebackupsPath = "/basebackups_005/aosegments/";

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

  int64_t dboid = 0, tableoid = 0;
  int64_t relation_segment = 0;

  auto len = fileName.size();

  for (size_t it = 0; it < len;) {
    if (!isdigit(fileName[it])) {
      ++it;
      continue;
    }
    if (dboid && tableoid && relation_segment) {
      break; // seg num follows
    }
    if (dboid == 0) {
      while (it < len && isdigit(fileName[it])) {
        dboid *= 10;
        dboid += fileName[it++] - '0';
      }
    } else if (tableoid == 0) {
      while (it < len && isdigit(fileName[it])) {
        tableoid *= 10;
        tableoid += fileName[it++] - '0';
      }
    } else if (relation_segment == 0) {
      while (it < len && isdigit(fileName[it])) {
        relation_segment *= 10;
        relation_segment += fileName[it++] - '0';
      }
    }
  }

  return {dboid, tableoid, relation_segment};
}

std::string
getYezzeyRelationUrl_internal(const std::string &nspname,
                              const std::string &relname,
                              const std::string &external_storage_prefix,
                              relnodeCoord coords, int32_t segid) {

  std::string url = external_storage_prefix + "/seg" + std::to_string(segid) +
                    basebackupsPath;

  int64_t dboid, tableoid, relation_segment;
  dboid = std::get<0>(coords);
  tableoid = std::get<1>(coords);
  relation_segment = std::get<2>(coords);

  unsigned char md[MD5_DIGEST_LENGTH];

  url +=
      std::to_string(DEFAULTTABLESPACE_OID) + "_" + std::to_string(dboid) + "_";

  std::string full_name = nspname + "." + relname;
  /* compute AO/AOCS relation name, just like walg does*/
  (void)MD5((const unsigned char *)full_name.c_str(), full_name.size(), md);

  for (size_t i = 0; i < MD5_DIGEST_LENGTH; ++i) {
    char chunk[3];
    (void)sprintf(chunk, "%.2x", md[i]);
    url += chunk[0];
    url += chunk[1];
  }

  url += "_" + std::to_string(tableoid) + "_" +
         std::to_string(relation_segment) + "_";

  return url;
}

/// @brief
/// @param nspname
/// @param relname
/// @param external_storage_prefix
/// @param fileName
/// @param segid greenplum executor segment id
/// @return
std::string getYezzeyRelationUrl(const char *nspname, const char *relname,
                                 const char *external_storage_prefix,
                                 const char *fileName, int32_t segid) {
  std::string filename_str = std::string(fileName);
  auto coords = getRelnodeCoordinate(filename_str);

  return getYezzeyRelationUrl_internal(nspname, relname,
                                       external_storage_prefix, coords, segid);
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

std::string make_yezzey_url(const std::string &prefix,
                            const std::vector<int64_t> &modcounts) {
  auto ret = prefix;

  for (size_t i = 0; i < modcounts.size(); ++i) {
    ret += std::to_string(modcounts[i]);
    if (i + 1 != modcounts.size()) {
      ret += "_D_";
    }
  }

  ret += "_aoseg_yezzey";

  return ret;
}

/* calc size of external files */
int64_t yezzey_virtual_relation_size(std::shared_ptr<IOadv> adv,
                                     int32_t segid) {
  try {
    auto r = ExternalReader(adv, segid);

    int64_t sz = 0;
    for (auto key : r.reader_->getKeyList().contents) {
      sz += r.reader_->bucketReader.constructReaderParams(key).getKeySize();
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
}
/*XXX: fix cleanup*/
