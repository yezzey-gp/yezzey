

#include "proxy.h"

#include "meta.h"
#include "unordered_map"
#include "virtual_index.h"

#include "util.h"
#include <cassert>

#include "io.h"
#include "io_adv.h"

typedef struct YVirtFD {
  int y_vfd; /* Either YEZZEY_* preserved fd or pg internal fd >= 9 */

  int localTmpVfd; /* for writing cache files */

  /* s3-related params */
  std::string filepath;
  std::string nspname;
  std::string relname;

  std::unique_ptr<YIO> handler;

  /* gpg-related params */

  /* open args */
  int fileFlags;
  int fileMode;

  int64 reloid;

  int64 offset;
  int64 virtualSize;
  int64 modcount;

  bool offloaded{false};

  YVirtFD()
      : y_vfd(-1), localTmpVfd(0), handler(nullptr), fileFlags(0), fileMode(0),
        reloid(InvalidOid), offset(0), virtualSize(0), modcount(0),
        offloaded(false) {}

  YVirtFD &operator=(YVirtFD &&vfd) {
    handler = std::move(vfd.handler);
    return *this;
  }
} YVirtFD;

#define YEZZEY_VANANT_VFD 0 /* not used */
#define YEZZEY_OFFLOADED_FD -2
#define YEZZEY_NOT_OPENED 1
#define YEZZEY_OPENED 2
#define YEZZEY_MIN_VFD 3

// be unordered map
std::unordered_map<SMGRFile, YVirtFD> YVirtFD_cache;

/* lazy allocate external storage connections */
int readprepare(std::shared_ptr<IOadv> ioadv, SMGRFile yezzey_fd) {
#ifdef CACHE_LOCAL_WRITES_FEATURE
/* CACHE_LOCAL_WRITES_FEATURE to do*/
#endif
  try {
    YVirtFD_cache[yezzey_fd].handler =
        make_unique<YIO>(ioadv, GpIdentity.segindex);
  } catch (...) {
    return -1;
  }

#ifdef CACHE_LOCAL_WRITES_FEATURE
/* CACHE_LOCAL_WRITES_FEATURE to do*/
#endif
  return 0;
}

int writeprepare(std::shared_ptr<IOadv> ioadv, int64_t modcount,
                 SMGRFile yezzey_fd) {
  try {
    YVirtFD_cache[yezzey_fd].handler =
        make_unique<YIO>(ioadv, GpIdentity.segindex, modcount, "");
  } catch (...) {
    return -1;
  }

  elog(yezzey_ao_log_level, "prepared writer handle for modcount %ld",
       modcount);

  //   Assert(YVirtFD_cache[file].handler.writer_ != NULL);

#ifdef CACHE_LOCAL_WRITES_FEATURE
/* CACHE_LOCAL_WRITES_FEATURE to do*/
#endif

  return 0;
}

int64 yezzey_NonVirtualCurSeek(SMGRFile file) {
  if (YVirtFD_cache[file].y_vfd == YEZZEY_OFFLOADED_FD) {
    elog(yezzey_ao_log_level,
         "yezzey_NonVirtualCurSeek: non virt file seek with yezzey fd %d and "
         "actual file in external storage, responding %ld",
         file, YVirtFD_cache[file].offset);
    return YVirtFD_cache[file].offset;
  }
  elog(yezzey_ao_log_level,
       "yezzey_NonVirtualCurSeek: non virt file seek with yezzey fd %d and "
       "actual %d",
       file, YVirtFD_cache[file].y_vfd);
  return FileNonVirtualCurSeek(YVirtFD_cache[file].y_vfd);
}

int64 yezzey_FileSeek(SMGRFile file, int64 offset, int whence) {
  File actual_fd = YVirtFD_cache[file].y_vfd;
  if (actual_fd == YEZZEY_OFFLOADED_FD) {
    // what?
    YVirtFD_cache[file].offset = offset;
    return offset;
  }
  elog(yezzey_ao_log_level,
       "yezzey_FileSeek: file seek with yezzey fd %d offset %ld actual %d",
       file, offset, actual_fd);
  return FileSeek(actual_fd, offset, whence);
}

int yezzey_FileSync(SMGRFile file) {
  File actual_fd = YVirtFD_cache[file].y_vfd;
  if (actual_fd == YEZZEY_OFFLOADED_FD) {
    /* s3 always sync ? */
    /* sync tmp buf file here */
    return 0;
  }
  elog(yezzey_ao_log_level, "file sync with fd %d actual %d", file, actual_fd);
  return FileSync(actual_fd);
}

SMGRFile yezzey_AORelOpenSegFile(Oid reloid, char *nspname, char *relname,
                                 FileName fileName, int fileFlags, int fileMode,
                                 int64 modcount) {
  elog(yezzey_ao_log_level,
       "yezzey_AORelOpenSegFile: path name open file %s with modcount %ld",
       fileName, modcount);

  if (modcount != -1) {
    /* advance modcount to the value it will be after commit */
    ++modcount;
  }

  /* lookup for virtual file desc entry */
  for (SMGRFile yezzey_fd = YEZZEY_NOT_OPENED + 1;; ++yezzey_fd) {
    if (!YVirtFD_cache.count(yezzey_fd)) {
      YVirtFD_cache[yezzey_fd] = YVirtFD();
      // memset(&YVirtFD_cache[file], 0, sizeof(YVirtFD));
      YVirtFD_cache[yezzey_fd].filepath = std::string(fileName);
      std::string spcPref = "";
      if (strlen(fileName) >= 6) {
        spcPref = std::string(fileName, 6);
      }
      bool offloaded = false;
      if (spcPref == "yezzey") {
        offloaded = true;
        if (relname == NULL || nspname == NULL) {
          /* Should be possible only in recovery */
          /* or changing tablespace */
          /* but we forbit change tablespace to yezzey not using yezzey sql api
           */
          Assert(RecoveryInProgress());
        } else {
          YVirtFD_cache[yezzey_fd].relname = std::string(relname);
          YVirtFD_cache[yezzey_fd].nspname = std::string(nspname);
        }
      } else {
        /* nothing*/
      }

      YVirtFD_cache[yezzey_fd].fileFlags = fileFlags;
      YVirtFD_cache[yezzey_fd].fileMode = fileMode;
      YVirtFD_cache[yezzey_fd].modcount = modcount;
      YVirtFD_cache[yezzey_fd].reloid = reloid;

      YVirtFD_cache[yezzey_fd].offloaded = offloaded;

      /* we dont need to interact with s3 while in recovery*/

      if (offloaded) {
        YVirtFD_cache[yezzey_fd].y_vfd = YEZZEY_OFFLOADED_FD;
        if (!RecoveryInProgress()) {
          auto ioadv = std::make_shared<IOadv>(
              std::string(gpg_engine_path), std::string(gpg_key_id),
              std::string(storage_config), YVirtFD_cache[yezzey_fd].nspname,
              YVirtFD_cache[yezzey_fd].relname,
              std::string(storage_host /*host*/),
              std::string(storage_bucket /*bucket*/),
              std::string(storage_prefix /*prefix*/),
              YVirtFD_cache[yezzey_fd].filepath /* coords */,
              reloid /* reloid */, std::string(walg_bin_path),
              std::string(walg_config_path), use_gpg_crypto);

          /*
           * Ignore fileFlags here
           * yezzey is able to write only if modcount is correctly set
           */
          if (modcount == -1) {
            /* allocate handle struct */
            if (readprepare(ioadv, yezzey_fd) == -1) {
              YVirtFD_cache.erase(yezzey_fd);
              return -1;
            }
          } else {
            /* allocate handle struct */
            if (writeprepare(ioadv, modcount, yezzey_fd) == -1) {
              YVirtFD_cache.erase(yezzey_fd);
              return -1;
            }
            /* insert entry in yezzey index */
            YezzeyVirtualIndexInsert(
                YezzeyFindAuxIndex(YVirtFD_cache[yezzey_fd].reloid),
                std::get<2>(ioadv->coords_) /* segindex*/, modcount,
                InvalidXLogRecPtr);
          }
        }
      } else {
        /* not offloaded */
        YVirtFD_cache[yezzey_fd].y_vfd = PathNameOpenFile(
            (FileName)YVirtFD_cache[yezzey_fd].filepath.c_str(),
            YVirtFD_cache[yezzey_fd].fileFlags,
            YVirtFD_cache[yezzey_fd].fileMode);

        if (YVirtFD_cache[yezzey_fd].y_vfd == -1) {
          YVirtFD_cache.erase(yezzey_fd);
          return -1;
        }
      }

      return yezzey_fd;
    }
  }
}

void yezzey_FileClose(SMGRFile file) {
  if (!YVirtFD_cache.count(file)) {
    return;
  }
  if (YVirtFD_cache[file].y_vfd != -1 &&
      YVirtFD_cache[file].y_vfd != YEZZEY_OFFLOADED_FD) {

    elog(yezzey_ao_log_level, "file close with %d actual %d", file,
         YVirtFD_cache[file].y_vfd);

    FileClose(YVirtFD_cache[file].y_vfd);
    YVirtFD_cache[file].y_vfd = -1;
  }

  if (YVirtFD_cache[file].y_vfd == YEZZEY_OFFLOADED_FD) {
    if (!RecoveryInProgress()) {
      assert(YVirtFD_cache[file].handler);
      if (!YVirtFD_cache[file].handler->io_close()) {
        // very bad
        elog(ERROR, "failed to complete external storage interfacrtion: fd %d",
             file);
      }
    } else {

      /* not need to do anything */
    }
  }

#ifdef DISKCACHE
/* CACHE_LOCAL_WRITES_FEATURE to do*/
#endif
  YVirtFD_cache.erase(file);
}

#define ALLOW_MODIFY_EXTERNAL_TABLE

int yezzey_FileWrite(SMGRFile file, char *buffer, int amount) {
  File actual_fd = YVirtFD_cache[file].y_vfd;
  if (actual_fd == YEZZEY_OFFLOADED_FD) {
    assert(YVirtFD_cache[file].modcount);

    /* Assert here we are not in crash or regular recovery
     * If yes, simply skip this call as data is already
     * persisted in external storage
     */
    if (RecoveryInProgress()) {
      /* Should we return $amount or min (virtualSize - currentLogicalEof,
       * amount) ? */
      return amount;
    }

#ifdef ALLOW_MODIFY_EXTERNAL_TABLE
#ifdef CACHE_LOCAL_WRITES_FEATURE
/* CACHE_LOCAL_WRITES_FEATURE to do*/
#endif
#else
    elog(ERROR, "external table modifications are not supported yet");
#endif
    size_t rc = amount;
    if (!YVirtFD_cache[file].handler->io_write(buffer, &rc)) {
      elog(WARNING, "failed to write to external storage");
      return -1;
    }
    elog(yezzey_ao_log_level,
         "yezzey_FileWrite: write %d bytes, %ld transfered, yezzey fd %d",
         amount, rc, file);
    YVirtFD_cache[file].offset += rc;
    return rc;
  }
  elog(yezzey_ao_log_level, "file write with %d, actual %d", file, actual_fd);
  size_t rc = FileWrite(actual_fd, buffer, amount);
  if (rc > 0) {
    YVirtFD_cache[file].offset += rc;
  }
  return rc;
}

int yezzey_FileRead(SMGRFile file, char *buffer, int amount) {
  size_t curr = amount;
  File actual_fd = YVirtFD_cache[file].y_vfd;
  if (actual_fd == YEZZEY_OFFLOADED_FD) {
    if (YVirtFD_cache[file].handler->reader_empty()) {
      if (YVirtFD_cache[file].localTmpVfd <= 0) {
        return 0;
      }
#ifdef DISKCACHE
/* CACHE_LOCAL_WRITES_FEATURE to do*/
#endif
    } else {
      if (!YVirtFD_cache[file].handler->io_read(buffer, &curr)) {
        elog(yezzey_ao_log_level,
             "problem while direct read from s3 read with %d curr: %ld", file,
             curr);
        return -1;
      }
#ifdef DISKCACHE
/* CACHE_LOCAL_WRITES_FEATURE to do*/
#endif
    }

    YVirtFD_cache[file].offset += curr;

    elog(yezzey_ao_log_level,
         "file read with %d, actual %d, amount %d real %ld", file, actual_fd,
         amount, curr);
    return curr;
  }
  return FileRead(actual_fd, buffer, amount);
}

int yezzey_FileTruncate(SMGRFile yezzey_fd, int64 offset) {
  File actual_fd = YVirtFD_cache[yezzey_fd].y_vfd;
  if (actual_fd == YEZZEY_OFFLOADED_FD) {
    /* Leave external storage file untouched
     * We may need them for point-in-time recovery
     * later. We will face no issues with writing to
     * this AO/AOCS table later, because truncate operation
     * with AO/AOCS table changes relfilenode OID of realtion
     * segments.
     */

    if (!RecoveryInProgress()) {
      assert(YVirtFD_cache[yezzey_fd].handler);

      /* Do it only on QE? */
      if (Gp_role == GP_ROLE_EXECUTE) {
        (void)emptyYezzeyIndexBlkno(
            YezzeyFindAuxIndex(YVirtFD_cache[yezzey_fd].reloid),
            std::get<2>(YVirtFD_cache[yezzey_fd].handler->adv_->coords_));
      }
    }
    return 0;
  }
  return FileTruncate(actual_fd, offset);
}
