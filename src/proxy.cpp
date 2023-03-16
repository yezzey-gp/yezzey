

#include "proxy.h"

#include "unordered_map"

#include "util.h"

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

  int64 offset;
  int64 virtualSize;
  int64 modcount;

  YVirtFD()
      : y_vfd(0), localTmpVfd(0), handler(nullptr), fileFlags(0), fileMode(0),
        offset(0), virtualSize(0), modcount(0) {}

  YVirtFD &operator=(YVirtFD &&vfd) {
    handler = std::move(vfd.handler);
    return *this;
  }
} YVirtFD;

#define YEZZEY_VANANT_VFD 0 /* not used */
#define YEZZEY_NOT_OPENED 1
#define YEZZEY_OPENED 2
#define YEZZEY_MIN_VFD 3

File virtualEnsure(SMGRFile file);

// be unordered map
std::unordered_map<SMGRFile, YVirtFD> YVirtFD_cache;

File s3ext = -2;

/* lazy allocate external storage connections */
int readprepare(SMGRFile file) {
#ifdef CACHE_LOCAL_WRITES_FEATURE
/* CACHE_LOCAL_WRITES_FEATURE to do*/
#endif

  //   (void)createReaderHandle(YVirtFD_cache[file].handler,
  //   GpIdentity.segindex);

  //   Assert(YVirtFD_cache[file].handler.read_ptr != NULL);

#ifdef CACHE_LOCAL_WRITES_FEATURE
/* CACHE_LOCAL_WRITES_FEATURE to do*/
#endif
  return 0;
}

int writeprepare(SMGRFile file) {

  /* should be called once*/
  if (readprepare(file) == -1) {
    return -1;
  }

  //   (void)createWriterHandle(YVirtFD_cache[file].handler,
  //   GpIdentity.segindex,
  //                            1 /*because modcount will increase on write*/ +
  //                                YVirtFD_cache[file].modcount);

  elog(yezzey_ao_log_level, "prepared writer handle for modcount %ld",
       YVirtFD_cache[file].modcount);

  //   Assert(YVirtFD_cache[file].handler.writer_ != NULL);

#ifdef CACHE_LOCAL_WRITES_FEATURE
/* CACHE_LOCAL_WRITES_FEATURE to do*/
#endif

  return 0;
}

File virtualEnsure(SMGRFile file) {
  File internal_vfd;
  if (YVirtFD_cache[file].y_vfd == YEZZEY_VANANT_VFD) {
    elog(ERROR, "attempt to ensure locality of not opened file");
  }
  if (YVirtFD_cache[file].y_vfd == YEZZEY_NOT_OPENED) {
    // not opened yet
    // use std::filesystem::exists here
    if (!ensureFilepathLocal(YVirtFD_cache[file].filepath)) {
      // do s3 read
      return s3ext;
    }

    /* Do we need this? */
    // use postgresql fd-cache subsystem
    internal_vfd = PathNameOpenFile(
        (FileName)YVirtFD_cache[file].filepath.c_str(),
        YVirtFD_cache[file].fileFlags, YVirtFD_cache[file].fileMode);

    elog(yezzey_ao_log_level,
         "virtualEnsure: yezzey virtual file descriptor for file %s become %d",
         YVirtFD_cache[file].filepath.c_str(), internal_vfd);

    if (internal_vfd == -1) {
      // error
      elog(ERROR, "virtualEnsure: failed to proxy open file %s for fd %d",
           YVirtFD_cache[file].filepath.c_str(), file);
    }
    elog(yezzey_ao_log_level, "y vfd become %d", internal_vfd);

    YVirtFD_cache[file].y_vfd = internal_vfd; // -1 is ok

    elog(yezzey_ao_log_level,
         "virtualEnsure: file %s yezzey descriptor become %d",
         YVirtFD_cache[file].filepath.c_str(), file);
    /* allocate handle struct */
  }

  return YVirtFD_cache[file].y_vfd;
}

int64 yezzey_NonVirtualCurSeek(SMGRFile file) {
  File actual_fd = virtualEnsure(file);
  if (actual_fd == s3ext) {
    elog(yezzey_ao_log_level,
         "yezzey_NonVirtualCurSeek: non virt file seek with yezzey fd %d and "
         "actual file in external storage, responding %ld",
         file, YVirtFD_cache[file].offset);
    return YVirtFD_cache[file].offset;
  }
  elog(yezzey_ao_log_level,
       "yezzey_NonVirtualCurSeek: non virt file seek with yezzey fd %d and "
       "actual %d",
       file, actual_fd);
  return FileNonVirtualCurSeek(actual_fd);
}

int64 yezzey_FileSeek(SMGRFile file, int64 offset, int whence) {
  File actual_fd = virtualEnsure(file);
  if (actual_fd == s3ext) {
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
  File actual_fd = virtualEnsure(file);
  if (actual_fd == s3ext) {
    /* s3 always sync ? */
    /* sync tmp buf file here */
    return 0;
  }
  elog(yezzey_ao_log_level, "file sync with fd %d actual %d", file, actual_fd);
  return FileSync(actual_fd);
}

SMGRFile yezzey_AORelOpenSegFile(char *nspname, char *relname,
                                 FileName fileName, int fileFlags, int fileMode,
                                 int64 modcount) {
  elog(yezzey_ao_log_level,
       "yezzey_AORelOpenSegFile: path name open file %s with modcount %ld",
       fileName, modcount);

  if (modcount != -1) {
    /* advance modcount to the valu it will be after commit */
    ++modcount;
  }

  /* lookup for virtual file desc entry */
  for (SMGRFile yezzey_fd = YEZZEY_NOT_OPENED + 1;; ++yezzey_fd) {
    if (!YVirtFD_cache.count(yezzey_fd)) {
      YVirtFD_cache[yezzey_fd] = YVirtFD();
      // memset(&YVirtFD_cache[file], 0, sizeof(YVirtFD));
      YVirtFD_cache[yezzey_fd].filepath = std::string(fileName);
      if (relname == NULL) {
        /* Should be possible only in recovery */
        Assert(RecoveryInProgress());
      } else {
        YVirtFD_cache[yezzey_fd].relname = std::string(relname);
      }
      if (nspname == NULL) {
        /* Should be possible only in recovery */
        Assert(RecoveryInProgress());
      } else {
        YVirtFD_cache[yezzey_fd].nspname = std::string(nspname);
      }
      YVirtFD_cache[yezzey_fd].fileFlags = fileFlags;
      YVirtFD_cache[yezzey_fd].fileMode = fileMode;
      YVirtFD_cache[yezzey_fd].modcount = modcount;

      YVirtFD_cache[yezzey_fd].y_vfd = YEZZEY_NOT_OPENED;

      auto ioadv = std::make_shared<IOadv>(
          std::string(gpg_engine_path), std::string(gpg_key_id),
          std::string(storage_config), YVirtFD_cache[yezzey_fd].nspname,
          YVirtFD_cache[yezzey_fd].relname, std::string(storage_host /*host*/),
          std::string(storage_bucket /*bucket*/),
          std::string(storage_prefix /*prefix*/),
          YVirtFD_cache[yezzey_fd].filepath, std::string(walg_bin_path),
           std::string(walg_config_path),
          use_gpg_crypto);
      /* we dont need to interact with s3 while in recovery*/

      YVirtFD_cache[yezzey_fd].handler =
          std::make_unique<YIO>(ioadv, GpIdentity.segindex, modcount, "");

      if (RecoveryInProgress()) {
        /* replicae */
        return yezzey_fd;
      } else {
        /* primary */
        if (!ensureFilepathLocal(YVirtFD_cache[yezzey_fd].filepath)) {
          switch (fileFlags) {
          case O_WRONLY:
            /* allocate handle struct */
            if (writeprepare(yezzey_fd) == -1) {
              return -1;
            }
            break;
          case O_RDONLY:
            /* allocate handle struct */
            if (readprepare(yezzey_fd) == -1) {
              return -1;
            }
            break;
          case O_RDWR:
            if (writeprepare(yezzey_fd) == -1) {
              return -1;
            }
            break;
          default:
            break;
            /* raise error */
          }
          // do s3 read
        }
        return yezzey_fd;
      }
    }
  }
  /* no match*/
  return -1;
}

void yezzey_FileClose(SMGRFile file) {
  File actual_fd = virtualEnsure(file);
  elog(yezzey_ao_log_level, "file close with %d actual %d", file, actual_fd);
  if (actual_fd == s3ext) {
    if (!YVirtFD_cache[file].handler->io_close()) {
      // very bad

      elog(ERROR, "failed to complete external storage interfacrtion: fd %d",
           file);
    }
  } else {
    FileClose(actual_fd);
  }

#ifdef DISKCACHE
/* CACHE_LOCAL_WRITES_FEATURE to do*/
#endif
  YVirtFD_cache.erase(file);
}

#define ALLOW_MODIFY_EXTERNAL_TABLE

int yezzey_FileWrite(SMGRFile file, char *buffer, int amount) {
  File actual_fd = virtualEnsure(file);
  if (actual_fd == s3ext) {

    /* Assert here we are not in crash or regular recovery
     * If yes, simply skip this call as data is already
     * persisted in external storage
     */
    if (RecoveryInProgress()) {
      /* Should we return $amount or min (virtualSize - currentLogicalEof,
       * amount) ? */
      return amount;
    }

    // if (YVirtFD_cache[file].handler.write_ptr == NULL) {
    //   elog(yezzey_ao_log_level,
    //        "read from external storage while read handler uninitialized");
    //   return -1;
    // }
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
  File actual_fd = virtualEnsure(file);
  size_t curr = amount;

  if (actual_fd == s3ext) {
    // if (YVirtFD_cache[file].handler.read_ptr == NULL) {
    //   elog(yezzey_ao_log_level,
    //        "read from external storage while read handler uninitialized");
    //   return -1;
    // }
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

int yezzey_FileTruncate(SMGRFile file, int offset) {
  File actual_fd = virtualEnsure(file);
  if (actual_fd == s3ext) {
    /* Leave external storage file untouched
     * We may need them for point-in-time recovery
     * later. We will face no issues with writing to
     * this AO/AOCS table later, because truncate operation
     * with AO/AOCS table changes relfilenode OID of realtion
     * segments.
     */
    return 0;
  }
  return FileTruncate(actual_fd, offset);
}
