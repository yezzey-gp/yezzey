

#include "proxy.h"

#include "meta.h"
#include "unordered_map"
#include "virtual_index.h"

#include "util.h"
#include <cassert>

#include "io.h"
#include "io_adv.h"

#include "url.h"
#include "yezzey_meta.h"

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

  int64 op_start_offset;
  /* unneede, because this offset is equal to total offset and moment of
   * FileClose */
  // int64 op_end_offset;

  int64 virtualSize;
  int64 modcount;

  int64 op_write;

  bool offloaded{false};

  relnodeCoord coord;

  YVirtFD()
      : y_vfd(-1), localTmpVfd(0), handler(nullptr), fileFlags(0), fileMode(0),
        reloid(InvalidOid), offset(0), op_start_offset(0), virtualSize(0),
        modcount(0), op_write(0), offloaded(false) {}

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

#if GP_VERSION_NUM < 70000

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
#endif

#if GP_VERSION_NUM < 70000
int64 yezzey_FileSeek(SMGRFile file, int64 offset, int whence) {
  File actual_fd = YVirtFD_cache[file].y_vfd;
  if (actual_fd == YEZZEY_OFFLOADED_FD) {
    /* TDB: check that offset == max_offset from metadata table */
    YVirtFD_cache[file].offset = offset;
    /* TDB: check sanity of this operation */
    YVirtFD_cache[file].op_start_offset = offset;
    return offset;
  }
  elog(yezzey_ao_log_level,
       "yezzey_FileSeek: file seek with yezzey fd %d offset %ld actual %d",
       file, offset, actual_fd);
  return FileSeek(actual_fd, offset, whence);
}
#endif

#if GP_VERSION_NUM >= 70000
EXTERNC int yezzey_FileSync(SMGRFile file, uint32 wait_event_info)
#else
EXTERNC int yezzey_FileSync(SMGRFile file)
#endif
{
  File actual_fd = YVirtFD_cache[file].y_vfd;
  if (actual_fd == YEZZEY_OFFLOADED_FD) {
    /* s3 always sync ? */
    /* sync tmp buf file here */
    return 0;
  }
  elog(yezzey_ao_log_level, "file sync with fd %d actual %d", file, actual_fd);

#if GP_VERSION_NUM >= 70000
  return FileSync(actual_fd, wait_event_info);
#else
  return FileSync(actual_fd);
#endif
}

#if GP_VERSION_NUM >= 70000
EXTERNC SMGRFile yezzey_AORelOpenSegFile(Oid reloid, const char *nspname,
                                         const char *relname,
                                         const char *fileName, int fileFlags,
                                         int64 modcount)
#else
EXTERNC SMGRFile yezzey_AORelOpenSegFile(Oid reloid, char *nspname,
                                         char *relname, const char *fileName,
                                         int fileFlags, int fileMode,
                                         int64 modcount)
#endif
{
  if (modcount != -1) {
    /* advance modcount to the value it will be after commit */
    ++modcount;
  }

  /* lookup for virtual file desc entry */
  for (SMGRFile yezzey_fd = YEZZEY_NOT_OPENED + 1;; ++yezzey_fd) {
    if (!YVirtFD_cache.count(yezzey_fd)) {
      YVirtFD_cache[yezzey_fd] = YVirtFD();

      YVirtFD &yfd = YVirtFD_cache[yezzey_fd];
      // memset(&YVirtFD_cache[file], 0, sizeof(YVirtFD));
      yfd.filepath = std::string(fileName);
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
          yfd.relname = std::string(relname);
          yfd.nspname = std::string(nspname);
        }
      } else {
        /* nothing*/
      }

      yfd.fileFlags = fileFlags;
#if GP_VERSION_NUM < 70000
      yfd.fileMode = fileMode;
#endif
      yfd.modcount = modcount;
      yfd.reloid = reloid;
      yfd.offloaded = offloaded;

      /* we dont need to interact with s3 while in recovery*/

      if (offloaded) {
        yfd.y_vfd = YEZZEY_OFFLOADED_FD;
        if (!RecoveryInProgress()) {
          auto ioadv = std::make_shared<IOadv>(
              std::string(gpg_engine_path), std::string(gpg_key_id),
              std::string(storage_config), yfd.nspname, yfd.relname,
              std::string(storage_host /*host*/),
              std::string(storage_bucket /*bucket*/),
              std::string(storage_prefix /*prefix*/), 
              std::string(storage_class /* storage_class */),
              "BASE",
              DEFAULTTABLESPACE_OID, yfd.filepath /* coords */,
              reloid /* reloid */, std::string(walg_bin_path),
              std::string(walg_config_path), use_gpg_crypto, yproxy_socket);

          yfd.coord = ioadv->coords_;

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
            auto writer = yfd.handler->writer_;
          }
        }
      } else {
        /* not offloaded */
#if GP_VERSION_NUM >= 70000
        yfd.y_vfd = PathNameOpenFile(yfd.filepath.c_str(), yfd.fileFlags);
#else
        yfd.y_vfd = PathNameOpenFile((char *)yfd.filepath.c_str(),
                                     yfd.fileFlags, yfd.fileMode);
#endif
        if (yfd.y_vfd == -1) {
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
  YVirtFD &yfd = YVirtFD_cache[file];
  if (yfd.y_vfd != -1 && yfd.y_vfd != YEZZEY_OFFLOADED_FD) {
    elog(yezzey_ao_log_level, "file close with %d actual %d", file, yfd.y_vfd);

    FileClose(yfd.y_vfd);
    yfd.y_vfd = -1;
  }

  if (yfd.y_vfd == YEZZEY_OFFLOADED_FD) {
    if (!RecoveryInProgress()) {
      assert(yfd.handler);
      if (!yfd.handler->io_close()) {
        // very bad
        elog(ERROR, "failed to complete external storage interaction: fd %d",
             file);
      } else {
        elog(DEBUG1, "yezzey: complete external storage interaction: fd %d",
             file);
      }
      /* record file only if non-zero bytes was stored */
      if (yfd.op_write) {
        /* insert entry in yezzey index */
        YezzeyUpdateMetadataRelations(
            YezzeyFindAuxIndex(yfd.reloid), yfd.reloid, yfd.coord.filenode,
            yfd.coord.blkno /* blkno*/, yfd.op_start_offset,
            yfd.offset /* io operation finish offset */, yfd.handler->adv_->use_gpg_crypto /* encrypted */,
            0 /* reused */, yfd.modcount,
            yfd.handler->writer_->getInsertionStorageLsn(),
            yfd.handler->writer_->getExternalStoragePath().c_str() /* path ? */,
            yezzey_fqrelname_md5(yfd.nspname, yfd.relname).c_str());
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

#if GP_VERSION_NUM >= 70000
int yezzey_FileWrite(SMGRFile file, char *buffer, int amount, off_t offset,
                     uint32 wait_event_info)
#else
int yezzey_FileWrite(SMGRFile file, char *buffer, int amount)
#endif
{
  YVirtFD &yfd = YVirtFD_cache[file];

#if GP_VERSION_NUM >= 70000
  yfd.op_start_offset = offset;
#endif

  File actual_fd = yfd.y_vfd;
  if (actual_fd == YEZZEY_OFFLOADED_FD) {
    assert(yfd.modcount);

    /* Assert here we are not in crash or regular recovery
     * If yes, simply skip this call as data is already
     * persisted in external storage
     */
    if (RecoveryInProgress()) {
      /* Should we return $amount or min (virtualSize - currentLogicalEof,
       * amount) ? */
      return amount;
    }

#ifdef CACHE_LOCAL_WRITES_FEATURE
/* CACHE_LOCAL_WRITES_FEATURE to do*/
#endif
    size_t rc = amount;
    if (!yfd.handler->io_write(buffer, &rc)) {
      elog(WARNING, "failed to write to external storage");
      return -1;
    }
    elog(yezzey_ao_log_level,
         "yezzey_FileWrite: write %d bytes, %ld transfered, yezzey fd %d",
         amount, rc, file);
    yfd.offset += rc;
    yfd.op_write += rc;
    return rc;
  }

#if GP_VERSION_NUM >= 70000
  size_t rc = FileWrite(actual_fd, buffer, amount, offset, wait_event_info);
#else
  size_t rc = FileWrite(actual_fd, buffer, amount);
#endif
  if (rc > 0) {
    yfd.offset += rc;
    yfd.op_write += rc;
  }
  return rc;
}

#if GP_VERSION_NUM >= 70000
int yezzey_FileRead(SMGRFile file, char *buffer, int amount, off_t offset,
                    uint32 wait_event_info) {
#else
int yezzey_FileRead(SMGRFile file, char *buffer, int amount) {
#endif

  size_t curr = amount;
  YVirtFD &yfd = YVirtFD_cache[file];

#if GP_VERSION_NUM >= 70000
  yfd.op_start_offset = offset;
#endif

  File actual_fd = yfd.y_vfd;
  if (actual_fd == YEZZEY_OFFLOADED_FD) {
    if (yfd.handler->reader_empty()) {
      if (yfd.localTmpVfd <= 0) {
        return 0;
      }
#ifdef DISKCACHE
/* CACHE_LOCAL_WRITES_FEATURE to do*/
#endif
    } else {
      if (!yfd.handler->io_read(buffer, &curr)) {
        elog(yezzey_ao_log_level,
             "yezzey_FileRead: problem while direct read from s3 read with %d "
             "curr: %ld",
             file, curr);
        return -1;
      }
#ifdef DISKCACHE
/* CACHE_LOCAL_WRITES_FEATURE to do*/
#endif
    }

    yfd.offset += curr;

    elog(yezzey_ao_log_level,
         "yezzey_FileRead: file read with %d, actual %d, amount %d real %ld",
         file, actual_fd, amount, curr);
    return curr;
  }

#if GP_VERSION_NUM >= 70000
  return FileRead(actual_fd, buffer, amount, offset, wait_event_info);
#else
  return FileRead(actual_fd, buffer, amount);
#endif
}

#if GP_VERSION_NUM >= 70000
EXTERNC int yezzey_FileTruncate(SMGRFile yezzey_fd, int64 offset,
                                uint32 wait_event_info)
#else
EXTERNC int yezzey_FileTruncate(SMGRFile yezzey_fd, int64 offset)
#endif
{
  YVirtFD &yfd = YVirtFD_cache[yezzey_fd];
  File actual_fd = yfd.y_vfd;
  if (actual_fd == YEZZEY_OFFLOADED_FD) {
    /* Leave external storage file untouched
     * We may need them for point-in-time recovery
     * later. We will face no issues with writing to
     * this AO/AOCS table later, because truncate operation
     * with AO/AOCS table changes relfilenode OID of realtion
     * segments.
     */

    if (!RecoveryInProgress()) {
      assert(yfd.handler);

      /* if truncatetoeof, do nothing */
      /* we need addintinal checks that offset == virtual_size */
      if (offset) {
        return 0;
      }

      /* Do it only on QE? */
      if (Gp_role == GP_ROLE_EXECUTE) {
        (void)emptyYezzeyIndexBlkno(YezzeyFindAuxIndex(yfd.reloid), yfd.reloid,
                                    yfd.handler->adv_->coords_.filenode,
                                    yfd.handler->adv_->coords_.blkno);
      }
    }
    return 0;
  }

#if GP_VERSION_NUM >= 70000
  return FileTruncate(actual_fd, offset, wait_event_info);
#else
  return FileTruncate(actual_fd, offset);
#endif
}

#if GP_VERSION_NUM >= 70000
EXTERNC int yezzey_FileDiskSize(SMGRFile file) { return FileDiskSize(file); }
#endif