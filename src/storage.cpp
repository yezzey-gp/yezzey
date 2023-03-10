

#include "storage.h"
#include "util.h"
#include <filesystem>

#ifdef __cplusplus
extern "C" {
#endif

#include "pgstat.h"
#include "utils/builtins.h"

#include "access/aosegfiles.h"
#include "access/htup_details.h"
#include "utils/tqual.h"

#include "catalog/pg_namespace.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "storage/smgr.h"
#include "utils/catcache.h"
#include "utils/syscache.h"

// For GpIdentity
#include "cdb/cdbvars.h"

#ifdef __cplusplus
}
#endif

#include "io.h"

int offloadFileToExternalStorage(const std::string &spname,
                                 const std::string &relname,
                                 const std::string &localPath, int64 modcount,
                                 int64 logicalEof,
                                 const std::string &xternal_storage_path);

int offloadRelationSegmentPath(const std::string &nspname,
                               const std::string &relname,
                               const std::string &localpath, int64 modcount,
                               int64 logicalEof,
                               const std::string &external_storage_path);

int yezzey_log_level = INFO;
int yezzey_ao_log_level = INFO;

/*
 * This function used by AO-related realtion functions
 */
bool ensureFilepathLocal(const std::string &filepath) {
  const std::filesystem::path path{filepath};
  return std::filesystem::exists(path);
}

int offloadFileToExternalStorage(const std::string &nspname,
                                 const std::string &relname,
                                 const std::string &localPath, int64 modcount,
                                 int64 logicalEof,
                                 const std::string &external_storage_path) {
  int rc;
  int tot;
  size_t chunkSize;
  File vfd;
  int64 curr_read_chunk;
  int64 progress;
  int64 virtual_size;

  chunkSize = 1 << 20;
  std::vector<char> buffer(chunkSize);
  vfd = PathNameOpenFile((FileName)localPath.c_str(), O_RDONLY, 0600);
  if (vfd <= 0) {
    elog(ERROR,
         "yezzey: failed to open %s file to transfer to external storage",
         localPath.c_str());
  }
  progress = 0;

  auto ioadv = std::make_shared<IOadv>(
      gpg_engine_path, gpg_key_id, storage_config, nspname, relname,
      storage_host /* host */, storage_bucket /*bucket*/,
      storage_prefix /*prefix*/, localPath, use_gpg_crypto);

  auto iohandler =
      YIO(ioadv, GpIdentity.segindex, modcount, external_storage_path);

  /* Create external storage reader handle to calculate total external files
   * size. this is needed to skip offloading of data already present in external
   * storage.
   */

  virtual_size = yezzey_calc_virtual_relation_size(
      ioadv, GpIdentity.segindex, modcount, external_storage_path);

  elog(WARNING, "yezzey: relation virtaul size calculated: %ld", virtual_size);
  progress = virtual_size;
  FileSeek(vfd, progress, SEEK_SET);
  rc = 0;

  while (progress < logicalEof) {
    curr_read_chunk = chunkSize;
    if (progress + curr_read_chunk > logicalEof) {
      /* should not read beyond logical eof */
      curr_read_chunk = logicalEof - progress;
    }
    /* code */
    rc = FileRead(vfd, buffer.data(), curr_read_chunk);
    if (rc < 0) {
      return rc;
    }
    if (rc == 0)
      continue;

    tot = 0;
    char *bptr = buffer.data();

    while (tot < rc) {
      size_t currptrtot = rc - tot;
      if (!iohandler.io_write(bptr, &currptrtot)) {
        return -1;
      }

      tot += currptrtot;
      bptr += currptrtot;
    }

    progress += rc;
  }

  if (!iohandler.io_close()) {
    elog(ERROR, "yezzey: failed to complete %s offloading", localPath.c_str());
  }

  FileClose(vfd);
  return rc;
}

bool ensureFileLocal(RelFileNode rnode, BackendId backend, ForkNumber forkNum,
                     BlockNumber blkno) {
  /* MDB-19689: do not consult catalog */

  elog(yezzey_log_level, "ensuring %d is local", rnode.relNode);
  return true;
  StringInfoData path;
  bool result = true;
  initStringInfo(&path);

  // result = ensureFilepathLocal(path.data);

  pfree(path.data);
  return result;
}

int removeLocalFile(const char *localPath) {
  int rc = remove(localPath);
  elog(yezzey_ao_log_level,
       "[YEZZEY_SMGR_BG] remove local file \"%s\", result: %d", localPath, rc);
  return rc;
}

int offloadRelationSegmentPath(const std::string &nspname,
                               const std::string &relname,
                               const std::string &localpath, int64 modcount,
                               int64 logicalEof,
                               const std::string &external_storage_path) {
  if (!ensureFilepathLocal(localpath)) {
    // nothing to do
    return 0;
  }
  return offloadFileToExternalStorage(nspname, relname, localpath, modcount,
                                      logicalEof, external_storage_path);
}

std::string getlocalpath(Oid dbnode, Oid relNode, int segno) {
  std::string local_path;
  if (segno == 0) {
    local_path =
        "base/" + std::to_string(dbnode) + "/" + std::to_string(relNode);
  } else {
    local_path = "base/" + std::to_string(dbnode) + "/" +
                 std::to_string(relNode) + "." + std::to_string(segno);
  }
  return local_path;
}

int offloadRelationSegment(Relation aorel, RelFileNode rnode, int segno,
                           int64 modcount, int64 logicalEof,
                           bool remove_locally,
                           const char *external_storage_path) {
  int rc;
  int64_t virtual_sz;
  HeapTuple tp;

  auto local_path = getlocalpath(rnode.dbNode, rnode.relNode, segno);
  elog(yezzey_ao_log_level, "contructed path %s", local_path.c_str());

  /* xlog goes first */
  // xlog_smgr_local_truncate(rnode, MAIN_FORKNUM, 'a');

  tp = SearchSysCache1(NAMESPACEOID,
                       ObjectIdGetDatum(aorel->rd_rel->relnamespace));

  if (!HeapTupleIsValid(tp)) {
    elog(ERROR, "yezzey: failed to get namescape name of relation %s",
         aorel->rd_rel->relname.data);
  }

  Form_pg_namespace nsptup = (Form_pg_namespace)GETSTRUCT(tp);
  auto nspname = std::string(NameStr(nsptup->nspname));
  auto relname = std::string(aorel->rd_rel->relname.data);
  auto storage_path =
      !external_storage_path ? "" : std::string(external_storage_path);
  ReleaseSysCache(tp);

  if ((rc = offloadRelationSegmentPath(nspname, relname, local_path, modcount,
                                       logicalEof, storage_path)) < 0) {
    return rc;
  }

  if (remove_locally) {
    /*wtf*/
    RelationDropStorageNoClose(aorel);
  }

  auto ioadv = std::make_shared<IOadv>(
      std::string(gpg_engine_path), std::string(gpg_key_id),
      std::string(storage_config), nspname,
      std::string(aorel->rd_rel->relname.data),
      std::string(storage_host /*host*/),
      std::string(storage_bucket /*bucket*/),
      std::string(storage_prefix /*prefix*/), local_path, use_gpg_crypto);
  /* we dont need to interact with s3 while in recovery*/

  auto iohandler = YIO(ioadv, GpIdentity.segindex, modcount, "");

  virtual_sz = yezzey_virtual_relation_size(ioadv, GpIdentity.segindex);

  elog(yezzey_ao_log_level,
       "yezzey: relation segment reached external storage path %s, virtual "
       "size %ld, logical eof %ld",
       local_path.c_str(), virtual_sz, logicalEof);
  return 0;
}

int statRelationSpaceUsage(Relation aorel, int segno, int64 modcount,
                           int64 logicalEof, size_t *local_bytes,
                           size_t *local_commited_bytes,
                           size_t *external_bytes) {
  size_t virtual_sz;
  RelFileNode rnode;
  HeapTuple tp;
  int vfd;

  rnode = aorel->rd_node;

  tp = SearchSysCache1(NAMESPACEOID,
                       ObjectIdGetDatum(aorel->rd_rel->relnamespace));

  if (!HeapTupleIsValid(tp)) {

    elog(ERROR, "yezzey: failed to get namescape name of relation %s",
         aorel->rd_rel->relname.data);
  }

  Form_pg_namespace nsptup = (Form_pg_namespace)GETSTRUCT(tp);
  auto nspname = std::string(NameStr(nsptup->nspname));
  ReleaseSysCache(tp);

  auto local_path = getlocalpath(rnode.dbNode, rnode.relNode, segno);
  elog(yezzey_ao_log_level, "yezzey: contructed path %s", local_path.c_str());

  auto ioadv = std::make_shared<IOadv>(
      std::string(gpg_engine_path), std::string(gpg_key_id),
      std::string(storage_config), nspname,
      std::string(aorel->rd_rel->relname.data),
      std::string(storage_host /*host*/),
      std::string(storage_bucket /*bucket*/),
      std::string(storage_prefix /*prefix*/), local_path, use_gpg_crypto);
  /* we dont need to interact with s3 while in recovery*/

  auto iohandler = YIO(ioadv, GpIdentity.segindex, modcount, "");

  /* stat external storage usage */
  virtual_sz = yezzey_virtual_relation_size(ioadv, GpIdentity.segindex);

  *external_bytes = virtual_sz;

  /* No local storage cache logic for now */
  *local_bytes = 0;

  vfd = PathNameOpenFile((char *)local_path.c_str(), O_RDONLY, 0600);
  if (vfd != -1) {
    *local_bytes += FileSeek(vfd, 0, SEEK_END);
    FileClose(vfd);
  }

  // Assert(virtual_sz <= logicalEof);
  *local_commited_bytes = logicalEof - virtual_sz;
  return 0;
}

int statRelationSpaceUsagePerExternalChunk(Relation aorel, int segno,
                                           int64 modcount, int64 logicalEof,
                                           size_t *local_bytes,
                                           size_t *local_commited_bytes,
                                           yezzeyChunkMeta **list,
                                           size_t *cnt_chunks) {
  RelFileNode rnode;
  int vfd;
  HeapTuple tp;

  rnode = aorel->rd_node;

  auto local_path = getlocalpath(rnode.dbNode, rnode.relNode, segno);
  elog(yezzey_ao_log_level, "contructed path %s", local_path.c_str());

  tp = SearchSysCache1(NAMESPACEOID,
                       ObjectIdGetDatum(aorel->rd_rel->relnamespace));

  if (!HeapTupleIsValid(tp)) {
    elog(ERROR, "yezzey: failed to get namescape name of relation %s",
         aorel->rd_rel->relname.data);
  }

  Form_pg_namespace nsptup = (Form_pg_namespace)GETSTRUCT(tp);
  auto nspname = std::string(NameStr(nsptup->nspname));
  ReleaseSysCache(tp);

  auto ioadv = std::make_shared<IOadv>(
      std::string(gpg_engine_path), std::string(gpg_key_id),
      std::string(storage_config), nspname,
      std::string(aorel->rd_rel->relname.data),
      std::string(storage_host /*host*/),
      std::string(storage_bucket /*bucket*/),
      std::string(storage_prefix /*prefix*/), local_path, use_gpg_crypto);
  /* we dont need to interact with s3 while in recovery*/

  auto iohandler = YIO(ioadv, GpIdentity.segindex, modcount, "");

  Assert((*cnt_chunks) >= 0);
  /* stat external storage usage */

  auto meta = iohandler.list_relation_chunks();

  // do copy;
  // list will be allocated in current PostgreSQL mempry context
  *list = (struct yezzeyChunkMeta *)palloc(sizeof(struct yezzeyChunkMeta) *
                                           (*cnt_chunks));

  for (size_t i = 0; i < *cnt_chunks; ++i) {
    (*list)[i].chunkSize = meta[i].chunkSize;
    (*list)[i].chunkName = pstrdup(meta[i].chunkName.c_str());
  }

  /* No local storage cache logic for now */
  *local_bytes = 0;

  vfd = PathNameOpenFile((char *)local_path.c_str(), O_RDONLY, 0600);
  if (vfd != -1) {
    *local_bytes += FileSeek(vfd, 0, SEEK_END);
    FileClose(vfd);
  }

  *local_commited_bytes = 0;
  return 0;
}

int loadRelationSegment(RelFileNode rnode, int segno) {

  if (segno == 0) {
    /* should never happen */
    return 0;
  }

  auto path = getlocalpath(rnode.dbNode, rnode.relNode, segno);
  elog(yezzey_ao_log_level, "contructed path %s", path.c_str());
  if (ensureFilepathLocal(path)) {
    // nothing to do
    return 0;
  }

#if 0
    if ((rc = getFilepathFromS3(path.data)) < 0) {
        return rc;
    }
#endif
  return 0;
}