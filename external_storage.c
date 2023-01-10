

#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>

#include "postgres.h"
#include "fmgr.h"

#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>

#include "utils/builtins.h"
#include "executor/spi.h"
#include "pgstat.h"

#include "yezzey.h"
#include "storage/lmgr.h"
#include "access/aosegfiles.h"
#include "utils/tqual.h"

#include "external_storage.h"
#include "smgr_s3.h"


#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "storage/smgr.h"


// For GpIdentity
#include "c.h"
#include "cdb/cdbvars.h"


int yezzey_log_level = INFO;
int yezzey_ao_log_level = INFO;


/*
* This function used by AO-related realtion functions
*/
bool
ensureFilepathLocal(const char *filepath)
{
	int fd, cp, errno_;

	fd = open(filepath, O_RDONLY);
	errno_ = errno;

	cp = fd;
	//elog(yezzey_ao_log_level, "[YEZZEY_SMGR] trying to open %s, result - %d, %d", filepath, fd, errno_);
	close(fd);

	return cp >= 0;
}

int
offloadFileToExternalStorage(const char *relname, const char *localPath, int64 modcount, int64 logicalEof)
{
	void * whandle;
	void * rhandle;
	int rc;
	int tot;
	int currptrtot;
	char * buffer;
	char * bptr;
	size_t chunkSize;
	File vfd;
	int64 curr_read_chunk;
	int64 progress;
	
	chunkSize = 1 << 20;
	buffer = palloc(chunkSize);
	vfd = PathNameOpenFile(localPath, O_RDONLY, 0600);
	if (vfd <= 0) {
		elog(ERROR, "failed to open %s file to transfer to external storage", localPath);
	}
	progress = 0;


	rhandle = createReaderHandle(relname, 
		storage_bucket/*bucket*/, storage_prefix /*prefix*/, localPath, GpIdentity.segindex);

	if (rhandle == NULL) {
		elog(ERROR, "offloading %s: failed to get external storage read handler", localPath);
	}

	whandle = createWriterHandle(rhandle, relname/*relname*/, 
		storage_bucket/*bucket*/, storage_prefix /*prefix*/, localPath, GpIdentity.segindex, modcount);
	
	if (whandle == NULL) {
		elog(ERROR, "offloading %s: failed to get external storage write handler", localPath);
	}

	rc = 0;

	while (progress < logicalEof)
	{
		curr_read_chunk = chunkSize;
		if (progress + curr_read_chunk > logicalEof) {
			/* should not read beyond logical eof */
			curr_read_chunk = logicalEof - progress;
		}
 		/* code */
		rc = FileRead(vfd, buffer, curr_read_chunk);
		if (rc < 0) {
			return rc;
		}
		if (rc == 0) continue;

		tot = 0;
		bptr = buffer;

		while (tot < rc) {
			currptrtot = rc - tot;
			if (!yezzey_writer_transfer_data(whandle, bptr, &currptrtot)) {
				return -1;
			}

			tot += currptrtot;
			bptr += currptrtot;
		}

		progress += rc;
	}

	if (!yezzey_complete_w_transfer_data(&whandle)) {
		elog(ERROR, "failed to complete %s offloading", localPath);
	}
	FileClose(vfd);
	pfree(buffer);

	return rc;
}

bool
ensureFileLocal(RelFileNode rnode, BackendId backend, ForkNumber forkNum, BlockNumber blkno)
{	
	/* MDB-19689: do not consult catalog */

    elog(yezzey_log_level, "ensuring %d is local", rnode.relNode);
    return true;
	StringInfoData path;
	bool result;
	initStringInfo(&path);

	result = ensureFilepathLocal(path.data);

	pfree(path.data);
	return result;
}


int
removeLocalFile(const char *localPath)
{
	int rc = remove(localPath);
	elog(yezzey_ao_log_level, "[YEZZEY_SMGR_BG] remove local file \"%s\", result: %d", localPath, rc);
	return rc;
}

static int
offloadRelationSegmentPath(const char * relname, const char * localpath, int64 modcount, int64 logicalEof) {
	int rc;

    if (!ensureFilepathLocal(localpath)) {
        // nothing to do
        return 0;
    }

    if ((rc = offloadFileToExternalStorage(relname, localpath, modcount, logicalEof)) < 0) {
        return rc;
    }
	return 0;
}

int
offloadRelationSegment(Relation aorel, RelFileNode rnode, int segno, int64 modcount, int64 logicalEof) {
    StringInfoData local_path;
	int rc;
	int64_t virtual_sz;

    initStringInfo(&local_path);
	if (segno == 0) {
    	appendStringInfo(&local_path, "base/%d/%d", rnode.dbNode, rnode.relNode);
	} else {
    	appendStringInfo(&local_path, "base/%d/%d.%d", rnode.dbNode, rnode.relNode, segno);
	}

    elog(yezzey_ao_log_level, "contructed path %s", local_path.data);

	/* xlog goes first */
	// xlog_smgr_local_truncate(rnode, MAIN_FORKNUM, 'a');

	if ((rc = offloadRelationSegmentPath(aorel->rd_rel->relname.data, local_path.data, modcount, logicalEof)) < 0) {
		pfree(local_path.data);
		return rc;
	}

	/*wtf*/
	RelationDropStorageNoClose(aorel);
	
	virtual_sz = yezzey_virtual_relation_size(GpIdentity.segindex, local_path.data);

    appendStringInfo(&local_path, "_tmpbuf");
    elog(yezzey_ao_log_level, "contructed path %s, virtual size %ld, logical eof %ld", local_path.data, virtual_sz, logicalEof);

	Assert(virtual_sz <= logicalEof);

	if ((rc = offloadRelationSegmentPath(aorel->rd_rel->relname.data, local_path.data, modcount, logicalEof - virtual_sz)) < 0) {
		pfree(local_path.data);
		return rc;
	}

    pfree(local_path.data);
    return 0;
}



int
statRelationSpaceUsage(RelFileNode rnode, int segno, int64 modcount, int64 logicalEof, size_t *local_bytes, size_t *local_commited_bytes, size_t *external_bytes) {
    StringInfoData local_path;
	size_t virtual_sz;
	int vfd;

    initStringInfo(&local_path);
	if (segno == 0) {
    	appendStringInfo(&local_path, "base/%d/%d", rnode.dbNode, rnode.relNode);
	} else {
    	appendStringInfo(&local_path, "base/%d/%d.%d", rnode.dbNode, rnode.relNode, segno);
	}

    elog(yezzey_ao_log_level, "contructed path %s", local_path.data);
	
	/* stat external storage usage */
	virtual_sz = yezzey_virtual_relation_size(GpIdentity.segindex, local_path.data);
	*external_bytes = virtual_sz;

	*local_bytes = 0;
	vfd = PathNameOpenFile(local_path.data, O_RDONLY, 0600);
	if (vfd != -1) {
		*local_bytes += FileSeek(vfd, 0, SEEK_END);
		FileClose(vfd);
	}

    appendStringInfo(&local_path, "_tmpbuf");
    elog(yezzey_ao_log_level, "contructed path %s, virtual size %ld, logical eof %ld", local_path.data, virtual_sz, logicalEof);
	vfd = PathNameOpenFile(local_path.data, O_RDONLY, 0600);
	if (vfd != -1) {
		*local_bytes += FileSeek(vfd, 0, SEEK_END);
		FileClose(vfd);
	}
	Assert(virtual_sz <= logicalEof);
	*local_commited_bytes = logicalEof - virtual_sz;

    pfree(local_path.data);
    return 0;
}

int
loadRelationSegment(RelFileNode rnode, int segno) {
    StringInfoData path;
	int rc;

    if (segno == 0) {
        /* should never happen */
        return 0;
    }

    initStringInfo(&path);
    appendStringInfo(&path, "base/%d/%d.%d", rnode.dbNode, rnode.relNode, segno);

    elog(yezzey_ao_log_level, "contructed path %s", path.data);
    if (ensureFilepathLocal(path.data)) {
        // nothing to do
        return 0;
    }

#if 0
    if ((rc = getFilepathFromS3(path.data)) < 0) {
        pfree(path.data);
        return rc;
    }
#endif

    pfree(path.data);
    return 0;
}