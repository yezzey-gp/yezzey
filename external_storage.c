

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


int yezzey_log_level = INFO;
int yezzey_ao_log_level = INFO;


/*
* This function used by AO-related realtion functions
*/
bool
ensureFilepathLocal(char *filepath)
{
	int fd, cp, errno_;

	fd = open(filepath, O_RDONLY);
	errno_ = errno;

	cp = fd;
	elog(yezzey_ao_log_level, "[YEZZEY_SMGR] trying to open %s, result - %d, %d", filepath, fd, errno_);
	close(fd);

	return cp >= 0;
}

int
offloadFileToExternalStorage(const char *localPath)
{
	void * whandle;
	int rc;
	int tot;
	int currptrtot;
	char * buffer;
	char * bptr;
	size_t chunkSize;
	File vfd;
	int64 sz;
	int64 progress;
	
	chunkSize = 1 << 20;
	buffer = palloc(chunkSize);
	vfd = PathNameOpenFile(localPath, O_RDONLY, 0600);
	sz = FileDiskSize(vfd);
	progress = 0;

	whandle = createWriterHandle(s3_prefix, localPath);
	rc = 0;

	while (progress < sz)
	{
		/* code */
		rc = FileRead(vfd, buffer, chunkSize);
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

	yezzey_complete_w_transfer_data(&whandle);

	pfree(buffer);

	return rc;
}

bool
ensureFileLocal(RelFileNode rnode, BackendId backend, ForkNumber forkNum, BlockNumber blkno)
{	
	// XXX: not used by AOseg logic
	// if (IsCrashRecoveryOnly()) {
	// 	/* MDB-19689: do not consult catalog 
	// 		if crash recovery is in progress */

	// 	elog(yezzey_log_level, "[YEZZEY_SMGR]: skip ensuring while crash recovery");
	// 	return true;
	// }

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
	elog(INFO, "[YEZZEY_SMGR_BG] tried to remove local file \"%s\", result: %d", localPath, rc);
	return rc;
}

int
offloadRelationSegment(RelFileNode rnode, int segno) {
    StringInfoData path;
	int rc;

    if (segno == 0) {
        /* should never happen */
        return 0;
    }

    initStringInfo(&path);
    appendStringInfo(&path, "base/%d/%d.%d", rnode.dbNode, rnode.relNode, segno);

    elog(INFO, "contructed path %s", path.data);
    if (!ensureFilepathLocal(path.data)) {
        // nothing to do
        return 0;
    }

    if ((rc = offloadFileToExternalStorage(path.data)) < 0) {
        pfree(path.data);
        return rc;
    }

    if ((rc = removeLocalFile(path.data)) < 0) {
        elog(INFO, "errno while remove %d", errno);
        pfree(path.data);
        return rc;
    }

    pfree(path.data);
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

    elog(INFO, "contructed path %s", path.data);
    if (ensureFilepathLocal(path.data)) {
        // nothing to do
        return 0;
    }


    if ((rc = getFilepathFromS3(path.data)) < 0) {
        pfree(path.data);
        return rc;
    }

    pfree(path.data);
    return 0;
}

int
getFilepathFromS3(const char *filepath)
{
	// void * rhandle;
	// int rc;
	// char * buffer;
	// size_t chunkSize;
	// File vfd;
	// int64 sz;
	// int64 progress;

	// elog(INFO, "[YEZZEY_SMGR] fetching %s", filepath);

	// chunkSize = 1 << 20;
	// buffer = palloc(chunkSize);
	// vfd = PathNameOpenFile(localPath, O_WRONLY, 0600);
	// sz = FileDiskSize(vfd);
	// progress = 0;

	// rhandle = createReaderHandle(localPath);
	// rc = 0;

	// while (progress < sz)
	// {
	// 	if (!yezzey_reader_transfer_data(rhandle, buffer, rc)) {
	// 		return -1;
	// 	}
	// 	progress += rc;

	// 			/* code */
	// 	rc = FileWrite(vfd, buffer, chunkSize);
	// 	if (rc < 0) {
	// 		return rc;
	// 	}
	// 	if (rc == 0) continue;
	// }

	// yezzey_complete_r_transfer_data(&whandle);
	// elog(INFO, "[YEZZEY_SMGR] load %s, retcode %d", filepath, rc);

	// pfree(buffer);

	// return rc;

	return 0;
}

char *
buildExternalStorageCommand(const char *s3Command, const char *localPath, const char *externalPath)
{
	StringInfoData result;
	const char *sp;

	initStringInfo(&result);

	for (sp = s3Command; *sp; sp++)
	{
		if (*sp == '%')
		{
			switch (sp[1])
			{
				case 'f':
					{
						sp++;
						appendStringInfoString(&result, localPath);
						break;
					}
				case 's':
					{
						sp++;
						appendStringInfoString(&result, externalPath);
						break;
					}
				case '%':
					{
						sp++;
						appendStringInfoChar(&result, *sp);
						break;
					}
				default:
					{
						appendStringInfoChar(&result, *sp);
						break;
					}
			}
		}
		else
		{
			appendStringInfoChar(&result, *sp);
		}
	}

	return result.data;
}
