

#include "proxy.h"

#include "gpreader.h"
#include "gpwriter.h"

#include "unordered_map"

#include "external_storage_smgr.h"

#include "io.h"
#include "io_adv.h"

typedef struct yezzey_vfd {
	int y_vfd; /* Either YEZZEY_* preserved fd or pg internal fd >= 9 */

	int localTmpVfd; /* for writing cache files */
	
	/* s3-related params */
	char * filepath;
	char * nspname;
	char * relname;

	yezzey_io_handler handler;

	/* gpg-related params */
	

	/* open args */ 
	int fileFlags; 
	int fileMode;
	
	
	int64 offset;
	int64 virtualSize;
	int64 modcount;

	yezzey_vfd() : y_vfd(0), localTmpVfd(0), filepath(NULL), nspname(NULL),
	relname(NULL), fileFlags(0), fileMode(0), offset(0), virtualSize(0), modcount(0), handler(yezzey_io_handler()) {

	}
	yezzey_vfd& operator=(yezzey_vfd&& vfd) {
		handler = vfd.handler;
		return *this;
	}
} yezzey_vfd;

#define YEZZEY_VANANT_VFD 0 /* not used */
#define YEZZEY_NOT_OPENED 1
#define YEZZEY_OPENED 2
#define YEZZEY_MIN_VFD 3

File virtualEnsure(SMGRFile file);

// be unordered map
std::unordered_map<SMGRFile, yezzey_vfd> yezzey_vfd_cache;

File s3ext = -2;

/* lazy allocate external storage connections */
int readprepare(SMGRFile file) {
#ifdef CACHE_LOCAL_WRITES_FEATURE
/* CACHE_LOCAL_WRITES_FEATURE to do*/
#endif

	(void)createReaderHandle(yezzey_vfd_cache[file].handler,
		GpIdentity.segindex);

	if (yezzey_vfd_cache[file].handler.read_ptr == NULL) {
		return -1;
	}

	Assert(yezzey_vfd_cache[file].handler.read_ptr != NULL);

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

	(void)createWriterHandle(
		yezzey_vfd_cache[file].handler, 
		GpIdentity.segindex,
		1 /*because modcount will increase on write*/ + yezzey_vfd_cache[file].modcount);

	elog(yezzey_ao_log_level, "prepared writer handle for modcount %ld", yezzey_vfd_cache[file].modcount);
	if (yezzey_vfd_cache[file].handler.write_ptr  == NULL) {
		return -1;
	}

	Assert(yezzey_vfd_cache[file].handler.write_ptr != NULL);


#ifdef CACHE_LOCAL_WRITES_FEATURE
/* CACHE_LOCAL_WRITES_FEATURE to do*/
#endif

	return 0;
}

File virtualEnsure(SMGRFile file) {
	File internal_vfd;
	if (yezzey_vfd_cache[file].y_vfd == YEZZEY_VANANT_VFD) {
		elog(ERROR, "attempt to ensure locality of not opened file");
	}	
	if (yezzey_vfd_cache[file].y_vfd == YEZZEY_NOT_OPENED) {
		// not opened yet
		if (!ensureFilepathLocal(yezzey_vfd_cache[file].filepath)) {
			// do s3 read
			return s3ext;
		}

		/* Do we need this? */

		internal_vfd = PathNameOpenFile(yezzey_vfd_cache[file].filepath,
		yezzey_vfd_cache[file].fileFlags, yezzey_vfd_cache[file].fileMode);

		elog(
			yezzey_ao_log_level, 
			"virtualEnsure: yezzey virtual file descriptor for file %s become %d", 
			yezzey_vfd_cache[file].filepath, 
			internal_vfd);
		
		if (internal_vfd == -1) {
			// error
			elog(ERROR, "virtualEnsure: failed to proxy open file %s for fd %d", yezzey_vfd_cache[file].filepath, file);
		}
		elog(yezzey_ao_log_level, "y vfd become %d", internal_vfd);

		yezzey_vfd_cache[file].y_vfd = internal_vfd; // -1 is ok

		elog(yezzey_ao_log_level, "virtualEnsure: file %s yezzey descriptor become %d", yezzey_vfd_cache[file].filepath, file);
		/* allocate handle struct */
	}

	return yezzey_vfd_cache[file].y_vfd;
}

int64 yezzey_NonVirtualCurSeek(SMGRFile file) {
	File actual_fd = virtualEnsure(file);
	if (actual_fd == s3ext) {
		elog(yezzey_ao_log_level, 
			"yezzey_NonVirtualCurSeek: non virt file seek with yezzey fd %d and actual file in external storage, responding %ld", 
			file,
			yezzey_vfd_cache[file].offset);
		return yezzey_vfd_cache[file].offset;
	}
	elog(yezzey_ao_log_level, 
		"yezzey_NonVirtualCurSeek: non virt file seek with yezzey fd %d and actual %d", 
		file, 
		actual_fd);
	return FileNonVirtualCurSeek(actual_fd);
}


int64 yezzey_FileSeek(SMGRFile file, int64 offset, int whence) {
	File actual_fd = virtualEnsure(file);
	if (actual_fd == s3ext) {
		// what?
		yezzey_vfd_cache[file].offset = offset;
		return offset; 
	}
	elog(yezzey_ao_log_level, "yezzey_FileSeek: file seek with yezzey fd %d offset %ld actual %d", file, offset, actual_fd);
	return FileSeek(actual_fd, offset, whence);
}

int	yezzey_FileSync(SMGRFile file) {
	File actual_fd = virtualEnsure(file);
	if (actual_fd == s3ext) {
		/* s3 always sync ? */
		/* sync tmp buf file here */
		return 0;
	}
	elog(yezzey_ao_log_level, "file sync with fd %d actual %d", file, actual_fd);
	return FileSync(actual_fd);
}

SMGRFile yezzey_AORelOpenSegFile(char *nspname, char * relname, FileName fileName, int fileFlags, int fileMode, int64 modcount) {
	elog(yezzey_ao_log_level, "yezzey_AORelOpenSegFile: path name open file %s", fileName);

	/* lookup for virtual file desc entry */
	for (SMGRFile yezzey_fd = YEZZEY_NOT_OPENED + 1;; ++yezzey_fd) {
		if (!yezzey_vfd_cache.count(yezzey_fd)) {
			yezzey_vfd_cache[yezzey_fd] = yezzey_vfd();
			// memset(&yezzey_vfd_cache[file], 0, sizeof(yezzey_vfd));
			yezzey_vfd_cache[yezzey_fd].filepath = strdup(fileName);
			if (relname == NULL) {
				/* Should be possible only in recovery */
				Assert(RecoveryInProgress());
			} else {
				yezzey_vfd_cache[yezzey_fd].relname = strdup(relname);
			}
			if (nspname == NULL) {
				/* Should be possible only in recovery */
				Assert(RecoveryInProgress());
			} else {
				yezzey_vfd_cache[yezzey_fd].nspname = strdup(nspname);
			}
			yezzey_vfd_cache[yezzey_fd].fileFlags = fileFlags;
			yezzey_vfd_cache[yezzey_fd].fileMode = fileMode;
			yezzey_vfd_cache[yezzey_fd].modcount = modcount;
			if (yezzey_vfd_cache[yezzey_fd].filepath == NULL || 
			(!RecoveryInProgress() && yezzey_vfd_cache[yezzey_fd].relname == NULL)) {
				elog(ERROR, "out of memory");
			}

			yezzey_vfd_cache[yezzey_fd].y_vfd = YEZZEY_NOT_OPENED;
		
			yezzey_vfd_cache[yezzey_fd].handler = yezzey_io_handler(
				gpg_engine_path,
				gpg_key_id,
				use_gpg_crypto,
				storage_config,
				yezzey_vfd_cache[yezzey_fd].nspname,
				yezzey_vfd_cache[yezzey_fd].relname,
				storage_host /*host*/,
				storage_bucket/*bucket*/,
				storage_prefix/*prefix*/,
				yezzey_vfd_cache[yezzey_fd].filepath
			);

			/* we dont need to interact with s3 while in recovery*/

			if (RecoveryInProgress()) {
				/* replicae */ 
				return yezzey_fd;
			} else {
				/* primary */
				if (!ensureFilepathLocal(yezzey_vfd_cache[yezzey_fd].filepath)) {
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
		if (!yezzey_io_close(yezzey_vfd_cache[file].handler)) {
			// very bad 

			elog(ERROR, "failed to complete external storage interfacrtion: fd %d", file);
		}
	} else {
		FileClose(actual_fd);
	}

#ifdef DISKCACHE
/* CACHE_LOCAL_WRITES_FEATURE to do*/
#endif
	if (yezzey_vfd_cache[file].filepath) {
		free(yezzey_vfd_cache[file].filepath);
	}
	if (yezzey_vfd_cache[file].relname) {
		free(yezzey_vfd_cache[file].relname);
	}
	if (yezzey_vfd_cache[file].nspname) {
		free(yezzey_vfd_cache[file].nspname);
	}
	yezzey_io_free(yezzey_vfd_cache[file].handler);
	yezzey_vfd_cache.erase(file);
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
			/* Should we return $amount or min (virtualSize - currentLogicalEof, amount) ? */
			return amount;
		}

		if (yezzey_vfd_cache[file].handler.write_ptr == NULL) {
			elog(yezzey_ao_log_level, "read from external storage while read handler uninitialized");
			return -1;
		}
#ifdef ALLOW_MODIFY_EXTERNAL_TABLE
#ifdef CACHE_LOCAL_WRITES_FEATURE
/* CACHE_LOCAL_WRITES_FEATURE to do*/
#endif
#else
		elog(ERROR, "external table modifications are not supported yet");
#endif
		size_t rc = amount;
		if (!yezzey_io_write(yezzey_vfd_cache[file].handler, buffer, &rc)) {
			elog(WARNING, "failed to write to external storage");
			return -1;
		}
		elog(yezzey_ao_log_level, "yezzey_FileWrite: write %d bytes, %ld transfered, yezzey fd %d", amount, rc, file);
		yezzey_vfd_cache[file].offset += rc;
		return rc;
	}
	elog(yezzey_ao_log_level, "file write with %d, actual %d", file, actual_fd);
	size_t rc = FileWrite(actual_fd, buffer, amount);
	if (rc > 0) {
		yezzey_vfd_cache[file].offset += rc;
	}
	return rc;
}

int yezzey_FileRead(SMGRFile file, char *buffer, int amount) {
	File actual_fd = virtualEnsure(file);
	size_t curr = amount;

	if (actual_fd == s3ext) {
		if (yezzey_vfd_cache[file].handler.read_ptr == NULL) {
			elog(yezzey_ao_log_level, "read from external storage while read handler uninitialized");
			return -1;
		}
		if (yezzey_reader_empty(yezzey_vfd_cache[file].handler)) {
			if (yezzey_vfd_cache[file].localTmpVfd <= 0) {
				return 0;
			}
#ifdef DISKCACHE
/* CACHE_LOCAL_WRITES_FEATURE to do*/
#endif
		} else {
			if (!yezzey_io_read(yezzey_vfd_cache[file].handler, buffer, &curr)) {
				elog(yezzey_ao_log_level, "problem while direct read from s3 read with %d curr: %ld", file, curr);
				return -1;
			}
#ifdef DISKCACHE
/* CACHE_LOCAL_WRITES_FEATURE to do*/
#endif
		}

		yezzey_vfd_cache[file].offset += curr;

		elog(yezzey_ao_log_level, "file read with %d, actual %d, amount %d real %ld", file, actual_fd, amount, curr);
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
