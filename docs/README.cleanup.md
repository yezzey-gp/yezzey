
## Yezzey files cleanup

# TODO: enmhance this doc

Yezzey stores files in external storage in chunks. When relation (offloaded AO/AOCS table) dropped, this files in no longer neede. However, we do not delete them instantly. The first reason to this is transaction feature of yezzey-related operations: if transation, that drops yezzey relation files is aborted or rolled back, files in external storage must persist. Also, for greenplum installation with PITR feature enabled, yezzey files must persist in external storage until all backups, with backup end LSN <= table drop lsn, will be outdated (deleted).

To handle all this cases, we track all info about yezzey expired files in metadata table (yezzey_expire_index). This contains relation OID, relfile OID (relation file OID may change after DDL) and md5 of fully qualified relation name, and also expire LSN, meaning last LSN, on which Greenplum has this table persisted. 