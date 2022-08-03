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

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(move_to_s3);
PG_FUNCTION_INFO_V1(set_cluster_wal_g_config);
PG_FUNCTION_INFO_V1(set_wal_g_config);

Datum
move_to_s3(PG_FUNCTION_ARGS)
{
	text *table_name = PG_GETARG_TEXT_P(0);
	char *tableName = text_to_cstring(table_name);
	int ret;
	StringInfoData buf, b1;

	initStringInfo(&buf);

	elog(yezzey_log_level, "[YEZZEY_SMGR] checking table for ao or aocs type");
	
	SPI_connect();

	initStringInfo(&b1);	
	appendStringInfoString(&b1, "SELECT aosegtablefqn FROM (SELECT seg.aooid, quote_ident(aoseg_c.relname) AS aosegtablefqn, seg.relfilenode FROM pg_class aoseg_c JOIN ");
	appendStringInfoString(&b1, "(SELECT pg_ao.relid AS aooid, pg_ao.segrelid, aotables.aotablefqn, aotables.relstorage, aotables.relnatts, aotables.relfilenode, aotables.reltablespace FROM pg_appendonly pg_ao JOIN ");
	appendStringInfoString(&b1, "(SELECT c.oid, quote_ident(n.nspname)|| '.' || quote_ident(c.relname) AS aotablefqn, c.relstorage, c.relnatts, c.relfilenode, c.reltablespace FROM pg_class c JOIN ");
	appendStringInfoString(&b1, "pg_namespace n ON c.relnamespace = n.oid WHERE relstorage IN ( 'ao', 'co' ) AND relpersistence='p') aotables ON pg_ao.relid = aotables.oid) seg ON aoseg_c.oid = seg.segrelid) ");
	appendStringInfoString(&b1, "AS x WHERE aooid = '");
	appendStringInfoString(&b1, tableName);
	appendStringInfoString(&b1, "'::regclass::oid;");

	ret = SPI_execute(b1.data, true, 0);

	if (ret != SPI_OK_SELECT)
		elog(FATAL, "Error while finding ao info");
	if (SPI_processed < 1)
		elog(ERROR, "Not an ao or aocs table");
	
	appendStringInfoString(&buf, "INSERT INTO yezzey.metatable VALUES ('");
	appendStringInfoString(&buf, tableName);
	appendStringInfoString(&buf, "', FALSE);");
	
	ret = SPI_execute(buf.data, false, 0);

	if (ret != SPI_OK_INSERT)
		elog(FATAL, "Error while trying to insert oid into move_table");

        SPI_finish();

	PG_RETURN_VOID();
}
