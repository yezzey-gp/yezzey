#include "postgres.h"
#include "fmgr.h"

#include <string.h>
#include <stdlib.h>

#include "utils/builtins.h"
#include "executor/spi.h"
#include "pgstat.h"

#define SmgrTrace DEBUG5

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(move_to_s3);

Datum
move_to_s3(PG_FUNCTION_ARGS)
{
	text *table_name = PG_GETARG_TEXT_P(0);
	//text *shm_name = PG_GETARG_TEXT_P(1);
	char *buf = (char *)palloc(100);
	char *tableName = text_to_cstring(table_name);
	int ret;
	TupleDesc tupdesc;
	SPITupleTable *tuptable;
	HeapTuple tuple;

	elog(SmgrTrace, "[YEZZEY_SMGR] trying to move to S3");
	elog(SmgrTrace, "[YEZZEY_SMGR] selecting oid from %s", tableName);

        SPI_connect();

	strcpy(buf, "SELECT '");
	strcat(buf, tableName);
	strcat(buf, "'::regclass::oid;");
	ret = SPI_execute(buf, true, 1);

	elog(SmgrTrace, "[YEZZEY_SMGR] tried %s, result = %d", buf, ret);

	if (ret != SPI_OK_SELECT || SPI_processed < 1)
		elog(FATAL, "Error while trying to get info about table");
	
	tupdesc = SPI_tuptable->tupdesc;
        tuptable = SPI_tuptable;
	tuple = tuptable->vals[0];
	
	elog(SmgrTrace, "[YEZZEY_SMGR] moving %s into move_table", SPI_getvalue(tuple, tupdesc, 1));

	strcpy(buf, "INSERT INTO yezzey.move_table VALUES('");
	strcat(buf, SPI_getvalue(tuple, tupdesc, 1));
	strcat(buf, "', 'main', '0', true);");
	ret = SPI_execute(buf, false, 1);

	if (ret != SPI_OK_INSERT)
		elog(FATAL, "Error while trying to insert oid into move_table");

        SPI_finish();

	PG_RETURN_VOID();
}
