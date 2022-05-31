#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"

PG_FUNCTION_INFO_V1(test_function);

Datum
test_function(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text("test"));
}
