
#include "partition.h"

bool
yezzey_check_const_expr(Const *constval)
{
	if (constval->constisnull)
	{
		return false;
	}

	switch (constval->consttype)
	{
		case TIMESTAMPOID:
		case DATEOID:
			return true;
		default:
			return false;
	}
}


bool
yezzey_check_rule_expr(Node *node)
{
	if (node == NULL)
		return false;

	/*
	 * Each level of get_rule_expr must emit an indivisible term
	 * (parenthesized if necessary) to ensure result is reparsed into the same
	 * expression tree.  The only exception is that when the input is a List,
	 * we emit the component items comma-separated with no surrounding
	 * decoration; this is convenient for most callers.
	 */
	switch (nodeTag(node))
	{
		case T_Const:
			return yezzey_check_const_expr((Const *) node);
		case T_List:
			{
				ListCell   *l;
				foreach(l, (List *) node)
				{
					return yezzey_check_rule_expr((Node *) lfirst(l));
				}

				return false;
			}
		default:
			return false;
	}
}

bool
yezzey_get_expr_worker(text *expr)
{
	/* Convert input TEXT object to C string */
	auto exprstr = text_to_cstring(expr);

	/* Convert expression to node tree */
	auto node = (Node *) stringToNode(exprstr);

	pfree(exprstr);

	return yezzey_check_rule_expr(node);
}
