#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "access/htup.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#if PG_VERSION_NUM >= 80500
#include "utils/bytea.h"
#endif
#include "utils/date.h"
#include "utils/memutils.h"
#include "utils/palloc.h"

#include "odbclink.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(odbclink_connect);
PG_FUNCTION_INFO_V1(odbclink_driverconnect);
PG_FUNCTION_INFO_V1(odbclink_connections);
PG_FUNCTION_INFO_V1(odbclink_disconnect);
PG_FUNCTION_INFO_V1(odbclink_query_n);
PG_FUNCTION_INFO_V1(odbclink_query_dsn);
PG_FUNCTION_INFO_V1(odbclink_query_connstr);

static odbcconn	*conns;
static int	n_conn;

static int
realloc_conns(void)
{
	odbcconn *new_conns;
	int	n_new_conn;

	n_new_conn = n_conn + CONNCHUNK;

	new_conns = MemoryContextAllocZero(TopMemoryContext, n_new_conn * sizeof(odbcconn));
	if (new_conns == NULL)
		return 0;

	memcpy(new_conns, conns, n_conn * sizeof(odbcconn));
	if (conns)
		pfree(conns);

	conns = new_conns;
	n_conn = n_new_conn;

	return 1;
}

static int
find_conn_dsn(const char *dsn, const char *uid, const char *pwd)
{
	int	i;
	bool	found = false;

	for (i = 0; i < n_conn; i++)
		if (conns[i].connected && conns[i].dsn &&
			!strcmp(conns[i].dsn, dsn) &&
			!strcmp(conns[i].uid, uid) &&
			!strcmp(conns[i].pwd, pwd))
		{
			found = true;
			break;
		}
	if (found)
		return i;
	return -1;
}

static int
find_conn_connstr(const char *connstr)
{
	int	i;
	bool	found = false;

	for (i = 0; i < n_conn; i++)
		if (conns[i].connected && conns[i].connstr && 
			!strcmp(conns[i].connstr, connstr))
		{
			found = true;
			break;
		}
	if (found)
		return i;
	return -1;
}

void
_PG_init(void)
{
	conns = NULL;
	n_conn = 0;
}

void
_PG_fini(void)
{
	int	i;
	for (i = 0; i < n_conn; i++)
		if (conns[i].connected)
		{
			SQLDisconnect(conns[i].hCon);
			SQLFreeConnect(conns[i].hCon);
			SQLFreeEnv(conns[i].hEnv);
		}
	pfree(conns);
}

static SQLCHAR		sqlstate[16];
static SQLINTEGER	nativeerr;
static SQLCHAR		errormsg[2048];
static char		totalerrmsg[2120];

static char *
get_sql_error(int i, int type, odbcstmt *stmt)
{
	SQLSMALLINT	errmsgsize;
	switch (type)
	{
		case SQL_HANDLE_ENV:
			SQLGetDiagRec(type, conns[i].hEnv, 1, sqlstate, &nativeerr, errormsg, sizeof(errormsg), &errmsgsize);
			break;
		case SQL_HANDLE_DBC:
			SQLGetDiagRec(type, conns[i].hCon, 1, sqlstate, &nativeerr, errormsg, sizeof(errormsg), &errmsgsize);
			break;
		default:
		case SQL_HANDLE_STMT:
			if (stmt)
				SQLGetDiagRec(type, stmt->hStmt, 1, sqlstate, &nativeerr, errormsg, sizeof(errormsg), &errmsgsize);
			else
			{
				strcpy((char *)sqlstate, "n/a");
				nativeerr = 0;
				strcpy((char *)errormsg, "n/a");
			}
			break;
	}
	sprintf(totalerrmsg, "[%s] [%d] [%s]", sqlstate, nativeerr, errormsg);

	return totalerrmsg;
}

static int
connect_dsn(const char *dsn, const char *uid, const char *pwd)
{
	int	i, old_max;
	bool	found;
	SQLRETURN	ret;

	old_max = n_conn;
	for (i = 0, found = false; i < old_max; i++)
		if (!conns[i].connected)
		{
			found = true;
			break;
		}
	if (!found)
		if (!realloc_conns())
			elog(ERROR, "odbclink: cannot allocate new connections");

	ret = SQLAllocEnv(&(conns[i].hEnv));
	if (ret != SQL_SUCCESS)
		elog(ERROR, "odbclink: unsuccessful SQLAllocEnv call");

	ret = SQLAllocConnect(conns[i].hEnv, &(conns[i].hCon));
	if (ret != SQL_SUCCESS)
	{
		get_sql_error(i, SQL_HANDLE_ENV, NULL);
		SQLFreeEnv(conns[i].hEnv);
		elog(ERROR, "odbclink: unsuccessful SQLAllocConnect call: %s", totalerrmsg);
	}

	ret = SQLConnect(conns[i].hCon, (SQLCHAR *)dsn, SQL_NTS, (SQLCHAR *)uid, SQL_NTS, (SQLCHAR *)pwd, SQL_NTS);
	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
	{
		get_sql_error(i, SQL_HANDLE_DBC, NULL);
		SQLFreeConnect(conns[i].hCon);
		SQLFreeEnv(conns[i].hEnv);
		elog(ERROR, "odbclink: unsuccessful SQLConnect call: %s", totalerrmsg);
	}

	conns[i].dsn = MemoryContextAlloc(TopMemoryContext, strlen(dsn) + 1); strcpy(conns[i].dsn, dsn);
	conns[i].uid = MemoryContextAlloc(TopMemoryContext, strlen(uid) + 1); strcpy(conns[i].uid, uid);
	conns[i].pwd = MemoryContextAlloc(TopMemoryContext, strlen(uid) + 1); strcpy(conns[i].pwd, pwd);
	conns[i].connstr = NULL;
	conns[i].connected = 1;

	return i;
}

static int
connect_connstr(const char *connstr)
{
	int	i, old_max;
	bool	found;
	SQLRETURN	ret;

	old_max = n_conn;
	for (i = 0, found = false; i < old_max; i++)
		if (!conns[i].connected)
		{
			found = true;
			break;
		}
	if (!found)
		if (!realloc_conns())
			elog(ERROR, "odbclink: cannot allocate new connections");

	ret = SQLAllocEnv(&(conns[i].hEnv));
	if (ret != SQL_SUCCESS)
		elog(ERROR, "odbclink: unsuccessful SQLAllocEnv call");

	ret = SQLAllocConnect(conns[i].hEnv, &(conns[i].hCon));
	if (ret != SQL_SUCCESS)
	{
		get_sql_error(i, SQL_HANDLE_ENV, NULL);
		SQLFreeEnv(conns[i].hEnv);
		elog(ERROR, "odbclink: unsuccessful SQLAllocConnect call: %s", totalerrmsg);
	}

	/*
	 * Connect using a connection string
	 * This code runs in the database backend
	 * so we cannot prompt the user.
	 */
	ret = SQLDriverConnect(conns[i].hCon, NULL,
				(SQLCHAR *)connstr, SQL_NTS,
				NULL, 0, NULL,
				SQL_DRIVER_NOPROMPT);
	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
	{
		get_sql_error(i, SQL_HANDLE_DBC, NULL);
		SQLFreeConnect(conns[i].hCon);
		SQLFreeEnv(conns[i].hEnv);
		elog(ERROR, "odbclink: unsuccessful SQLConnect call: %s", totalerrmsg);
	}

	conns[i].dsn = NULL;
	conns[i].uid = NULL;
	conns[i].pwd = NULL;
	conns[i].connstr = MemoryContextAlloc(TopMemoryContext, strlen(connstr) + 1); strcpy(conns[i].connstr, connstr);
	conns[i].connected = 1;

	return i;
}

Datum
odbclink_connect(PG_FUNCTION_ARGS)
{
	char	*dsn, *uid, *pwd;
	int	i;

	dsn = TextDatumGetCString(PG_GETARG_DATUM(0));
	uid = TextDatumGetCString(PG_GETARG_DATUM(1));
	pwd = TextDatumGetCString(PG_GETARG_DATUM(2));

	i = connect_dsn(dsn, uid, pwd);

	pfree(pwd); pfree(uid); pfree(dsn);

	PG_RETURN_INT32(i + 1);
}

Datum
odbclink_driverconnect(PG_FUNCTION_ARGS)
{
	char	*connstr;
	int	i;

	connstr = TextDatumGetCString(PG_GETARG_DATUM(0));

	i = connect_connstr(connstr);

	pfree(connstr);

	PG_RETURN_INT32(i + 1);
}

Datum
odbclink_connections(PG_FUNCTION_ARGS)
{
	FuncCallContext	   *funcctx;
	int		call_cntr;
	int		max_calls;
	MemoryContext	oldcontext;
	TupleDesc	tupdesc;

	if (SRF_IS_FIRSTCALL())
	{

		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* get a tuple descriptor for our result type */
		switch (get_call_result_type(fcinfo, NULL, &tupdesc))
		{
			case TYPEFUNC_COMPOSITE:
				/* success */
				break;
			case TYPEFUNC_RECORD:
				/* failed to determine actual type of RECORD */
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("function returning record called in context "
							"that cannot accept type record")));
				break;
			default:
				/* result type isn't composite */
				elog(ERROR, "return type must be a row type");
				break;
		}

		funcctx->max_calls = n_conn;
		funcctx->user_fctx = tupdesc;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	call_cntr = funcctx->call_cntr;
	max_calls = funcctx->max_calls;
	tupdesc = funcctx->user_fctx;

	if (call_cntr < max_calls)  /* do when there is more left to send */
	{
		Datum	   values[6];
		bool	   nulls[6];
		HeapTuple	tuple;

		values[0] = Int32GetDatum(call_cntr + 1);
		nulls[0] = false;

		values[1] = BoolGetDatum(conns[call_cntr].connected);
		nulls[1] = false;

		if (conns[call_cntr].dsn)
		{
			values[2] = DirectFunctionCall1(textin, CStringGetDatum(conns[call_cntr].dsn));
			nulls[2] = false;
		}
		else
			nulls[2] = true;

		if (conns[call_cntr].uid)
		{
			values[3] = DirectFunctionCall1(textin, CStringGetDatum(conns[call_cntr].uid));
			nulls[3] = false;
		}
		else
			nulls[3] = true;

		if (conns[call_cntr].pwd)
		{
			values[4] = DirectFunctionCall1(textin, CStringGetDatum(conns[call_cntr].pwd));
			nulls[4] = false;
		}
		else
			nulls[4] = true;

		if (conns[call_cntr].connstr)
		{
			values[5] = DirectFunctionCall1(textin, CStringGetDatum(conns[call_cntr].connstr));
			nulls[5] = false;
		}
		else
			nulls[5] = true;

		tuple = heap_form_tuple(tupdesc, values, nulls);

		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		SRF_RETURN_DONE(funcctx);
	}
}

Datum
odbclink_disconnect(PG_FUNCTION_ARGS)
{
	int	i = PG_GETARG_INT32(0) - 1;
	SQLRETURN	ret;

	if (!(i >= 0 && i < n_conn && conns[i].connected))
		elog(ERROR, "odbclink: no such connection");

	ret = SQLDisconnect(conns[i].hCon);
	if (!SQL_SUCCEEDED(ret))
		elog(NOTICE, "odbclink: unsuccessful SQLDisconnect call");

	ret = SQLFreeConnect(conns[i].hCon);
	if (!SQL_SUCCEEDED(ret))
		elog(NOTICE, "odbclink: unsuccessful SQLFreeConnect call");

	ret = SQLFreeEnv(conns[i].hEnv);
	if (!SQL_SUCCEEDED(ret))
		elog(NOTICE, "odbclink: unsuccessful SQLFreeEnv call");

	conns[i].connected = 0;
	if (conns[i].dsn)
		pfree(conns[i].dsn);
	if (conns[i].uid)
		pfree(conns[i].uid);
	if (conns[i].pwd)
		pfree(conns[i].pwd);
	if (conns[i].connstr)
		pfree(conns[i].connstr);

	PG_RETURN_VOID();
}

static bool
compatTupleDescs(odbcstmt *stmt)
{
	TupleDesc	tupdesc = stmt->tupdesc;
	SQLRETURN	ret;
	int		col;
	int		retval = true;

	ret = SQLNumResultCols(stmt->hStmt, &stmt->cols);
	if (!SQL_SUCCEEDED(ret))
		return false;
	if (tupdesc->natts != stmt->cols)
		return false;

	for (col = 0; col < stmt->cols; col++)
	{
		char		colname[50];
		SQLSMALLINT	colnamesz, type, decimals, nullable;
		SQLULEN		columnsz;
		Oid		typeoid = tupdesc->attrs[col]->atttypid;
		int		typemod = tupdesc->attrs[col]->atttypmod;

		ret = SQLDescribeCol(stmt->hStmt, col + 1,
					(SQLCHAR *)colname, sizeof(colname), &colnamesz,
					&type, &columnsz, &decimals, &nullable);

		switch (type)
		{
			case SQL_CHAR:
			case SQL_VARCHAR:
				if (typeoid != CHAROID && typeoid != BPCHAROID && typeoid != VARCHAROID && typeoid != TEXTOID)
				{
					if (typemod >= 1 && typemod < columnsz) /* maybe >= 0 ? */
						retval = false;
				}
				break;

			case SQL_LONGVARCHAR:
				if (typeoid != TEXTOID)
					retval = false;
				break;

			case SQL_NUMERIC:
			case SQL_DECIMAL:
				if (typeoid != INT2OID && typeoid != INT4OID && typeoid != INT8OID && typeoid != NUMERICOID)
					retval = false;
				break;

			case SQL_BIGINT:
				if (typeoid != INT8OID)
					retval = false;
				break;

			case SQL_INTEGER:
				if (typeoid != INT4OID && typeoid != INT8OID)
					retval = false;
				break;
			case SQL_SMALLINT:
				if (typeoid != INT2OID && typeoid != INT4OID && typeoid != INT8OID)
					retval = false;
				break;

			case SQL_FLOAT:
			case SQL_REAL:
				if (typeoid != FLOAT4OID)
					retval = false;
				break;

			case SQL_DOUBLE:
				if (typeoid != FLOAT8OID)
					retval = false;
				break;

			case SQL_DATE:
				if (typeoid != DATEOID)
					retval = false;
				break;

			case SQL_TIME:
				if (typeoid != TIMEOID && typeoid != TIMETZOID)
					retval = false;
				break;

			case SQL_TIMESTAMP:
				if (typeoid != TIMESTAMPOID && typeoid != TIMESTAMPTZOID)
					retval = false;
				break;

			case SQL_BINARY:
			case SQL_VARBINARY:
			case SQL_LONGVARBINARY:
#if PG_VERSION_NUM >= 80500
				if (typeoid != BYTEAOID)
#endif
					retval = false;
				break;

			case SQL_BIT:
				if (typeoid != BOOLOID)
					retval = false;
				break;
		}
		if (!retval)
		{
			elog(NOTICE, "field index %d input type from ODBC: %d output type for PG: %d", col, type, typeoid);
			break;
		}
	}

	return retval;
}

static SQLRETURN
get_char_data(odbcstmt *stmt, int col, char **value, int *length, bool *isnull)
{
	SQLRETURN	ret;
	char	   *char_val;
	int	char_len = 0, char_pos = 0;
	SQLLEN	size_ind;

	ret = SQL_SUCCESS;
	char_val = palloc(CHARVALCHUNK);
	char_pos = 0;
	char_len = CHARVALCHUNK;
	while (ret != SQL_NO_DATA)
	{
		ret = SQLGetData(stmt->hStmt, col, SQL_C_CHAR, char_val + char_pos, CHARVALCHUNK, &size_ind);

		if (size_ind == SQL_NULL_DATA)
			break;

		if (ret == SQL_NO_DATA)
			break;

		if (size_ind == CHARVALCHUNK)
		{
			char_len += CHARVALCHUNK;
			char_val = repalloc(char_val, char_len);
		}
		char_pos += size_ind;
	}
	if (char_pos == char_len)
	{
		char_len += CHARVALCHUNK;
		char_val = realloc(char_val, char_len);
	}
	char_val[char_pos] = '\0';

	if (value)
		*value = char_val;
	if (*length)
		*length = char_pos;
	if (isnull)
		*isnull = (size_ind == SQL_NULL_DATA);
	return ret;
}

static void
get_data(odbcstmt *stmt, int col, Datum *value, bool *isnull)
{
	SQLRETURN	ret;
	char		colname[50];
	SQLSMALLINT	colnamesz, type, decimals, nullable;
	SQLULEN		columnsz;
	Oid		typeoid = stmt->tupdesc->attrs[col - 1]->atttypid;
	int		typemod = stmt->tupdesc->attrs[col - 1]->atttypmod;

	/* These are the values SQLGetData() puts data into */
	int16	smallint_val = 0;
	int32	int_val = 0;
	int64	bigint_val = 0;
	float	float_val = 0;
	double	double_val = 0;
	char	*char_val = NULL;
	int	char_pos = 0;
	SQLLEN	size_ind;

	ret = SQLDescribeCol(stmt->hStmt, col,
				(SQLCHAR *)colname, sizeof(colname), &colnamesz,
				&type, &columnsz, &decimals, &nullable);
	if (!SQL_SUCCEEDED(ret))
	{
		get_sql_error(0, SQL_HANDLE_STMT, stmt);
		elog(ERROR, "odbclink: unsuccessful SQLDescribeCol call: %s", totalerrmsg);
	}

	switch (type)
	{
		case SQL_SMALLINT:
			ret = SQLGetData(stmt->hStmt, col, SQL_C_SSHORT,
					(SQLPOINTER)&smallint_val, sizeof(smallint_val), &size_ind);
			if (!SQL_SUCCEEDED(ret))
			{
				get_sql_error(0, SQL_HANDLE_STMT, stmt);
				elog(ERROR, "odbclink: unsuccessful SQLGetData call: %s", totalerrmsg);
			}
			*isnull = (size_ind == SQL_NULL_DATA);
			if (!*isnull)
				switch (typeoid)
				{
					case INT2OID:
						*value = Int16GetDatum(smallint_val);
						break;
					case INT4OID:
						*value = Int32GetDatum(smallint_val);
						break;
					case INT8OID:
						*value = Int64GetDatum(smallint_val);
						break;
				}
			break;

		case SQL_INTEGER:
		case SQL_BIT:
			ret = SQLGetData(stmt->hStmt, col, SQL_C_SLONG,
					(SQLPOINTER)&int_val, sizeof(int_val), &size_ind);
			if (!SQL_SUCCEEDED(ret))
			{
				get_sql_error(0, SQL_HANDLE_STMT, stmt);
				elog(ERROR, "odbclink: unsuccessful SQLGetData call: %s", totalerrmsg);
			}
			*isnull = (size_ind == SQL_NULL_DATA);
			if (!*isnull)
				switch (typeoid)
				{
					case INT4OID:
						*value = Int32GetDatum(int_val);
						break;
					case INT8OID:
						*value = Int64GetDatum(int_val);
						break;
					case BOOLOID:
						*value = BoolGetDatum(int_val != 0);
				}
			break;

		case SQL_BIGINT:
			ret = SQLGetData(stmt->hStmt, col, SQL_C_SBIGINT,
					(SQLPOINTER)&bigint_val, sizeof(bigint_val), &size_ind);
			if (!SQL_SUCCEEDED(ret))
			{
				get_sql_error(0, SQL_HANDLE_STMT, stmt);
				elog(ERROR, "odbclink: unsuccessful SQLGetData call: %s", totalerrmsg);
			}
			*isnull = (size_ind == SQL_NULL_DATA);
			if (!*isnull)
				*value = Int64GetDatum(bigint_val);
			break;

		case SQL_NUMERIC:
		case SQL_DECIMAL:
			/*
			 * give truncated integer data out of numeric/decimal types
			 * report overflow as error
			 * this is to cause the least surprise, Oracle reports SQL_DECIMAL
			 * for tables created with INTEGER fields.
			 */
			if (typeoid == INT2OID || typeoid == INT4OID || typeoid == INT8OID)
			{
				ret = get_char_data(stmt, col, &char_val, &char_pos, isnull);
				if (!*isnull)
				{
					char *endptr;

					bigint_val = strtoll(char_val, &endptr, 10);

					switch (typeoid)
					{
						case INT8OID:
							if (endptr && *endptr != '\0' && *endptr != '.')
								elog(ERROR, "too large decimal value for 64-bit integer");
							*value = Int64GetDatum(bigint_val);
							break;
						case INT4OID:
							if (bigint_val > INT_MAX)
								elog(ERROR, "too large decimal value for 32-bit integer");
							int_val = bigint_val;
							*value = Int32GetDatum(int_val);
							break;
						case INT2OID:
							if (bigint_val > SHRT_MAX)
								elog(ERROR, "too large decimal value for 16-bit integer");
							smallint_val = bigint_val;
							*value = Int16GetDatum(smallint_val);
							break;
					}
				}
				break;
			}
			/* fall through */
		case SQL_CHAR:
		case SQL_VARCHAR:
		case SQL_LONGVARCHAR:
		case SQL_DATE:
		case SQL_TIME:
		case SQL_TIMESTAMP:
		case SQL_BINARY:
		case SQL_VARBINARY:
		case SQL_LONGVARBINARY:
			ret = get_char_data(stmt, col, &char_val, &char_pos, isnull);
			if (!*isnull)
				switch (typeoid)
				{
					case CHAROID:
						*value = DirectFunctionCall1(charin, CStringGetDatum(char_val));
						break;
					case BPCHAROID:
						*value = DirectFunctionCall3(bpcharin, CStringGetDatum(char_val), ObjectIdGetDatum(typeoid), Int32GetDatum(typemod));
						break;
					case VARCHAROID:
						*value = DirectFunctionCall3(varcharin, CStringGetDatum(char_val), ObjectIdGetDatum(typeoid), Int32GetDatum(typemod));
						break;
					case TEXTOID:
						*value = DirectFunctionCall3(textin, CStringGetDatum(char_val), ObjectIdGetDatum(typeoid), Int32GetDatum(typemod));
						break;
					case NUMERICOID:
						*value = DirectFunctionCall3(numeric_in, CStringGetDatum(char_val), ObjectIdGetDatum(typeoid), Int32GetDatum(typemod));
						break;
					case DATEOID:
						*value = DirectFunctionCall1(date_in, CStringGetDatum(char_val));
						break;
					case TIMEOID:
						*value = DirectFunctionCall1(time_in, CStringGetDatum(char_val));
						break;
					case TIMETZOID:
						*value = DirectFunctionCall1(timetz_in, CStringGetDatum(char_val));
						break;
					case TIMESTAMPOID:
						*value = DirectFunctionCall3(timestamp_in, CStringGetDatum(char_val), ObjectIdGetDatum(typeoid), Int32GetDatum(typemod));
						break;
					case TIMESTAMPTZOID:
						*value = DirectFunctionCall3(timestamptz_in, CStringGetDatum(char_val), ObjectIdGetDatum(typeoid), Int32GetDatum(typemod));
						break;
#if PG_VERSION_NUM >= 80500
					case BYTEAOID:
					{
						char	*bin_val;
						bin_val = palloc(2 * char_pos + 1);
						hex_encode(char_val, char_pos, bin_val);
						bin_val[2 * char_pos] = '\0';
						*value = DirectFunctionCall1(byteain, CStringGetDatum(bin_val));
						pfree(bin_val);
					}
#endif
				}

			pfree(char_val);
			break;

		case SQL_FLOAT:
		case SQL_REAL:
			ret = SQLGetData(stmt->hStmt, col, SQL_C_FLOAT,
					(SQLPOINTER)&float_val, sizeof(float_val), &size_ind);
			if (!SQL_SUCCEEDED(ret))
			{
				get_sql_error(0, SQL_HANDLE_STMT, stmt);
				elog(ERROR, "odbclink: unsuccessful SQLGetData call: %s", totalerrmsg);
			}
			*isnull = (size_ind == SQL_NULL_DATA);
			if (!*isnull)
				*value = Float4GetDatum(float_val);
			break;

		case SQL_DOUBLE:
			ret = SQLGetData(stmt->hStmt, col, SQL_C_DOUBLE,
					(SQLPOINTER)&double_val, sizeof(double_val), &size_ind);
			if (!SQL_SUCCEEDED(ret))
			{
				get_sql_error(0, SQL_HANDLE_STMT, stmt);
				elog(ERROR, "odbclink: unsuccessful SQLGetData call: %s", totalerrmsg);
			}
			*isnull = (size_ind == SQL_NULL_DATA);
			if (!*isnull)
				*value = Float8GetDatum(double_val);
			break;
	}
}

static void
init_query_common(PG_FUNCTION_ARGS, int i, char *query)
{
	FuncCallContext	   *funcctx;
	MemoryContext	oldcontext;
	SQLRETURN	ret;
	odbcstmt	   *stmt;

	funcctx = SRF_FIRSTCALL_INIT();

	oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

	stmt = palloc(sizeof(odbcstmt));

	/*
	 * The allocated statement handle is the only thing
	 * that must be freed manually. All the other allocations
	 * are dealt by PostgreSQL's garbage collection.
	 */
	ret = SQLAllocStmt(conns[i].hCon, &stmt->hStmt);
	if (!SQL_SUCCEEDED(ret))
	{
		get_sql_error(i, SQL_HANDLE_DBC, NULL);
		MemoryContextSwitchTo(oldcontext);
		elog(ERROR, "odbclink: unsuccessful SQLAllocStmt call: %s", totalerrmsg);
	}

	ret = SQLExecDirect(stmt->hStmt, (SQLCHAR *)query, SQL_NTS);
	if (!SQL_SUCCEEDED(ret))
	{
		get_sql_error(0, SQL_HANDLE_STMT, stmt);
		SQLFreeHandle(SQL_HANDLE_STMT, stmt->hStmt);
		elog(ERROR, "odbclink: unsuccessful SQLExecDirect call: %s", totalerrmsg);
	}

	/* get a tuple descriptor for our result type */
	switch (get_call_result_type(fcinfo, NULL, &(stmt->tupdesc)))
	{
		case TYPEFUNC_COMPOSITE:
			/* success */
			break;
		case TYPEFUNC_RECORD:
			/* failed to determine actual type of RECORD */
			SQLFreeHandle(SQL_HANDLE_STMT, stmt->hStmt);
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("function returning record called in context "
							"that cannot accept type record")));
			break;
		default:
			/* result type isn't composite */
			SQLFreeHandle(SQL_HANDLE_STMT, stmt->hStmt);
			elog(ERROR, "return type must be a row type");
			break;
	}

	/*
	 * Check that return tupdesc is compatible with the data we got from SPI,
	 * at least based on number and type of attributes
	 */
	if (!compatTupleDescs(stmt))
	{
		SQLFreeHandle(SQL_HANDLE_STMT, stmt->hStmt);
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("return and sql tuple descriptions are " \
						"incompatible")));
	}

	funcctx->user_fctx = stmt;

	MemoryContextSwitchTo(oldcontext);
}

static Datum
query_common(PG_FUNCTION_ARGS)
{
	FuncCallContext	   *funcctx;
	int		call_cntr;
	SQLRETURN	ret;
	odbcstmt	   *stmt;

	funcctx = SRF_PERCALL_SETUP();

	call_cntr = funcctx->call_cntr;
	stmt = funcctx->user_fctx;

	ret = SQLFetch(stmt->hStmt);

	if (SQL_SUCCEEDED(ret))  /* do when there is more left to send */
	{
		Datum	   *values;
		bool	   *nulls;
		HeapTuple	tuple;
		int		i;

		if (!SQL_SUCCEEDED(ret))
		{
			get_sql_error(0, SQL_HANDLE_STMT, stmt);
			SQLFreeHandle(SQL_HANDLE_STMT, stmt->hStmt);
			elog(ERROR, "odbclink: unsuccessful SQLFetch call: %s", totalerrmsg);
		}

		values = palloc(stmt->cols * sizeof(Datum));
		nulls = palloc(stmt->cols * sizeof(bool));

		PG_TRY();
		{
			/*
			 * Some functions called by get_data() can throw an error,
			 * catch them and don't leak the STMT handle...
			 */
			for (i = 0; i < stmt->cols; i++)
				get_data(stmt, i + 1, &values[i], &nulls[i]);
		}
		PG_CATCH();
		{
			SQLFreeHandle(SQL_HANDLE_STMT, stmt->hStmt);
			PG_RE_THROW();
		}
		PG_END_TRY();

		tuple = heap_form_tuple(stmt->tupdesc, values, nulls);

		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		if (ret != SQL_NO_DATA)
		{
			get_sql_error(0, SQL_HANDLE_STMT, stmt);
			SQLFreeHandle(SQL_HANDLE_STMT, stmt->hStmt);
			elog(ERROR, "odbclink: unsuccessful SQLFetch call: %s", totalerrmsg);
		}
		SQLFreeHandle(SQL_HANDLE_STMT, stmt->hStmt);
		SRF_RETURN_DONE(funcctx);
	}
}

Datum
odbclink_query_n(PG_FUNCTION_ARGS)
{
	if (SRF_IS_FIRSTCALL())
	{
		int		i;
		char	   *query;

		i = PG_GETARG_INT32(0) - 1;
		if (!(i >= 0 && i < n_conn && conns[i].connected))
			elog(ERROR, "odbclink: no such connection");

		query = TextDatumGetCString(PG_GETARG_DATUM(1));

		init_query_common(fcinfo, i, query);
	}

	return query_common(fcinfo);
}

Datum
odbclink_query_dsn(PG_FUNCTION_ARGS)
{
	if (SRF_IS_FIRSTCALL())
	{
		int		i;
		char	   *dsn, *uid, *pwd;
		char	   *query;

		dsn = TextDatumGetCString(PG_GETARG_DATUM(0));
		uid = TextDatumGetCString(PG_GETARG_DATUM(1));
		pwd = TextDatumGetCString(PG_GETARG_DATUM(2));
		query = TextDatumGetCString(PG_GETARG_DATUM(3));

		i = find_conn_dsn(dsn, uid, pwd);
		if (i < 0)
			i = connect_dsn(dsn, uid, pwd);

		init_query_common(fcinfo, i, query);

		pfree(dsn); pfree(uid); pfree(pwd); pfree(query);
	}

	return query_common(fcinfo);
}

Datum
odbclink_query_connstr(PG_FUNCTION_ARGS)
{
	if (SRF_IS_FIRSTCALL())
	{
		int		i;
		char	   *connstr;
		char	   *query;

		connstr = TextDatumGetCString(PG_GETARG_DATUM(0));
		query = TextDatumGetCString(PG_GETARG_DATUM(1));

		i = find_conn_connstr(connstr);
		if (i < 0)
			i = connect_connstr(connstr);

		init_query_common(fcinfo, i, query);

		pfree(connstr); pfree(query);
	}

	return query_common(fcinfo);
}
