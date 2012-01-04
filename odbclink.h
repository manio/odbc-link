#ifndef ODBCLINK_H
#define ODBCLINK_H

#include <sql.h>
#include <sqlext.h>

typedef struct {
	int	connected;
	char	   *dsn, *uid, *pwd;
	char	   *connstr;
	SQLHENV	hEnv;
	SQLHDBC	hCon;
} odbcconn;

typedef struct {
	TupleDesc	tupdesc;
	SQLHSTMT	hStmt;
	SQLSMALLINT	cols;
	int		conn_idx;
} odbcstmt;

#define CONNCHUNK	(4)

#define CHARVALCHUNK	(4096)

extern void  _PG_init(void);
extern void  _PG_fini(void);
extern Datum odbclink_connect(PG_FUNCTION_ARGS);
extern Datum odbclink_driverconnect(PG_FUNCTION_ARGS);
extern Datum odbclink_connections(PG_FUNCTION_ARGS);
extern Datum odbclink_disconnect(PG_FUNCTION_ARGS);
extern Datum odbclink_query_n(PG_FUNCTION_ARGS);
extern Datum odbclink_query_dsn(PG_FUNCTION_ARGS);
extern Datum odbclink_query_connstr(PG_FUNCTION_ARGS); 
extern Datum odbclink_exec_n(PG_FUNCTION_ARGS);
extern Datum odbclink_exec_dsn(PG_FUNCTION_ARGS);
extern Datum odbclink_exec_connstr(PG_FUNCTION_ARGS); 

#endif
