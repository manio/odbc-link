# $PostgreSQL: pgsql/contrib/tablefunc/Makefile,v 1.9 2007/11/10 23:59:51 momjian Exp $

MODULE_big = odbclink
DATA_built = odbclink.sql
DATA = uninstall_odbclink.sql
OBJS = odbclink.o
SHLIB_LINK = -lodbc

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/odbclink
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
