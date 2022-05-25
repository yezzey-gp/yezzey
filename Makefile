# contrib/yezzey/Makefile

MODULE_big = yezzey

OBJS = \
	$(WIN32RES) \
	yezzey.o f_yezzey_smgr.o

EXTENSION = yezzey
DATA = yezzey--1.0.sql
PGFILEDESC = "yezzey - something not so yezzey for gpdb"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/yezzey
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
