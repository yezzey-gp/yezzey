# contrib/yezzey/Makefile

override CFLAGS = -Wall -Wmissing-prototypes -Wpointer-arith -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -fexcess-precision=standard -fno-aggressive-loop-optimizations -Wno-unused-but-set-variable -Wno-address -Werror=implicit-fallthrough=3 -Wno-format-truncation -Wno-stringop-truncation -O3 -std=gnu99 -Werror=uninitialized -Werror=implicit-function-declaration -DGPBUILD



MODULE_big = yezzey

OBJS = \
	$(WIN32RES) \
	f_yezzey_smgr.o yezzey.o

EXTENSION = yezzey
DATA = yezzey--1.0.sql
PGFILEDESC = "yezzey - something not so yezzey for gpdb"

ifdef USE_PGXS
PG_CONFIG = "/home/fstilus/gpdb_src/src/bin/pg_config"
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/yezzey
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
