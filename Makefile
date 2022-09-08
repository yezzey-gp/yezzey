# gpcontrib/yezzey/Makefile

override CFLAGS = -Wall -Wmissing-prototypes -Wpointer-arith -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -fexcess-precision=standard -fno-aggressive-loop-optimizations -Wno-unused-but-set-variable -Wno-address -Werror=implicit-fallthrough=3 -Wno-format-truncation -Wno-stringop-truncation -O3 -std=gnu99 -Werror=uninitialized -Werror=implicit-function-declaration -DGPBUILD



MODULE_big = yezzey

OBJS = \
	$(WIN32RES) \
	smgr.o yezzey.o

EXTENSION = yezzey
DATA = yezzey--1.0.sql
PGFILEDESC = "yezzey - extnernal storage tables offloading extension"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = gpcontrib/yezzey
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

cleanall:
	@-$(MAKE) clean # incase PGXS not included
	rm -f *.o *.so *.a
	rm -f *.gcov src/*.gcov src/*.gcda src/*.gcno
	rm -f src/*.o src/*.d bin/gpcheckcloud/*.o bin/gpcheckcloud/*.d test/*.o test/*.d test/*.a lib/*.o lib/*.d
