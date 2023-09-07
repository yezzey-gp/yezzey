# gpcontrib/yezzey/Makefile

override CFLAGS = -Wall -Wmissing-prototypes -Wpointer-arith -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -fexcess-precision=standard -fno-aggressive-loop-optimizations -Wno-unused-but-set-variable -Wno-address -Wno-format-truncation -Wno-stringop-truncation -g -ggdb -std=gnu99 -Werror=uninitialized -Werror=implicit-function-declaration -DGPBUILD

# -Werror=implicit-fallthrough=3

override CXX = g++-8

COMMON_OBJS = \
	gpreader.o gpwriter.o gpcleaner.o \
	s3conf.o s3utils.o s3log.o s3url.o \
	s3http_headers.o s3interface.o s3restful_service.o \
	s3bucket_reader.o s3common_reader.o s3common_writer.o \
	decompress_reader.o compress_writer.o s3key_reader.o s3key_writer.o \
	xstorage.o

COMMON_LINK_OPTIONS = -lstdc++ -lxml2 -lpthread -lcrypto -lcurl -lz -lgpgme -lstdc++fs

COMMON_CPP_FLAGS = -std=c++11 -fPIC -I/usr/include/libxml2 -I/usr/local/opt/openssl/include -DENABLE_NLS -DHAVE_CRYPTO

override CPPFLAGS = -fPIC -lstdc++fs -lstdc++ -lxml2 -lpthread -lcrypto -lcurl -lgpgme -lz -g3 -ggdb -Wall -Wpointer-arith -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -fno-aggressive-loop-optimizations -Wno-unused-but-set-variable -Wno-address -Werror=format-security -Wno-format-truncation -Wno-stringop-truncation -g -ggdb -std=c++11 -fPIC -I/usr/include/libxml2 -I/usr/local/opt/openssl/include -Iinclude -Ilib -I../../src/interfaces/libpq -I../../src/interfaces/libpq/postgresql/server/utils -g -I. -I. -I../../src/include -D_GNU_SOURCE -I/usr/include/libxml2 -I./xstorage/include -I./xstorage/lib  -DGPBUILD


SHLIB_LINK += $(COMMON_LINK_OPTIONS)
PG_CPPFLAGS += $(COMMON_CPP_FLAGS) -I./include -Iinclude -Ilib -I$(libpq_srcdir) -I$(libpq_srcdir)/postgresql/server/utils


MODULE_big = yezzey

OBJS = \
	$(WIN32RES) \
	src/storage.o src/proxy.o src/encrypted_storage_reader.o \
	src/storage_lister.o \
	src/encrypted_storage_writer.o  src/io_adv.o src/io.o src/crypto.o  \
	src/x_reader.o src/x_writer.o \
	src/virtual_index.o \
	src/walg_reader.o \
	src/walg_writer.o \
	src/walg_st_reader.o \
	src/util.o \
	src/url.o \
	src/worker.o \
	src/offload_policy.o \
	src/offload.o \
	src/virtual_tablespace.o \
	src/partition.o \
	src/xvacuum.o \
	src/yezzey_expire.o \
	src/init.o \
	src/meta.o \
	src/json.o \
	smgr.o worker.o yezzey.o \
	./xstorage/lib/http_parser.o ./xstorage/lib/ini.o $(addprefix ./xstorage/src/,$(COMMON_OBJS))

EXTENSION = yezzey
DATA = yezzey--1.0.sql
PGFILEDESC = "yezzey - external storage tables offloading extension"

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
	rm -f src/*.o src/*.d


apply_fmt:
	clang-format -i ./src/*.cpp ./include/*.h

format:
	@-[ -n "`command -v dos2unix`" ] && dos2unix -k -q src/*.cpp bin/gpcheckcloud/*.cpp test/*.cpp include/*.h
	@-[ -n "`command -v clang-format`" ] && clang-format -style="{BasedOnStyle: Google, IndentWidth: 4, ColumnLimit: 100, AllowShortFunctionsOnASingleLine: None}" -i src/*.cpp bin/gpcheckcloud/*.cpp test/*.cpp include/*.h
