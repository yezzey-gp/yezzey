# gpcontrib/yezzey/Makefile

override CFLAGS = -Wall -Wmissing-prototypes -Wpointer-arith -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -fexcess-precision=standard -fno-aggressive-loop-optimizations -Wno-unused-but-set-variable -Wno-address -Wno-format-truncation -Wno-stringop-truncation -g -ggdb -std=gnu99 -Werror=uninitialized -Werror=implicit-function-declaration -DGPBUILD

# -Werror=implicit-fallthrough=3

COMMON_OBJS = gpreader.o gpwriter.o s3conf.o s3utils.o s3log.o s3url.o s3http_headers.o s3interface.o s3restful_service.o s3bucket_reader.o s3common_reader.o s3common_writer.o decompress_reader.o compress_writer.o s3key_reader.o s3key_writer.o

COMMON_LINK_OPTIONS = -lstdc++ -lxml2 -lpthread -lcrypto -lcurl -lz -lmd5

COMMON_CPP_FLAGS = -std=c++11 -fPIC -I/usr/include/libxml2 -I/usr/local/opt/openssl/include

override CPPFLAGS = -fPIC -lstdc++ -lxml2 -lpthread -lcrypto -lcurl -lz -g3 -ggdb -Wall -Wpointer-arith -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -fno-aggressive-loop-optimizations -Wno-unused-but-set-variable -Wno-address -Wno-format-truncation -Wno-stringop-truncation -g -ggdb -O3 -std=c++11 -fPIC -I/usr/include/libxml2 -I/usr/local/opt/openssl/include -Iinclude -Ilib -I../../src/interfaces/libpq -I../../src/interfaces/libpq/postgresql/server/utils -g -I. -I. -I../../src/include -D_GNU_SOURCE -I/usr/include/libxml2 -I../gpcloud_modified_for_yezzey/include -I../gpcloud_modified_for_yezzey/lib  -DGPBUILD

MODULE_big = yezzey
#Theseus

OBJS = \
	$(WIN32RES) \
	smgr.o worker.o yezzey.o external_storage.o proxy.o smgr_s3.o  ../gpcloud_modified_for_yezzey/src/gpcloud.o ../gpcloud_modified_for_yezzey/lib/http_parser.o ../gpcloud_modified_for_yezzey/lib/ini.o $(addprefix ../gpcloud_modified_for_yezzey/src/,$(COMMON_OBJS))

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


format:
	@-[ -n "`command -v dos2unix`" ] && dos2unix -k -q src/*.cpp bin/gpcheckcloud/*.cpp test/*.cpp include/*.h
	@-[ -n "`command -v clang-format`" ] && clang-format -style="{BasedOnStyle: Google, IndentWidth: 4, ColumnLimit: 100, AllowShortFunctionsOnASingleLine: None}" -i src/*.cpp bin/gpcheckcloud/*.cpp test/*.cpp include/*.h
