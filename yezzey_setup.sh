#/bin/bash

rm -fr ./pgdata

rm -f logfile

initdb -D ./pgdata
echo yezzey.S3_putter = 'lol' >> ./pgdata/postgresql.conf
echo yezzey.S3_getter = 'kek' >> ./pgdata/postgresql.conf
pg_ctl -D ./pgdata -l logfile -o '--port=5433' start


psql "host=localhost port=5433 dbname=postgres" -c 'CREATE EXTENSION yezzey';

sed -i "s/#shared_preload_libraries = ''/shared_preload_libraries = 'yezzey.so'/g" ./pgdata/postgresql.conf
pg_ctl -D ./pgdata -l logfile -o '--port=5433' restart
