#/bin/bash

pg_ctl -D /home/fstilus/pg_build/e -o '--port=5432' stop
rm -fr /home/fstilus/pg_build/e

rm -f /home/fstilus/pg_build/postgres/l0

initdb -D /home/fstilus/pg_build/e
echo yezzey.S3_putter = \'wal-g st put %f %s --config /home/fstilus/config.yaml -f\' >> /home/fstilus/pg_build/e/postgresql.conf
echo yezzey.S3_getter = \'wal-g st get %f %s --config /home/fstilus/config.yaml\' >> /home/fstilus/pg_build/e/postgresql.conf
pg_ctl -D /home/fstilus/pg_build/e -l /home/fstilus/pg_build/postgres/l0 -o '--port=5432' start


psql "host=localhost port=5432 dbname=postgres" -c 'CREATE EXTENSION yezzey';

sed -i "s/#shared_preload_libraries = ''/shared_preload_libraries = 'yezzey.so'/g" /home/fstilus/pg_build/e/postgresql.conf
pg_ctl -D /home/fstilus/pg_build/e -l /home/fstilus/pg_build/postgres/l0 -o '--port=5432' restart
