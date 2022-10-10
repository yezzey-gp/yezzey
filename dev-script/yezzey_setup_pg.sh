#/bin/bash

DB_PATH=./pgdata
pg_ctl -D $DB_PATH -o '--port=5432' stop
rm -fr $DB_PATH

rm -f $DB_PATH/logfile

initdb -D $DB_PATH
echo yezzey.S3_putter = \'wal-g st put %f %s --config /etc/wal-g/wal-g.yaml\' >> $DB_PATH/postgresql.conf
echo yezzey.S3_getter = \'wal-g st get %f %s --config /etc/wal-g/wal-g.yaml\' >> $DB_PATH/postgresql.conf
pg_ctl -D $DB_PATH -l $DB_PATH/logfile -o '--port=5432' start

psql "host=localhost port=5432 dbname=postgres" -c 'CREATE EXTENSION yezzey';

sed -i "s/#shared_preload_libraries = ''/shared_preload_libraries = 'yezzey.so'/g" $DB_PATH/postgresql.conf
pg_ctl -D $DB_PATH -l $DB_PATH/logfile -o '--port=5432' restart
