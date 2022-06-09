#/bin/bash

DB_PATH=/home/reshke/pgdata
pg_ctl -D $DB_PATH -o '--port=5432' stop
rm -fr $DB_PATH

rm -f $DB_PATH/logfile

initdb -D $DB_PATH
echo yezzey.S3_putter = \'cp -f %f /home/reshke/s3/%s \' >> $DB_PATH/postgresql.conf
echo yezzey.S3_getter = \'cp /home/reshke/s3/%f %s\' >> $DB_PATH/postgresql.conf
pg_ctl -D $DB_PATH -l $DB_PATH/logfile -o '--port=5432' start

psql "host=localhost port=5433 dbname=postgres" -c 'CREATE EXTENSION yezzey';

sed -i "s/#shared_preload_libraries = ''/shared_preload_libraries = 'yezzey.so'/g" $DB_PATH/postgresql.conf
pg_ctl -D $DB_PATH -l $DB_PATH/logfile -o '--port=5432' restart
