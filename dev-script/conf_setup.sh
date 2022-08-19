#conf_setup.sh

cd ../../gpAux/gpdemo/datadirs
for DIR in */; do
	ls $DIR*/postgresql.conf
	echo -e "shared_preload_libraries = 'yezzey.so'\nyezzey.S3_getter = 'wal-g st get %f %s --config $1'\nyezzey.S3_putter = 'wal-g st put %f %s --config $1 -f'\nyezzey.S3_prefix = '$DIR'" | tee -a `ls $DIR*/postgresql.conf` > /dev/null
done

