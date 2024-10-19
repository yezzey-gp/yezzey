
export GPHOME=/usr/local/gpdb/
source gpAux/gpdemo/gpdemo-env.sh
source /usr/local/gpdb/greenplum_path.sh



make clean && make -j32 install
make destroy-demo-cluster && make create-demo-cluster
gpconfig -c yezzey.storage_prefix -v "'wal-e/mdbrhqjnl6k5duk7loi2/6'"
gpconfig -c yezzey.storage_bucket -v "'gptest'"
gpconfig -c yezzey.storage_config -v "'/home/reshke/s3test.conf'"
gpconfig -c yezzey.storage_host -v "'https://storage.yandexcloud.net'"

gpconfig -c max_worker_processes -v 10

gpconfig -c yezzey.autooffload -v  "on"

gpconfig -c shared_preload_libraries -v yezzey

gpstop -a -i && gpstart -a


createdb $USER
psql postgres -f ./gpcontrib/yezzey/test/regress/yezzey.sql
