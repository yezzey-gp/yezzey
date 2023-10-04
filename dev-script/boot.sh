
export GPHOME=/usr/local/gpdb/
source gpAux/gpdemo/gpdemo-env.sh
source /usr/local/gpdb/greenplum_path.sh



make clean && make -j32 install
make destroy-demo-cluster && make create-demo-cluster
gpconfig -c yezzey.storage_prefix -v "'wal-e/6'"
gpconfig -c yezzey.storage_bucket -v "'gpyezzey'"
gpconfig -c yezzey.storage_config -v "'/home/reshke/s3test.conf'"
gpconfig -c yezzey.storage_host -v "'https://storage.yandexcloud.net'"
gpconfig -c yezzey.use_gpg_crypto -v "true"
gpconfig -c yezzey.gpg_key_id -v  "'400C834648CE54F9'"
gpconfig -c yezzey.walg_bin_path -v  "'/home/reshke/wal-g/main/gp/wal-g'"
gpconfig -c yezzey.walg_config_path -v  "'/home/reshke/wal-g/wal-g-yezzey.yaml'"

gpconfig -c max_worker_processes -v 10

gpconfig -c yezzey.autooffload -v  "on"

gpconfig -c shared_preload_libraries -v yezzey

gpstop -a -i && gpstart -a


createdb $USER
