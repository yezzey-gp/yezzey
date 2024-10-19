
export GPHOME=/usr/local/gpdb/
source gpAux/gpdemo/gpdemo-env.sh
source /usr/local/gpdb/greenplum_path.sh



make clean && make -j32 install
make destroy-demo-cluster && make create-demo-cluster
gpconfig -c yezzey.storage_prefix -v "'wal-e/6'"
gpconfig -c yezzey.storage_bucket -v "'gpyezzey'"

gpconfig -c yezzey.yproxy_socket -v "'/tmp/yproxy.sock'"
gpconfig -c yezzey.use_gpg_crypto -v "true"

gpconfig -c max_worker_processes -v 10

gpconfig -c yezzey.autooffload -v  "on"

gpconfig -c shared_preload_libraries -v yezzey

gpstop -a -i && gpstart -a


createdb $USER
