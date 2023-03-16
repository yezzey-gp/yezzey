#ifndef YEZZEY_GUCS_H
#define YEZZEY_GUCS_H

extern int yezzey_log_level;
extern int yezzey_ao_log_level;

/* ----- GPG ------ */
extern char *gpg_key_id;
extern char *gpg_engine_path;

extern bool use_gpg_crypto;

/* ----- STORAGE -----  */
extern char *storage_prefix;
extern char *storage_bucket;
extern char *storage_config;
extern char *storage_host;

/* WAL-G */

extern char *walg_bin_path;

#endif /* YEZZEY_GUCS_H */