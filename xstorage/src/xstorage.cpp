extern "C" {
#include "postgres.h"

/*
 * Spinlocks are in PostgreSQL defined with the register storage class,
 * which is perfectly legal C but which generates a warning when compiled
 * as C++. Ignore this warning as it's a false positive in this case.
 */
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-register"
#endif

#include "access/extprotocol.h"
#include "access/xact.h"
#include "catalog/pg_exttable.h"
#include "catalog/pg_proc.h"
#include "fmgr.h"
#include "funcapi.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

#ifdef __clang__
#pragma clang diagnostic pop
#endif
}

#include "gpreader.h"
#include "gpwriter.h"

string s3extErrorMessage;

/*
 * To detect the query interruption event (triggered by user), we should
 * consider both QueryCancelPending variable and transaction status. Here
 * QueryCancelPending is not sufficient because it will be reset before the
 * extprotocol last call, then it is hard to distinguish normal exit/finish
 * from abnormal transaction abort.
 *
 * IsAbortInProgress() can also be reset by GPDB while downloading file, we
 * must save the state
 */
static bool queryCancelFlag = false;
bool S3QueryIsAbortInProgress(void) {
    bool queryIsBeingCancelled = QueryCancelPending || IsAbortInProgress();

    // We need a thread-safe way to query and set queryCancelFlag.
    // queryCancelFlag is set to TRUE for the first time when cancel signal is
    // detected. If cancel signal is already captured, value will not be
    // swapped and queryIsCancelledAlready is true.
    bool queryIsCancelledAlready =
        !__sync_bool_compare_and_swap(&queryCancelFlag, false, queryIsBeingCancelled);

    return queryIsBeingCancelled || queryIsCancelledAlready;
}

void *S3Alloc(size_t size) {
    return palloc(size);
}

/*
 * MemoryContext will be free automatically when query is completed or aborted.
 * So no need to call pfree() again unless it is really needed inside query execution.
 * Otherwise might free already cleaned memory context for failed query.
 *
 * Currently download/upload buffer are using memory from MemoryContext.
 */
void S3Free(void *p) {
    // pfree(p);
}

/*
 * Set up the thread signal mask, we don't want to run our signal handlers
 * in downloading and uploading threads.
 */
void MaskThreadSignals() {
    sigset_t sigs;

    if (pthread_equal(main_tid, pthread_self())) {
        S3ERROR("thread_mask is called from main thread!");
        return;
    }

    sigemptyset(&sigs);

    /* make our thread to ignore these signals (which should allow that they be
     * delivered to the main thread) */
    sigaddset(&sigs, SIGHUP);
    sigaddset(&sigs, SIGINT);
    sigaddset(&sigs, SIGTERM);
    sigaddset(&sigs, SIGALRM);
    sigaddset(&sigs, SIGUSR1);
    sigaddset(&sigs, SIGUSR2);

    pthread_sigmask(SIG_BLOCK, &sigs, NULL);
}

/*
 * Detect data format
 * used to set file extension on S3 in gpwriter.
 */
static const char *getFormatStr(FunctionCallInfo fcinfo) {
    Relation rel = EXTPROTOCOL_GET_RELATION(fcinfo);
    ExtTableEntry *exttbl = GetExtTableEntry(rel->rd_id);
    char fmtcode = exttbl->fmtcode;

    if (fmttype_is_text(fmtcode)) return "txt";
    if (fmttype_is_csv(fmtcode)) return "csv";
    return S3_DEFAULT_FORMAT;
}

bool hasHeader = false;

char eolString[EOL_CHARS_MAX_LEN + 1] = "\n";  // LF by default
typedef struct gpcloudResHandle {
    GPReader *gpreader;
    GPWriter *gpwriter;

    ResourceOwner owner; /* owner of this handle */

    struct gpcloudResHandle *next;
    struct gpcloudResHandle *prev;
} gpcloudResHandle;

// Linked list of opened "handles", which are allocated in TopMemoryContext, and tracked by resource
// owners.
static gpcloudResHandle *openedResHandles;

static bool isGpcloudResReleaseCallbackRegistered;

static gpcloudResHandle *createGpcloudResHandle(void) {
    gpcloudResHandle *resHandle;

    resHandle = (gpcloudResHandle *)MemoryContextAlloc(TopMemoryContext, sizeof(gpcloudResHandle));
    resHandle->gpreader = NULL;
    resHandle->gpwriter = NULL;

    resHandle->owner = CurrentResourceOwner;
    resHandle->next = openedResHandles;
    resHandle->prev = NULL;

    if (openedResHandles) {
        openedResHandles->prev = resHandle;
    }

    openedResHandles = resHandle;

    return resHandle;
}

static void destroyGpcloudResHandle(gpcloudResHandle *resHandle) {
    if (resHandle == NULL) return;

    /* unlink from linked list first */
    if (resHandle->prev)
        resHandle->prev->next = resHandle->next;
    else
        openedResHandles = resHandle->next;

    if (resHandle->next) {
        resHandle->next->prev = resHandle->prev;
    }

    if (resHandle->gpreader != NULL) {
        if (!reader_cleanup(&resHandle->gpreader)) {
            elog(WARNING, "Failed to cleanup gpcloud extension: %s", s3extErrorMessage.c_str());
        }
    }

    if (resHandle->gpwriter != NULL) {
        if (!writer_cleanup(&resHandle->gpwriter)) {
            elog(WARNING, "Failed to cleanup gpcloud extension: %s", s3extErrorMessage.c_str());
        }
    }

    thread_cleanup();
    pfree(resHandle);
}

/*
 * Close any open handles on abort.
 */
static void gpcloudAbortCallback(ResourceReleasePhase phase, bool isCommit, bool isTopLevel,
                                 void *arg) {
    gpcloudResHandle *curr;
    gpcloudResHandle *next;

    if (phase != RESOURCE_RELEASE_AFTER_LOCKS) return;

    next = openedResHandles;
    while (next) {
        curr = next;
        next = curr->next;

        if (curr->owner == CurrentResourceOwner) {
            if (isCommit)
                elog(WARNING, "gpcloud external table reference leak: %p still referenced", curr);

            destroyGpcloudResHandle(curr);
        }
    }
}