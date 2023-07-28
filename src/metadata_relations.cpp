#include "metadata.h"
#include "offload_policy.h"
#include "virtual_index.h"

void YezzeyCreateMetadataRelations() {
  /* if any error, elog(ERROR, ...) is called
   * and due to magic of MVCC everything is rolled back
   * and no additional job needed.
   */
  (void)YezzeyCreateOffloadMetadataRelation();
  (void)YezzeyCreateAuxIndex();
}