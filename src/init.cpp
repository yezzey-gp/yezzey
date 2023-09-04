

#include "init.h"

#include "offload_policy.h"
#include "yezzey_expire.h"

void YezzeyInitMetadata(void) {
  (void)YezzeyCreateOffloadPolicyRelation();
  (void)YezzeyCreateRelationExpireIndex();
  (void)YezzeyCreateVirtualIndex();
}