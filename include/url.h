
#include "io_adv.h"
#include "util.h"
#include "ylister.h"

std::string craftUrl(const std::shared_ptr<YLister> &lister,
                     const std::shared_ptr<IOadv> &adv, ssize_t segindx,
                     ssize_t modcount);

std::string craftStoragePath(const std::shared_ptr<YLister> &lister,
                             const std::shared_ptr<IOadv> &adv, ssize_t segindx,
                             ssize_t modcount, const std::string &prefix);