#include "Util.h"
#include "time.h"

namespace dolphindb {

bool Util::getLocalTime(time_t t, struct tm& result){
    return localtime_r(&t, &result) != nullptr;
}

}
