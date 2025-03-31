#include "Util.h"
#include <ctime>

namespace dolphindb {

bool Util::getLocalTime(time_t t, struct tm& result){
    return localtime_s(&result, &t) == 0;
}

}
