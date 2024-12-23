#include "Types.h"
#include <string>
#include <map>

namespace dolphindb {

std::string getDataTypeName(DATA_TYPE type)
{
    static std::map<DATA_TYPE, std::string> arrTypeStr {
        {DT_VOID, "VOID"},
        {DT_BOOL, "BOOL"},
        {DT_CHAR, "CHAR"},
        {DT_SHORT, "SHORT"},
        {DT_INT, "INT"},
        {DT_LONG, "LONG"},
        {DT_DATE, "DATE"},
        {DT_MONTH, "MONTH"},
        {DT_TIME, "TIME"},
        {DT_MINUTE, "MINUTE"},
        {DT_SECOND, "SECOND"},
        {DT_DATETIME, "DATETIME"},
        {DT_TIMESTAMP, "TIMESTAMP"},
        {DT_NANOTIME, "NANOTIME"},
        {DT_NANOTIMESTAMP, "NANOTIMESTAMP"},
        {DT_FLOAT, "FLOAT"},
        {DT_DOUBLE, "DOUBLE"},
        {DT_SYMBOL, "SYMBOL"},
        {DT_STRING, "STRING"},
        {DT_UUID, "UUID"},
        {DT_FUNCTIONDEF, "FUNCTIONDEF"},
        {DT_HANDLE, "HANDLE"},
        {DT_CODE, "CODE"},
        {DT_DATASOURCE, "DATASOURCE"},
        {DT_RESOURCE, "RESOURCE"},
        {DT_ANY, "ANY"},
        {DT_COMPRESS, "COMPRESS"},
        {DT_DICTIONARY, "DICTIONARY"},
        {DT_DATEHOUR, "DATEHOUR"},
        {DT_DATEMINUTE, "DATEMINUTE"},
        {DT_IP, "IPADDR"},
        {DT_INT128, "INT128"},
        {DT_BLOB, "BLOB"},
        {DT_COMPLEX, "COMPLEX"},
        {DT_POINT, "POINT"},
        {DT_DURATION, "DURATION"},
        {DT_DECIMAL32, "DECIMAL32"},
        {DT_DECIMAL64, "DECIMAL64"},
        {DT_DECIMAL128, "DECIMAL128"},
        {DT_OBJECT, "OBJECT"},
        {DT_IOTANY, "IOTANY"},
    };
    if(type >= 0 && type < TYPE_COUNT)
        return arrTypeStr[type];
    else if(type >= ARRAY_TYPE_BASE) {
        return getDataTypeName((DATA_TYPE)(type-ARRAY_TYPE_BASE))+"[]";
    }else{
        return "Uknown data type " + std::to_string(type);
    }
}

}
