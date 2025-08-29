#include <gtest/gtest.h>
#include "config.h"
#include <algorithm>

#ifdef _WIN32
    #include <windows.h>
    #define GET_PID GetCurrentProcessId
#else
    #define GET_PID getpid
#endif

// single
const std::string HOST = "192.168.0.54";
const int PORT = 8848;
const std::string USER = "admin";
const std::string PASSWD = "123456";

// cluster
const std::string HOST_CLUSTER = "192.168.0.54";
const int PORT_CONTROLLER = 9900;
const int PORT_AGENT = 9901;
const int PORT_DNODE1 = 9902;
const int PORT_DNODE2 = 9903;
const int PORT_DNODE3 = 9904;
const int PORT_CNODE1 = 9905;
const std::string USER_CLUSTER = "admin";
const std::string PASSWD_CLUSTER = "123456";

std::vector<std::string> sites = {"192.168.0.54:9902:dnode9902", "192.168.0.54:9903:dnode9903", "192.168.0.54:9904:dnode9904"};
std::vector<std::string> nodeNames = {"dnode9902", "dnode9903", "dnode9904"};
dolphindb::DLogger::Level default_level = dolphindb::DLogger::LevelCount;

std::string getRandString(int len){
    std::string alphas = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    unsigned int seed = (unsigned)time(NULL) ^ (unsigned)GET_PID();
    srand(seed);
    std::string str;
    for (int i = 0; i < len; i++)
    {
        str += alphas[rand() % alphas.size()];
    }
    return str;
}

std::string getCaseName(){
    const auto* test_info = testing::UnitTest::GetInstance()->current_test_info();
    std::string case_suite = test_info->test_suite_name();
    std::replace(case_suite.begin(), case_suite.end(), '/', '_');
    std::string case_name =  test_info->name();
    std::replace(case_name.begin(), case_name.end(), '/', '_');
    return case_suite + "_" + case_name;
}

std::string getCaseNameHash(){
    const auto* test_info = testing::UnitTest::GetInstance()->current_test_info();
    std::string case_suite = test_info->test_suite_name();
    std::replace(case_suite.begin(), case_suite.end(), '/', '_');
    std::string case_name =  test_info->name();
    std::replace(case_name.begin(), case_name.end(), '/', '_');
    std::string input = case_name + "_" + case_name;
    std::hash<std::string> hasher;
    size_t hashValue = hasher(input);
    std::stringstream ss;
    ss << std::hex << std::setw(16) << std::setfill('0') << hashValue;
    return "case_"+ss.str().substr(0, 16);
}

dolphindb::TableSP AnyVectorToTable(dolphindb::VectorSP vec)
{
    std::vector<std::string> colNames;
    std::vector<dolphindb::ConstantSP> columnVecs;
    auto col_count = vec->size();
    for(auto i=0;i<col_count;i++){
        colNames.emplace_back("col"+ std::to_string(i));
    }

    columnVecs.reserve(col_count);
    for (auto i=0;i<col_count;i++)
    {
        dolphindb::DATA_FORM form = vec->get(i)->getForm();
        dolphindb::DATA_TYPE _t = vec->get(i)->getType();
        dolphindb::DATA_TYPE type = form == dolphindb::DF_VECTOR && _t < dolphindb::ARRAY_TYPE_BASE ? static_cast<dolphindb::DATA_TYPE>(_t+dolphindb::ARRAY_TYPE_BASE):_t;
        int extraParam = vec->get(i)->getExtraParamForType();
        if (vec->get(i)->getForm() == dolphindb::DF_VECTOR){
            dolphindb::VectorSP avCol = dolphindb::Util::createArrayVector(type, 0, 0, true, extraParam);
            avCol->append(vec->get(i));
            columnVecs.emplace_back(avCol);
        }else{
            dolphindb::VectorSP col = dolphindb::Util::createVector(type, 0, 0, true, extraParam);
            for (auto j=0;j<vec->get(i)->rows();j++)
            {
                col->append(vec->get(i)->get(j));
            }
            columnVecs.emplace_back(col);
        }
    }

    return dolphindb::Util::createTable(colNames, columnVecs);
}