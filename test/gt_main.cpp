#include <gtest/gtest.h>
#include "config.h"

int main(int argc, char *argv[]){
    dolphindb::DLogger::SetMinLevel(default_level);
    dolphindb::DBConnection::initialize();
    testing::InitGoogleTest(&argc, argv);
    int res = RUN_ALL_TESTS();
    return res;
}
