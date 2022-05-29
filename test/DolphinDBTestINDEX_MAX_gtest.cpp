#include "DolphinDBTestINDEX_MAX.h"
#include "../include/DolphinDB.h"
#include "../include/Util.h"
#include "Streaming.h"
#include <vector>
#include <limits.h>
#include <thread>
#include <atomic>
#include <stdio.h>
#include "../include/googletest/include/gtest/gtest.h"


using std::endl;
using std::cout;



TEST(DolphinDBTestINDEX_MAX,test_DolphinDBTestINDEX_MAX){
    dolphindb::DBConnection::initialize();
    int dolphindbINDEX_MAX = dolphindb::INDEX_MAX;
    int dolphindbINDEX_MIN = dolphindb::INDEX_MIN;
    int constINDEX_MAX = INDEX_MAX_1;
    int constINDEX_MIN = INDEX_MIN_2;
    EXPECT_EQ(dolphindbINDEX_MAX, INT_MAX);
    EXPECT_EQ(dolphindbINDEX_MIN, INT_MIN);
    EXPECT_EQ(constINDEX_MAX, (int)1);
    EXPECT_EQ(constINDEX_MIN, (int)-1);
}
