#include "config.h"
#include "gtest/gtest.h"
// #include "../test/DolphinDBTest_gtest.cpp"
#include "../test/DolphinDBTester_gtest.cpp"
// #include "../test/DolphinDBTestINDEX_MAX_gtest.cpp"
// #include "../test/MultithreadedTableWriter_gtest.cpp"
// #include "../test/streamingCheckNum_gtest.cpp"
// #include "../test/streamingMulThreadMoreTables_gtest.cpp"
// #include "../test/streamingMultithreadSubUnsub_gtest.cpp"
// #include "../test/StreamingPollingClientTester_gtest.cpp"
// #include "../test/streamingPressureSub_gtest.cpp"
// #include "../test/StreamingThreadedClientTester_gtest.cpp"
// #include "../test/StreamingThreadPooledClientTester_gtest.cpp"

// // DolphinDB server 200以上版本支持以下cases
// #include "../test/ArrayVectorTest_gtest.cpp"  
// #include "../test/CompressTest_gtest.cpp" 

// // api performancetest
// #include "../test/threadperformenceTest.cpp"

int main(int argc, char *argv[]){
    DBConnection::initialize();
    bool ret = conn.connect(hostName,port,"admin", "123456");
    bool ret1 = conn_compress.connect(hostName,port,"admin", "123456");
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
