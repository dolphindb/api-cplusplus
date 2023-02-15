#include "config.h"
#include "gtest/gtest.h"

// ----------------------api unitTest------------------------
#include "DolphinDBTest_gtest.cpp"
#include "DBConnectionTest_gtest.cpp"
#include "ConstantMarshall_gtest.cpp"
#include "ExceptionTest_gtest.cpp"
#include "ScalarTest_gtest.cpp"
#include "DataformDictionaryTest_gtest.cpp"
#include "DataformMatrixTest_gtest.cpp"
#include "DataformPairTest_gtest.cpp"
#include "DataformSetTest_gtest.cpp"
#include "DataformTableTest_gtest.cpp"
#include "DataformVectorTest_gtest.cpp"
#include "SysIOTest_gtest.cpp"
#include "FunctionTest_gtest.cpp"
#include "DolphinDBTestINDEX_MAX_gtest.cpp"
#include "PartitionedTableAppenderTest_gtest.cpp"
#include "AutoFitTableAppenderTest_gtest.cpp"
#include "AutoFitTableUpsertTest_gtest.cpp"
#include "BatchTableWriter_gtest.cpp"
#include "MultithreadedTableWriter_gtest.cpp"

// ------------DolphinDB server version >2.00.xx-------------
#include "ArrayVectorTest_gtest.cpp"
#include "CompressTest_gtest.cpp"

// ---------------------streaming api-----------------------
#include "StreamingDeserilizerTester_gtest.cpp"
#include "StreamingPollingClientTester_gtest.cpp"
#include "StreamingThreadedClientTester_gtest.cpp"
#include "StreamingThreadPooledClientTester_gtest.cpp"
#include "StreamingSubscribeHAstreamTableTest_gtest.cpp"

// -------------------IPCM test---------------------
#include "IPCinMemoryTableTest_gtest.cpp"


int main(int argc, char *argv[]){
    // DBConnection::initialize();
    testing::InitGoogleTest(&argc, argv);
    // test filter example:
    // ?	单个字符
    // *	任意字符
    // -	排除，如，-a 表示除了a
    // :	取或，如，a:b 表示a或b
    // ::testing::GTEST_FLAG(filter) = "*Counter*:*DolphinDBTest.*huge*:ScalarTest.testGuid";
    int res = RUN_ALL_TESTS();
    return EXIT_SUCCESS;
}
