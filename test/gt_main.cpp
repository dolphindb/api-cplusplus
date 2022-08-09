#include "config.h"
#include "gtest/gtest.h"

// ----------------------api unitTest------------------------
// #include "DolphinDBTest_gtest.cpp"
// #include "ExceptionTest_gtest.cpp"
// #include "ScalarTest_gtest.cpp"
// #include "DataformDictionaryTest_gtest.cpp"
// #include "DataformMatrixTest_gtest.cpp"
// #include "DataformPairTest_gtest.cpp"
// #include "DataformSetTest_gtest.cpp"
// #include "DataformTableTest_gtest.cpp"
#include "DataformVectorTest_gtest.cpp"
// #include "FunctionTest_gtest.cpp"
// #include "DolphinDBTestINDEX_MAX_gtest.cpp"
// #include "PartitionedTableAppenderTest_gtest.cpp"
// #include "AutoFitTableAppenderTest_gtest.cpp"
// #include "AutoFitTableUpsertTest_gtest.cpp"
// #include "BatchTableWriter_gtest.cpp"
// #include "MultithreadedTableWriter_gtest.cpp"

// ------------DolphinDB server version >2.00.xx-------------
// #include "ArrayVectorTest_gtest.cpp"
// #include "CompressTest_gtest.cpp"

// ---------------------streaming api-----------------------
// #include "streamingDeserilizerTester_gtest.cpp"
// #include "StreamingPollingClientTester_gtest.cpp"
// #include "StreamingThreadedClientTester_gtest.cpp"
// #include "StreamingThreadPooledClientTester_gtest.cpp"
// #include "streamingSubscribeHighAvailableTest_gtest.cpp"

// -------------------IPCM test---------------------
// #include "IPCinMemoryTableTest_gtest.cpp"

// ---------------------api Connect test-----------------------
// #include "DBConnectHighAvailableTest_gtest.cpp"
// #include "HightlyConcurrentConnTest_gtest.cpp"
// #include "DBConnectReconnectTest_gtest.cpp"

// ------------DolphinDB server version >2.10.xx---------------
// #include "DBConnectPythonTest_gtest.cpp"

// ------------------api perf test------------------
// #include "threadperformenceTest.cpp"

int main(int argc, char *argv[]){
    // DBConnection::initialize();
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
