target_include_directories(${CMAKE_PROJECT_NAME}_test
    PUBLIC ${PROJECT_SOURCE_DIR}/include/googletest/googletest/include
    PUBLIC ${OPENSSL_INCLUDE_DIR}
)

target_sources(${CMAKE_PROJECT_NAME}_test PUBLIC
    gt_main.cpp
    ArrayVectorTest_gtest.cpp
    AutoFitTableAppenderTest_gtest.cpp
    AutoFitTableUpsertTest_gtest.cpp
    BatchTableWriter_gtest.cpp
    CEPEventTest_gtest.cpp
    CompressTest_gtest.cpp
    config.cpp
    ConstantMarshall_gtest.cpp
    DataformDictionaryTest_gtest.cpp
    DataformMatrixTest_gtest.cpp
    DataformPairTest_gtest.cpp
    DataformSetTest_gtest.cpp
    DataformTableTest_gtest.cpp
    DataformVectorTest_gtest.cpp
    DBConnectionTest_gtest.cpp
    DolphinDBTest_gtest.cpp
    DolphinDBTestINDEX_MAX_gtest.cpp
    ExceptionTest_gtest.cpp
    FunctionTest_gtest.cpp
    IPCinMemoryTableTest_gtest.cpp
    MultithreadedTableWriter_gtest.cpp
    PartitionedTableAppenderTest_gtest.cpp
    ScalarTest_gtest.cpp
    SqlTest_gtest.cpp
    StreamingDeserilizerTester_gtest.cpp
    StreamingPollingClientTester_gtest.cpp
    StreamingSubscribeHAstreamTableTest_gtest.cpp
    StreamingThreadedClientTester_gtest.cpp
    StreamingThreadPooledClientTester_gtest.cpp
    SysIOTest_gtest.cpp
)
