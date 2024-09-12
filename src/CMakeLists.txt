if (UNIX)
    target_compile_definitions(${CMAKE_PROJECT_NAME} PUBLIC LINUX)
endif()

target_include_directories(${CMAKE_PROJECT_NAME}
    PUBLIC ${PROJECT_SOURCE_DIR}/include
    PUBLIC ${OPENSSL_INCLUDE_DIR}
)

target_sources(${CMAKE_PROJECT_NAME} PUBLIC
    AsynWorker.cpp
    BatchTableWriter.cpp
    Compress.cpp
    Concurrent.cpp
    ConstantImp.cpp
    ConstantMarshall.cpp
    DBConnectionPoolImpl.cpp
    DFSChunkMeta.cpp
    DictionaryImp.cpp
    DolphinDB.cpp
    DolphinDBImp.cpp
    DomainImp.cpp
    EventHandler.cpp
    Exceptions.cpp
    Format.cpp
    Guid.cpp
    int128.cpp
    LZ4.cpp
    Matrix.cpp
    MultithreadedTableWriter.cpp
    ScalarImp.cpp
    SetImp.cpp
    SharedMem.cpp
    Streaming.cpp
    StreamingUtil.cpp
    SymbolBase.cpp
    SysIO.cpp
    TableImp.cpp
    TaskStatusMgmt.cpp
    Util.cpp
    Vector.cpp
)
