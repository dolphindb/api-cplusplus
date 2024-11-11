if (UNIX)
    target_compile_definitions(${CMAKE_PROJECT_NAME} PUBLIC LINUX)
else()
    target_compile_definitions(${CMAKE_PROJECT_NAME} PUBLIC WINDOWS)
endif()

target_include_directories(${CMAKE_PROJECT_NAME}
    PUBLIC ${PROJECT_SOURCE_DIR}/include
)

target_sources(${CMAKE_PROJECT_NAME} PRIVATE
    AsynWorker.cpp
    BatchTableWriter.cpp
    Compress.cpp
    Concurrent.cpp
    ConstantImp.cpp
    ConstantMarshall.cpp
    Crypto.cpp
    DBConnectionPoolImpl.cpp
    DFSChunkMeta.cpp
    DictionaryImp.cpp
    DolphinDB.cpp
    DolphinDBImp.cpp
    DomainImp.cpp
    EventHandler.cpp
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
    Types.cpp
    Util.cpp
    Vector.cpp
)
