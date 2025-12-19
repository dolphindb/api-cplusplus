if (UNIX)
    target_sources(${CMAKE_PROJECT_NAME} PRIVATE
        platform/Linux.cpp
    )
elseif (WIN32)
    target_sources(${CMAKE_PROJECT_NAME} PRIVATE
        platform/Windows.cpp
    )
elseif (APPLE)
    target_compile_definitions(${CMAKE_PROJECT_NAME} PRIVATE MAC)
endif()

target_include_directories(${CMAKE_PROJECT_NAME} PUBLIC ${PROJECT_SOURCE_DIR}/include)

target_sources(${CMAKE_PROJECT_NAME} PRIVATE
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
    Format.cpp
    Guid.cpp
    int128.cpp
    Logger.cpp
    Matrix.cpp
    MultithreadedTableWriter.cpp
    ScalarImp.cpp
    SetImp.cpp
    Streaming.cpp
    StreamingUtil.cpp
    StreamReplicator.cpp
    SymbolBase.cpp
    SysIO.cpp
    TableImp.cpp
    TaskStatusMgmt.cpp
    Types.cpp
    Util.cpp
    Vector.cpp
)

if (USE_OPENSSL AND OPENSSL_FOUND)
target_sources(${CMAKE_PROJECT_NAME} PRIVATE Crypto.cpp)
endif()

add_subdirectory(lz4)
