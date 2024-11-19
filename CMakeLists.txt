cmake_minimum_required(VERSION 3.13)

project(DolphinDBAPI)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_EXTENSIONS OFF)

option(USE_OPENSSL "Use OpenSSL for encrypted communication." ON)
option(USE_AERON "Use Aeron for UDP connections. ${CMAKE_PROJECT_NAME} will search for Aeron in $\{CMAKE_INSTALL_PREFIX\} by default, you can specify a different location with AERON_INSTALL_DIR" ON)
option(WITH_GTEST "Compile testcases." OFF)

add_library(${CMAKE_PROJECT_NAME} SHARED)

add_subdirectory(src)

if (USE_OPENSSL)
    find_package(OpenSSL REQUIRED)
    target_compile_definitions(${CMAKE_PROJECT_NAME} PRIVATE USE_OPENSSL)
endif()

if (USE_AERON)
    if (NOT DEFINED AERON_INSTALL_DIR)
        set(AERON_INSTALL_DIR ${CMAKE_INSTALL_PREFIX})
    endif()
    set(AERON_INCLUDE_DIR
        "${AERON_INSTALL_DIR}/include/wrapper"
        "${AERON_INSTALL_DIR}/include/aeron"
        "${AERON_INSTALL_DIR}/include"
    )
    target_link_directories(${CMAKE_PROJECT_NAME} PRIVATE ${AERON_INSTALL_DIR}/lib)
    set(AERON_LIBRARY
        aeron
        aeron_client_shared
        aeron_driver
    )
    target_compile_definitions(${CMAKE_PROJECT_NAME} PRIVATE USE_AERON)
endif()

target_include_directories(${CMAKE_PROJECT_NAME}
    PRIVATE ${OPENSSL_INCLUDE_DIR}
    PRIVATE ${AERON_INCLUDE_DIR}
)
target_link_libraries(${CMAKE_PROJECT_NAME}
    PRIVATE ${OPENSSL_SSL_LIBRARY}
    PRIVATE ${OPENSSL_CRYPTO_LIBRARY}
    PRIVATE ${AERON_LIBRARY}
)

if (WIN32)
    target_compile_definitions(${CMAKE_PROJECT_NAME} PRIVATE NOMINMAX _DDBAPIDLL)
    target_link_libraries(${CMAKE_PROJECT_NAME} PRIVATE ws2_32)
    if (MINGW)
        target_compile_definitions(${CMAKE_PROJECT_NAME} PRIVATE _WIN32_WINNT=0x0600)
    endif()
else()
    target_link_libraries(${CMAKE_PROJECT_NAME} PRIVATE pthread rt uuid)
endif()

if (CMAKE_CXX_COMPILER_ID MATCHES "MSVC")
    # C4251: Class member of an exported class is not marked dllexport.
    # This is triggered by std::string but we cannot modify STL.
    # C4100: Parameter is unused. We should fix this later.
    # C4996: The code uses a deprecated API. We should fix this later.
    target_compile_options(${CMAKE_PROJECT_NAME} PRIVATE /W3 /MP /wd4251 /wd4100 /wd4996)
else()
    # overloaded-virtual: Derived class hides base class virtual functions
    # This is triggered by derived classes of Constant and we will fix this later
    target_compile_options(${CMAKE_PROJECT_NAME} PRIVATE -Wall -Wno-overloaded-virtual)
endif()

if (WITH_GTEST)
    if (NOT USE_CXX11_ABI)
        message(FATAL_ERROR "cannot compile testcases without C++11 ABI")
    endif()
    add_executable(${CMAKE_PROJECT_NAME}_test)
    add_subdirectory(test)

    if (CMAKE_CXX_COMPILER_ID MATCHES "MSVC")
        target_compile_options(${CMAKE_PROJECT_NAME}_test PRIVATE /W0 /utf-8 /MP)
        target_compile_definitions(${CMAKE_PROJECT_NAME}_test PRIVATE WINDOWS NOMINMAX)
    else()
        target_compile_options(${CMAKE_PROJECT_NAME}_test PRIVATE -w)
    endif()

    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    add_subdirectory("${PROJECT_SOURCE_DIR}/thirdparty/googletest")

    enable_testing()

    target_link_libraries(${CMAKE_PROJECT_NAME}_test
        ${CMAKE_PROJECT_NAME}
        gtest_main
        gmock_main
    )
endif()
