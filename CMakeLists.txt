cmake_minimum_required(VERSION 3.15)

project(DolphinDBAPI)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(USE_OPENSSL "Use OpenSSL for encrypted communication." ON)
option(USE_AERON "Use Aeron for UDP connections. ${CMAKE_PROJECT_NAME} will search for Aeron in $\{CMAKE_INSTALL_PREFIX\} by default, you can specify a different location with AERON_INSTALL_DIR" ON)
option(BUILD_SHARED_LIBS "Build shared library" ON)
option(LINK_SHARED_LIBS "Link shared libraries" ON)
option(WITH_UNITTEST "Compile testcases." OFF)

add_library(${CMAKE_PROJECT_NAME})
target_compile_features(${CMAKE_PROJECT_NAME} PRIVATE cxx_std_11)
target_compile_features(${CMAKE_PROJECT_NAME} PRIVATE c_std_99)

if (API_TEST)
    add_subdirectory(test)
endif()

if (USE_OPENSSL)
    if (NOT LINK_SHARED_LIBS)
        set(OPENSSL_USE_STATIC_LIBS TRUE)
    endif()
    # FindOpenSSL from cmake
    find_package(OpenSSL)
    if (OPENSSL_FOUND)
        target_link_libraries(${CMAKE_PROJECT_NAME} PRIVATE OpenSSL::SSL OpenSSL::Crypto)
        target_compile_definitions(${CMAKE_PROJECT_NAME} PRIVATE USE_OPENSSL)
    else()
        message(WARNING
        "Failed to compile ${CMAKE_PROJECT_NAME} with OpenSSL. Encrypted communication features are unavailable.\n"
        "To suppress this warning, set -DUSE_OPENSSL=OFF in cmake command.")
    endif()
endif()

include(CheckIncludeFileCXX)
include(CheckCXXSourceCompiles)

if (USE_AERON)
    find_package(aeron QUIET)
    if (aeron_FOUND)
        message(STATUS "Found aeron: ${aeron_VERSION}")
        target_compile_definitions(${CMAKE_PROJECT_NAME} PRIVATE USE_AERON)
        if (LINK_SHARED_LIBS)
            target_link_libraries(${CMAKE_PROJECT_NAME} PRIVATE aeron::aeron_client_wrapper aeron::aeron aeron::aeron_driver)
        else()
            target_link_libraries(${CMAKE_PROJECT_NAME} PRIVATE aeron::aeron_client_wrapper aeron::aeron_static aeron::aeron_driver_static)
        endif()
    else()
        message(WARNING
        "Failed to compile ${CMAKE_PROJECT_NAME} with Aeron. UDP features are unavailable.\n"
        "To suppress this warning, set -DUSE_AERON=OFF in cmake command.")
    endif()
endif()

find_package(Threads REQUIRED)
target_link_libraries(${CMAKE_PROJECT_NAME} PRIVATE Threads::Threads)
if (WIN32)
    target_compile_definitions(${CMAKE_PROJECT_NAME} PRIVATE _DDBAPIDLL)
    target_link_libraries(${CMAKE_PROJECT_NAME} PRIVATE ws2_32 crypt32)
    if (MINGW)
        target_compile_definitions(${CMAKE_PROJECT_NAME} PRIVATE _WIN32_WINNT=0x0600)
    endif()
else()
    # libuuid
    list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
    find_package(LibUUID REQUIRED)
    target_link_libraries(${CMAKE_PROJECT_NAME} PRIVATE LibUUID::LibUUID)
    # Linux shared memory API
    include(CheckLibraryExists)
    check_library_exists("" shm_open "" LIBRT_MERGED)
    if (NOT LIBRT_MERGED)
        target_link_libraries(${CMAKE_PROJECT_NAME} PRIVATE rt)
    endif()
endif()

set(CMAKE_CXX_FLAGS_DEV "${CMAKE_CXX_FLAGS_DEBUG}")
set(CMAKE_CXX_FLAGS_COV "${CMAKE_CXX_FLAGS_DEBUG}")
if (MSVC)
    target_compile_options(${CMAKE_PROJECT_NAME} PRIVATE /W4 /MP /Zc:__cplusplus /utf-8 /bigobj)
    target_compile_definitions(${CMAKE_PROJECT_NAME} PRIVATE _CRT_SECURE_NO_WARNINGS)
    if (${CMAKE_BUILD_TYPE} MATCHES "Dev")
        target_compile_options(${CMAKE_PROJECT_NAME} PRIVATE /WX)
    endif()
elseif (MINGW)
    target_compile_options(${CMAKE_PROJECT_NAME} PRIVATE -Wa,-mbig-obj)
else()
    target_compile_options(${CMAKE_PROJECT_NAME} PRIVATE -Wall -Wextra -Wpedantic -fstrict-aliasing)
    target_compile_options(${CMAKE_PROJECT_NAME} PRIVATE -fsigned-char)
    set(CMAKE_CXX_FLAGS_DEV "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -Werror")
    if (${CMAKE_BUILD_TYPE} MATCHES "Cov")
        target_compile_options(${CMAKE_PROJECT_NAME} PRIVATE --coverage)
        target_link_options(${CMAKE_PROJECT_NAME} PRIVATE --coverage)
    endif()
endif()

if (WITH_UNITTEST)
    find_package(GTest REQUIRED)
    set(UNITTEST ${CMAKE_PROJECT_NAME}_unittest)
    add_executable(${UNITTEST})
    add_subdirectory(unittest)

    target_link_libraries(${UNITTEST}
        ${CMAKE_PROJECT_NAME}
        GTest::gtest_main
    )
endif()

if (UNIX)
    include(GNUInstallDirs)
    if(CMAKE_VERSION VERSION_GREATER "3.23.0")
        file(GLOB_RECURSE HEADER_FILES "include/*")
        target_sources(${CMAKE_PROJECT_NAME} PUBLIC FILE_SET headers TYPE HEADERS
            BASE_DIRS "${CMAKE_CURRENT_SOURCE_DIR}/include"
            FILES ${HEADER_FILES}
        )
        install(TARGETS ${CMAKE_PROJECT_NAME}
            FILE_SET headers DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/dolphindb
        )
    else()
        install(DIRECTORY "include/"
            DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/dolphindb
            FILES_MATCHING PATTERN "*"
        )
        install(TARGETS ${CMAKE_PROJECT_NAME})
    endif()
endif()

add_subdirectory(src)
