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
add_subdirectory(src)

if (USE_OPENSSL)
    if (NOT LINK_SHARED_LIBS)
        set(OPENSSL_USE_STATIC_LIBS TRUE)
    endif()
    # FindOpenSSL from cmake
    find_package(OpenSSL)
    if (OPENSSL_FOUND)
        target_compile_definitions(${CMAKE_PROJECT_NAME} PRIVATE USE_OPENSSL)
        target_include_directories(${CMAKE_PROJECT_NAME} PRIVATE ${OPENSSL_INCLUDE_DIR})
        target_link_libraries(${CMAKE_PROJECT_NAME} PRIVATE ${OPENSSL_SSL_LIBRARY} ${OPENSSL_CRYPTO_LIBRARY})
    else()
        message(WARNING
        "Failed to compile ${CMAKE_PROJECT_NAME} with OpenSSL. Encrypted communication features are unavailable.\n"
        "To suppress this warning, set -DUSE_OPENSSL=OFF in cmake command.")
    endif()
endif()

include(CheckIncludeFileCXX)
include(CheckCXXSourceCompiles)

if (USE_AERON)
    if (NOT DEFINED AERON_INSTALL_DIR)
        set(AERON_INSTALL_DIR ${CMAKE_INSTALL_PREFIX})
    endif()
    set(AERON_INCLUDE_DIR
        "${AERON_INSTALL_DIR}/include/wrapper"
        "${AERON_INSTALL_DIR}/include/aeron"
        "${AERON_INSTALL_DIR}/include"
    )
    set(AERON_LIBRARY_DIR ${AERON_INSTALL_DIR}/lib)
    set(AERON_LIBRARY_STATIC
        aeron_static
        aeron_driver_static
        aeron_client
    )
    set(AERON_LIBRARY_SHARED
        aeron
        aeron_driver
        aeron_client_shared
    )
    if (LINK_SHARED_LIBS)
        set(AERON_LIBRARY ${AERON_LIBRARY_SHARED})
    else()
        set(AERON_LIBRARY ${AERON_LIBRARY_STATIC})
    endif()
    set(CMAKE_REQUIRED_INCLUDES ${AERON_INCLUDE_DIR})
    check_include_file_cxx("FragmentAssembler.h" HAS_AERON_HEADER)
    if(CMAKE_VERSION VERSION_GREATER "3.31.0")
        if (HAS_AERON_HEADER)
            set(CMAKE_REQUIRED_LINK_DIRECTORIES ${AERON_LIBRARY_DIR})
            set(CMAKE_REQUIRED_LIBRARIES ${AERON_LIBRARY})
            check_cxx_source_compiles("int main() {return 0;}" HAS_AERON)
        endif()
    else()
        set(HAS_AERON ${HAS_AERON_HEADER})
    endif()
    unset(CMAKE_REQUIRED_INCLUDES)
    unset(CMAKE_REQUIRED_LIBRARIES)
    unset(CMAKE_REQUIRED_LINK_DIRECTORIES)
    if (HAS_AERON)
        target_compile_definitions(${CMAKE_PROJECT_NAME} PRIVATE USE_AERON)
        target_include_directories(${CMAKE_PROJECT_NAME} PRIVATE ${AERON_INCLUDE_DIR})
        target_link_directories(${CMAKE_PROJECT_NAME} PRIVATE ${AERON_LIBRARY_DIR})
        target_link_libraries(${CMAKE_PROJECT_NAME} PRIVATE ${AERON_LIBRARY})
    else()
        message(WARNING
        "Failed to compile ${CMAKE_PROJECT_NAME} with Aeron. UDP features are unavailable.\n"
        "To suppress this warning, set -DUSE_AERON=OFF in cmake command.")
    endif()
endif()

if (WIN32)
    target_compile_definitions(${CMAKE_PROJECT_NAME} PRIVATE NOMINMAX _DDBAPIDLL)
    target_link_libraries(${CMAKE_PROJECT_NAME} PRIVATE ws2_32 crypt32)
    if (MINGW)
        target_compile_definitions(${CMAKE_PROJECT_NAME} PRIVATE WINDOWS _WIN32_WINNT=0x0600)
    endif()
else()
    check_include_file_cxx("uuid/uuid.h" HAS_UUID)
    if (NOT HAS_UUID)
        message(FATAL_ERROR "Failed to find uuid.")
    endif()
    target_link_libraries(${CMAKE_PROJECT_NAME} PRIVATE pthread rt uuid)
endif()

if (MSVC)
    # C4251: Class member of an exported class is not marked dllexport. This is triggered by std::string but we cannot modify STL.
    # C4100: Parameter is unused. We should fix this later.
    target_compile_options(${CMAKE_PROJECT_NAME} PRIVATE /W4 /MP /wd4251 /wd4100)
    # Unicode support of fmt (used in spdlog) requires /utf-8 of MSVC.
    target_compile_definitions(${CMAKE_PROJECT_NAME} PRIVATE FMT_UNICODE=0)
    if (${CMAKE_BUILD_TYPE} MATCHES "Dev")
        target_compile_options(${CMAKE_PROJECT_NAME} PRIVATE /WX)
    endif()
else()
    target_compile_options(${CMAKE_PROJECT_NAME} PRIVATE -Wall)
    target_compile_options(${CMAKE_PROJECT_NAME} PRIVATE $<$<COMPILE_LANGUAGE:CXX>:
        -Wno-overloaded-virtual # ConstantImp.h
        -Wno-inconsistent-missing-override # ConstantImp.h
    >)
    # Enable Wextra and Wconversion when possible
    set(CMAKE_CXX_FLAGS_DEV "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -Werror -Wpedantic")
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

if (API_TEST)
    if (NOT DEFINED TEST_BINARY_NAME)
        set(TEST_BINARY_NAME ${CMAKE_PROJECT_NAME}_test) # default test binary name is DolphinDBAPI_test
    endif()
    option(COMPATI "this is a compatibility testing parameter" OFF)
    if (COMPATI AND DEFINED ENV{COMPATI_PATH})
        set(COMPATI_PATH $ENV{COMPATI_PATH})
        file(GLOB COMPATI_SRC "${COMPATI_PATH}/*_gtest.cpp" "${COMPATI_PATH}/gt_main.cpp")
        add_executable(${TEST_BINARY_NAME}
            ${COMPATI_SRC}
            "test/config.cpp"
        )
    else()
        file(GLOB TEST_SRC "test/*_gtest.cpp" "test/gt_main.cpp")
        add_executable(${TEST_BINARY_NAME}
            ${TEST_SRC}
            "test/config.cpp"
            "test/concurrent_testing/ConcurrentTest_gtest.cpp"
            "test/HA_testing/HighAvailableTest_gtest.cpp"
        )
    endif()


    if(USE_OPENSSL AND OPENSSL_FOUND)
        target_compile_definitions(${TEST_BINARY_NAME} PRIVATE TEST_OPENSSL)
        target_include_directories(${TEST_BINARY_NAME} PRIVATE ${OPENSSL_INCLUDE_DIR})
        target_link_libraries(${TEST_BINARY_NAME} PRIVATE ${OPENSSL_SSL_LIBRARY} ${OPENSSL_CRYPTO_LIBRARY})
    endif()
    if(USE_AERON AND HAS_AERON)
        target_compile_definitions(${TEST_BINARY_NAME} PRIVATE TEST_AERON)
        target_include_directories(${TEST_BINARY_NAME} PRIVATE ${AERON_INCLUDE_DIR})
        target_link_directories(${TEST_BINARY_NAME} PRIVATE ${AERON_LIBRARY_DIR})
        target_link_libraries(${TEST_BINARY_NAME} PRIVATE ${AERON_LIBRARY})
    endif()

    if (MSVC)
        target_compile_options(${TEST_BINARY_NAME} PRIVATE /W0 /utf-8 /MP /bigobj)
        target_compile_definitions(${TEST_BINARY_NAME} PRIVATE WINDOWS NOMINMAX)
    elseif (MINGW)
        target_compile_options(${TEST_BINARY_NAME} PRIVATE -w -Wa,-mbig-obj -pthread -std=c++11 -g)
        target_compile_definitions(${TEST_BINARY_NAME} PRIVATE WINDOWS _WIN32_WINNT=0x0600)
    else()
        target_compile_options(${TEST_BINARY_NAME} PRIVATE -w -pthread -std=c++11 -g)
    endif()

    if(CMAKE_VERSION VERSION_GREATER "3.27.0")
        cmake_policy(SET CMP0144 NEW)
    endif()

    find_package(GTest 1.12.0 REQUIRED)
    if (NOT DEFINED API_INSTALL_DIR)
        set(API_INSTALL_LIB ${CMAKE_PROJECT_NAME})
    else()
        if (MSVC)
            set(LIB_NAME "${CMAKE_SHARED_LIBRARY_PREFIX}${CMAKE_PROJECT_NAME}.lib")
        else()
            set(LIB_NAME "${CMAKE_SHARED_LIBRARY_PREFIX}${CMAKE_PROJECT_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX}")
        endif()
        set(API_INSTALL_LIB ${API_INSTALL_DIR}/${LIB_NAME})
    endif()
    target_include_directories(${TEST_BINARY_NAME} PRIVATE ${PROJECT_SOURCE_DIR}/include)

    target_link_libraries(${TEST_BINARY_NAME} PRIVATE
        ${API_INSTALL_LIB}
        GTest::gtest
        GTest::gmock
        GTest::gtest_main
    )
endif()
