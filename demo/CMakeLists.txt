cmake_minimum_required(VERSION 3.13)

project(DolphinDBDemo)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(LINK_LIBS)
if(USE_OPENSSL)
    add_definitions("-DUSE_OPENSSL=1")
    find_package(OpenSSL REQUIRED)
    include_directories(${OPENSSL_INCLUDE_DIR})
    message("openssl include dir    : ${OPENSSL_INCLUDE_DIR}")
    message("openssl ssl lib dir    : ${OPENSSL_SSL_LIBRARY}")
    message("openssl crypto lib dir : ${OPENSSL_CRYPTO_LIBRARY}")
endif()
if(UNIX)
    check_include_file("uuid/uuid.h" HAS_UUID)
    if (NOT HAS_UUID)
        message(FATAL_ERROR "Failed to find uuid.")
    endif()
    add_compile_options(-std=c++11 -DLINUX -Wall -fPIC)
    list(APPEND LINK_LIBS DolphinDBAPI rt pthread)
elseif(WIN32)
    if(MSVC)
        add_compile_options(-DWINDOWS -DNOMINMAX)
        list(APPEND LINK_LIBS DolphinDBAPI)
    elseif(MINGW)
        add_compile_options(-std=c++11 -DWINDOWS -Wall -fPIC)
        list(APPEND LINK_LIBS DolphinDBAPI pthread)
    endif()
endif()
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../include)
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/lib/)
add_executable(${CMAKE_PROJECT_NAME} ${PROJECT_SOURCE_DIR}/src/main.cpp)
target_compile_features(${CMAKE_PROJECT_NAME} PRIVATE cxx_std_11)
target_link_libraries(${CMAKE_PROJECT_NAME} ${LINK_LIBS})
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_CURRENT_SOURCE_DIR}/bin/)

if (CMAKE_CXX_COMPILER_ID MATCHES "MSVC")
    # C4251: Class member of an exported class is not marked dllexport.
    # This is triggered by std::string but we cannot modify STL.
    # C4100: Parameter is unused. We should fix this later.
    # C4996: The code uses a deprecated API. We should fix this later.
    target_compile_options(${CMAKE_PROJECT_NAME} PRIVATE /W3 /MP /wd4251 /wd4100 /wd4996)
    # Unicode support of fmt (used in spdlog) requires /utf-8 of MSVC.
    target_compile_definitions(${CMAKE_PROJECT_NAME} PRIVATE FMT_UNICODE=0)
else()
    # overloaded-virtual: Derived class hides base class virtual functions
    # This is triggered by derived classes of Constant and we will fix this later
    target_compile_options(${CMAKE_PROJECT_NAME} PRIVATE -Wall -Wno-overloaded-virtual)
    # Enable Wextra and Wconversion when possible
    # Try clang hwasan when possible: -fsanitize=hwaddress -Wl,--no-relax
    set(CMAKE_CXX_FLAGS_DEV "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -Werror -Wpedantic")
endif()