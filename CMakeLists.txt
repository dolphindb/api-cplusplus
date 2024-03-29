cmake_minimum_required(VERSION 3.6)
set(CMAKE_CXX_STANDARD 11)
project(DolphinDBAPI)

set(LINK_LIBS)
if(USE_OPENSSL)
    add_definitions("-DUSE_OPENSSL=1")
    if(OPENSSL_PATH)
        set(ENV{OPENSSL_ROOT_DIR} ${OPENSSL_PATH})
    endif()
    find_package(OpenSSL REQUIRED)
    list(APPEND LINK_LIBS ${OPENSSL_SSL_LIBRARY} ${OPENSSL_CRYPTO_LIBRARY})
    include_directories(${OPENSSL_INCLUDE_DIR})
    message("openssl include dir    : ${OPENSSL_INCLUDE_DIR}")
    message("openssl ssl lib dir    : ${OPENSSL_SSL_LIBRARY}")
    message("openssl crypto lib dir : ${OPENSSL_CRYPTO_LIBRARY}")
endif()

if(UNIX)
    add_definitions(-fPIC -DNDEBUG -DLINUX -O3 -Wall -fmessage-length=0 -std=gnu++11 -msse -msse2 -msse3 -funroll-loops)
    if(ABI EQUAL 0)
        message("set _GLIBCXX_USE_CXX11_ABI to 0")
        add_definitions("-D_GLIBCXX_USE_CXX11_ABI=0")
    elseif(ABI EQUAL 1)
        message("set _GLIBCXX_USE_CXX11_ABI to 1")
        add_definitions("-D_GLIBCXX_USE_CXX11_ABI=1")
    endif()
    if(UUID_PATH)
        include_directories(${UUID_PATH}/include)
        link_directories(${UUID_PATH}/lib)
    endif()
    list(APPEND LINK_LIBS pthread uuid dl)
elseif(WIN32)
    if(MSVC)
        message("MSVC Compiling")
        add_definitions("-DWINDOWS -DWIN32_LEAN_AND_MEAN -D_DDBAPIDLL -DNOMINMAX")
        set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /MDd")
        set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /MDd")
    elseif(MINGW)
        message("MINGW Compiling")
        add_definitions(-fPIC -DNDEBUG -O3 -Wall -fmessage-length=0 -std=gnu++11 -msse -msse2 -msse3 -funroll-loops)
        add_definitions("-DWINDOWS -D_WIN32_WINNT=0x0600")
        if(ABI EQUAL 0)
            message("set _GLIBCXX_USE_CXX11_ABI to 0")
            add_definitions("-D_GLIBCXX_USE_CXX11_ABI=0")
        elseif(ABI EQUAL 1)
            message("set _GLIBCXX_USE_CXX11_ABI to 1")
            add_definitions("-D_GLIBCXX_USE_CXX11_ABI=1")
        endif()
    endif()
    list(APPEND LINK_LIBS ws2_32)
endif()

AUX_SOURCE_DIRECTORY(src DIR_SRCS)
include_directories(include)
ADD_LIBRARY(DolphinDBAPI SHARED ${DIR_SRCS})
target_link_libraries(
    DolphinDBAPI
    ${LINK_LIBS}
)