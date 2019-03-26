cmake_minimum_required(VERSION 3.0)
project(API_CXX)

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BUILD_DIR})
set(API_SRC ${CMAKE_CURRENT_SOURCE_DIR})

#===============================================================
if (WIN32)
    set(platform_macro "-DWINDOWS")
elseif (UNIX)
    set(platform_macro "-DLINUX")
endif ()
# -D_GLIBCXX_USE_CXX11_ABI=0
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${platform_macro} -fPIC  -std=c++11")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS} -g -DDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS} -O3")

#===============================================================
if (WIN32)
    link_directories(${CMAKE_BINARY_DIR})
endif ()
link_directories(${API_SRC}/bin)
include_directories(${API_SRC}/include)
include_directories(${API_SRC}/src)

#===============================================================
add_library(StreamingClient SHARED ${API_SRC}/src/Streaming.cpp)

if (EXISTS "/usr/lib/libssl.so.1.0.0") # archlinux
    set(LIBSSL /usr/lib/libssl.so.1.0.0)
else ()
    set(LIBSSL ssl)
endif ()

if (WIN32)
    set(platform_lib "ws2_32")
elseif (UNIX)
    set(platform_lib ${LIBSSL})
endif ()

target_link_libraries(StreamingClient
        DolphinDBAPI
        ${platform_lib}
        pthread
        uuid
        )

#===============================================================
macro(TEST module)
    add_executable(${module} ${API_SRC}/test/Streaming${module}.cpp)
    target_link_libraries(${module} StreamingClient)
endmacro()

TEST(PollingClientTester)
TEST(ThreadedClientTester)
TEST(ThreadPooledClientTester)

