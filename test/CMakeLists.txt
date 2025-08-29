if(POLICY CMP0144)
    cmake_policy(SET CMP0144 NEW)
endif()

set(TEST_BINARY_NAME ${CMAKE_PROJECT_NAME}_test)
file(GLOB TEST_SRC "*_gtest.cpp")
add_executable(
    ${TEST_BINARY_NAME}
    ${TEST_SRC}
    config.cpp
    gt_main.cpp
)

if (MSVC)
    target_compile_options(${TEST_BINARY_NAME} PRIVATE /W0 /utf-8 /MP /bigobj /EHa)
    target_compile_definitions(${TEST_BINARY_NAME} PRIVATE WINDOWS NOMINMAX WIN32_LEAN_AND_MEAN)
elseif (MINGW)
    target_compile_options(${TEST_BINARY_NAME} PRIVATE -w -Wa,-mbig-obj -pthread -g -O0)
    target_compile_definitions(${TEST_BINARY_NAME} PRIVATE WINDOWS _WIN32_WINNT=0x0600)
else()
    target_compile_options(${TEST_BINARY_NAME} PRIVATE -w -pthread -g -O0)
endif()

find_package(GTest 1.10.0 REQUIRED)
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
)