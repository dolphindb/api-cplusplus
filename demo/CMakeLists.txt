cmake_minimum_required(VERSION 3.15)

project(DolphinDBDemo)

add_executable(${CMAKE_PROJECT_NAME} ${PROJECT_SOURCE_DIR}/src/main.cpp)
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_CURRENT_SOURCE_DIR}/bin/)

target_compile_features(${CMAKE_PROJECT_NAME} PRIVATE cxx_std_11)

target_include_directories(${CMAKE_PROJECT_NAME} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_directories(${CMAKE_PROJECT_NAME} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/lib/)
target_link_libraries(${CMAKE_PROJECT_NAME} DolphinDBAPI)

if(WIN32)
    target_compile_definitions(${CMAKE_PROJECT_NAME} PRIVATE NOMINMAX WIN32_LEAN_AND_MEAN)
endif()

if (MSVC)
    target_compile_options(${CMAKE_PROJECT_NAME} PRIVATE /Zc:__cplusplus /utf-8 /wd4251)
endif()
