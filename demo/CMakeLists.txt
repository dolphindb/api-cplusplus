cmake_minimum_required(VERSION 3.4.1)
set(ProjectName apiDemo)

project(${ProjectName})

set(SRC_LIST 
	${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp
)
if(UNIX)
	add_compile_options(-std=c++11 -DLINUX -Wall -O0 -g -fPIC -Wl,-rpath,${CMAKE_CURRENT_SOURCE_DIR}:${CMAKE_CURRENT_SOURCE_DIR}/lib)
elseif(WIN32)
	if(MSVC)
		add_compile_options(-DWINDOWS -DNOMINMAX -D_DEBUG)
		include_directories(
			${SSL_LIBS}/include64
		)
	elseif(MINGW)
		add_compile_options(-std=c++11 -DWINDOWS -Wall -O0 -g -fPIC -Wl,-rpath,${CMAKE_CURRENT_SOURCE_DIR}:${CMAKE_CURRENT_SOURCE_DIR}/lib)
	endif()
endif()

include_directories(
	${CMAKE_CURRENT_SOURCE_DIR}/../include
)
link_directories(
	${CMAKE_CURRENT_SOURCE_DIR}/lib/
)
add_executable(${ProjectName} ${SRC_LIST})

if(UNIX)
	target_link_libraries(${ProjectName}
		DolphinDBAPI
		rt
		pthread
	)
elseif(WIN32)
	if(MSVC)
		target_link_libraries(${ProjectName}
			DolphinDBAPI
		)
	elseif(MINGW)
		target_link_libraries(${ProjectName}
			DolphinDBAPI
			pthread
		)
	endif()
endif()


set(EXECUTABLE_OUTPUT_PATH ${CMAKE_CURRENT_SOURCE_DIR}/bin/)
