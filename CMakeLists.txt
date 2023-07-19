cmake_minimum_required(VERSION 3.0)

set(CMAKE_CXX_STANDARD 11)

project(DolphinDBAPI)

if(UNIX)
	add_definitions(-fPIC -DNDEBUG -O3 -Wall -fmessage-length=0 -std=gnu++11 -msse -msse2 -msse3 -funroll-loops)
    add_definitions("-DLINUX")
    if(ABI EQUAL 0)
        message("set _GLIBCXX_USE_CXX11_ABI to 0")
        add_definitions("-D_GLIBCXX_USE_CXX11_ABI=0")
    elseif(ABI EQUAL 1)
        message("set _GLIBCXX_USE_CXX11_ABI to 1")
        add_definitions("-D_GLIBCXX_USE_CXX11_ABI=1")
    endif()
    if(SSL_LIBS)
        include_directories(${SSL_LIBS}/include)
        set(LIBSSL "-Wl,--whole-archive ${SSL_LIBS}/lib/libssl.a -Wl,--no-whole-archive")
        set(LIBCRYPTO "-Wl,--whole-archive ${SSL_LIBS}/lib/libcrypto.a -Wl,--no-whole-archive")
    else()
        set(LIBSSL ssl)
        set(LIBCRYPTO crypto)
    endif()

    if(UUID_LIBS)
        include_directories(${UUID_LIBS}/include)
        set(LIBUUID "-Wl,--whole-archive ${UUID_LIBS}/lib/libuuid.a -Wl,--no-whole-archive")
    else()
        set(LIBUUID uuid)
    endif()

elseif(WIN32)
	if(MSVC)
		message("MSVC Compiling")
	    add_definitions("-DWINDOWS -DWIN32_LEAN_AND_MEAN -D_DDBAPIDLL -DNOMINMAX")
		if(SSL_LIBS)
			include_directories(${SSL_LIBS}/include64)
			set(LIBSSL ${SSL_LIBS}/lib64/ssleay32MD.lib)
			set(LIBCRYPTO ${SSL_LIBS}/lib64/libeay32MD.lib)
		else()
			set(LIBSSL ssleay32MD.lib)
			set(LIBCRYPTO libeay32MD.lib)
		endif()
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
	    if(SSL_LIBS)
			include_directories(${SSL_LIBS}/include)
			set(LIBSSL ${SSL_LIBS}/lib/libssl.a)
			set(LIBCRYPTO ${SSL_LIBS}/lib/libcrypto.a)
		else()
			set(LIBSSL ssl)
			set(LIBCRYPTO crypto)
		endif()
	endif()

     
endif()

AUX_SOURCE_DIRECTORY(src DIR_SRCS)
include_directories(include)
ADD_LIBRARY(DolphinDBAPI SHARED ${DIR_SRCS})

if(UNIX)
    target_link_libraries(
        DolphinDBAPI
        pthread 
        ${LIBSSL}
        ${LIBCRYPTO}
        ${LIBUUID}
        dl
    )
elseif(WIN32)
    target_link_libraries(
        DolphinDBAPI
        ${LIBSSL}
        ${LIBCRYPTO}
        ws2_32
    )
endif()