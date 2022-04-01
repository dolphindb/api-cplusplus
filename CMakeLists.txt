cmake_minimum_required(VERSION 2.8)

set(CMAKE_CXX_STANDARD 11)

project(DolphinDBAPI)

add_definitions(-fPIC -DABI -DNDEBUG -DOPENBLAS -DLOGGING_LEVEL_2 -O3 -Wall -fmessage-length=0 -std=gnu++11 -D_GLIBCXX_USE_CXX11_ABI=0 -msse -msse2 -msse3 -funroll-loops)

if(UNIX)
    add_definitions("-DLINUX")

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

    if(OPENBLAS_LIBS)
        include_directories(${OPENBLAS_LIBS}\include)
        set(LIBOPENBLAS ${OPENBLAS_LIBS}/lib/libopenblas.a)
    endif()

    if(QUADMATH_LIBS)
        include_directories(${QUADMATH_LIBS}/include)
        set(LIBQUADMATH ${QUADMATH_LIBS}/lib/libquadmath.a)
    else()
        set(LIBQUADMATH, lquadmath)
    endif()

    if(TCMALLOC_LIBS)
        include_directories(${TCMALLOC_LIBS}/include)
        set(LIBTCMALLOC "-Wl,-Bstatic ${TCMALLOC_LIBS}/lib/libtcmalloc_minimal.a -Wl,-Bdynamic")
        add_definitions("-DTCMALLOC -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free")
    endif()

elseif(WIN32)
    add_definitions("-DWINDOWS -D_WIN32_LEAN_AND_MEAN -D_WIN32_WINNT=0x0600")
    if(SSL_LIBS)
        include_directories(${SSL_LIBS}/include)
        set(LIBSSL ${SSL_LIBS}/lib/libssl.a)
        set(LIBCRYPTO ${SSL_LIBS}/lib/libcrypto.a)
    else()
        set(LIBSSL ssl)
        set(LIBCRYPTO crypto)
    endif()

    if(OPENBLAS_LIBS)
        include_directories(${OPENBLAS_LIBS}/include)
        link_directories(${OPENBLAS_LIBS}/lib)
    else()
        set(LIBOPENBLAS, "")
    endif()

    if(TCMALLOC_LIBS)
        include_directories(${TCMALLOC_LIBS}/include)
        set(LIBTCMALLOC "-Wl,-Bstatic ${TCMALLOC_LIBS}/lib/libtcmalloc_minimal.a -Wl,-Bdynamic")
        add_definitions("-DTCMALLOC -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free")
    else()
        set(LIBTCMALLOC "")       
    endif()   
endif()

AUX_SOURCE_DIRECTORY(src DIR_SRCS)

ADD_LIBRARY(DolphinDBAPI SHARED ${DIR_SRCS})

if(UNIX)
    target_link_libraries(
        DolphinDBAPI
        pthread 
        ${LIBSSL}
        ${LIBCRYPTO}
        ${LIBUUID}
        ${LIBOPENBLAS}
        ${LIBQUADMATH}
        ${LIBTCMALLOC}
        dl
    )
elseif(WIN32)
    target_link_libraries(
        DolphinDBAPI
        ${LIBTCMALLOC}
        gcc_s 
        psapi
        ${LIBSSL}
        ${LIBCRYPTO}
        ole32
        ws2_32
        gdi32
        iphlpapi
        ${LIBOPENBLAS}
        libgfortran.a
        libquadmath.a
    )
endif()
