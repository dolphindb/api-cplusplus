FROM ubuntu:latest
WORKDIR /root
RUN echo "deb http://mirrors.aliyun.com/ubuntu/ noble main restricted universe multiverse\ndeb-src http://mirrors.aliyun.com/ubuntu/ noble main restricted universe multiverse\ndeb http://mirrors.aliyun.com/ubuntu/ noble-security main restricted universe multiverse\ndeb-src http://mirrors.aliyun.com/ubuntu/ noble-security main restricted universe multiverse\ndeb http://mirrors.aliyun.com/ubuntu/ noble-updates main restricted universe multiverse\ndeb-src http://mirrors.aliyun.com/ubuntu/ noble-updates main restricted universe multiverse\ndeb http://mirrors.aliyun.com/ubuntu/ noble-backports main restricted universe multiverse\ndeb-src http://mirrors.aliyun.com/ubuntu/ noble-backports main restricted universe multiverse" > /etc/apt/sources.list
RUN apt update -y
RUN apt-get install -y wget build-essential cmake uuid-dev gdb unzip python3.12
RUN mkdir -p /root/gcc /root/openssl /root/aeron /root/aeron_ABI /root/gtest /root/gtest_ABI
# gcc 8.4.0
RUN wget http://mirrors.concertpass.com/gcc/releases/gcc-8.4.0/gcc-8.4.0.tar.xz && tar -Jxvf gcc-8.4.0.tar.xz
RUN cd /root/gcc-8.4.0 && ./contrib/download_prerequisites && ./configure --prefix=/root/gcc --disable-multilib && make -j$(nproc) && make install
# openssl 3.5.0
RUN wget https://github.com/openssl/openssl/releases/download/openssl-3.5.0/openssl-3.5.0.tar.gz && tar -zxvf /root/openssl-3.5.0.tar.gz
RUN cd /root/openssl-3.5.0 && ./config CC=/root/gcc/bin/gcc CXX=/root/gcc/bin/g++ --prefix=/root/openssl/ shared && make -j$(nproc) && make install
# aeron 1.43.0
RUN wget https://github.com/aeron-io/aeron/archive/refs/tags/1.43.0.tar.gz && tar -zxvf /root/1.43.0.tar.gz
RUN cd /root/aeron-1.43.0 && mkdir ./build && cd ./build && cmake .. -DCMAKE_C_COMPILER=/root/gcc/bin/gcc -DCMAKE_CXX_COMPILER=/root/gcc/bin/g++ -DCMAKE_C_FLAGS="-fPIC" -DCMAKE_CXX_FLAGS="-fPIC -D_GLIBCXX_USE_CXX11_ABI=0" -DBUILD_AERON_ARCHIVE_API=OFF -DAERON_BUILD_DOCUMENTATION=OFF -DAERON_TESTS=OFF -DAERON_BUILD_SAMPLES=OFF && cmake --build . -j$(nproc) && cmake --install . --prefix=/root/aeron
RUN cd /root/aeron-1.43.0 && mkdir ./build_ABI && cd ./build_ABI && cmake .. -DCMAKE_C_COMPILER=/root/gcc/bin/gcc -DCMAKE_CXX_COMPILER=/root/gcc/bin/g++ -DCMAKE_C_FLAGS="-fPIC" -DCMAKE_CXX_FLAGS="-fPIC -D_GLIBCXX_USE_CXX11_ABI=1" -DBUILD_AERON_ARCHIVE_API=OFF -DAERON_BUILD_DOCUMENTATION=OFF -DAERON_TESTS=OFF -DAERON_BUILD_SAMPLES=OFF && cmake --build . -j$(nproc) && cmake --install . --prefix=/root/aeron_ABI
# gtest 1.12.1
RUN wget https://github.com/google/googletest/archive/refs/tags/release-1.12.1.tar.gz && tar -zxvf /root/release-1.12.1.tar.gz
RUN cd /root/googletest-release-1.12.1 && mkdir ./build && cd ./build && cmake .. -DCMAKE_C_COMPILER=/root/gcc/bin/gcc -DCMAKE_CXX_COMPILER=/root/gcc/bin/g++ -DBUILD_GMOCK=OFF -DBUILD_SHARED_LIBS=ON -DCMAKE_CXX_FLAGS="-D_GLIBCXX_USE_CXX11_ABI=0" && cmake --build . -j$(nproc) && cmake --install . --prefix=/root/gtest
RUN cd /root/googletest-release-1.12.1 && mkdir ./build_ABI && cd ./build_ABI && cmake .. -DCMAKE_C_COMPILER=/root/gcc/bin/gcc -DCMAKE_CXX_COMPILER=/root/gcc/bin/g++ -DBUILD_GMOCK=OFF -DBUILD_SHARED_LIBS=ON -DCMAKE_CXX_FLAGS="-D_GLIBCXX_USE_CXX11_ABI=1" && cmake --build . -j$(nproc) && cmake --install . --prefix=/root/gtest_ABI
# gtest-parallel
RUN wget https://github.com/google/gtest-parallel/archive/refs/heads/master.zip && unzip /root/master.zip
RUN echo '#!/bin/sh\n\
mount -o remount,rw /proc/sys 2>/dev/null || true\n\
echo "./core.%e.%p.%t" > /proc/sys/kernel/core_pattern 2>/dev/null || true\n\
exec "$@"' > /entrypoint.sh && \
    chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
CMD ["bash"]