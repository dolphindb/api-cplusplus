1. 解压demo.zip到src目录下
2. 将编译好的C++ API动态库拷贝到demo/lib目录下
3. 执行下面的命令，以Visual Studio 2022为例，其中用SSL_LIBS参数指定openssl库位置
cd api-cplusplus/demo
mkdir build && cd build
cmake .. -G "Visual Studio 17 2022" -A x64 -DSSL_LIBS=D:/openssl-1.0.2l-vs2017
cmake --build .
4. 生成的可执行文件apiDemo.exe在demo\bin\Debug目录下
5. 将所依赖的动态库（DolphinDBAPI.dll libeay32MD.dll ssleay32MD.dll）拷贝到该目录下即可执行
6. 这个demo同样支持mingw以及Linux，只是cmake语句略有不同