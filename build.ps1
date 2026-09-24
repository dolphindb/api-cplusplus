Remove-Item -Recurse -Force .\build\
mkdir build
cd build
cmake .. -DUSE_AERON=OFF -DUSE_OPENSSL=OFF
cmake --build . --config Debug
