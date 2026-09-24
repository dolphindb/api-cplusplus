aeron_install_dir="$(pwd)/thirdparty/aeron/out"

function mkdir_build()
{
	rm -rf build && mkdir build
}

function cmake_build()
{
	cmake --build . -j $(grep -c "processor" /proc/cpuinfo)
}

function build_aeron()
{
	mkdir_build
	mkdir $aeron_install_dir
	cd build
	cmake .. \
		-DCMAKE_C_FLAGS="-fPIC" \
		-DCMAKE_CXX_FLAGS="-fPIC" \
		-DBUILD_AERON_ARCHIVE_API=OFF \
		-DAERON_BUILD_DOCUMENTATION=OFF \
		-DAERON_TESTS=OFF \
		-DAERON_BUILD_SAMPLES=OFF
	cmake_build
	cmake --install . --prefix $aeron_install_dir
	cd ..
}

function build_thirdparty()
{
	cd thirdparty
	for dir in $(ls) ;do
		cd $dir
		build_$dir
		cd ..
	done
	cd ..
}

function build_DolphinDBAPI()
{
	mkdir_build
	cd build
	cmake .. \
		-DCMAKE_BUILD_TYPE=Debug \
		-DAERON_INSTALL_DIR=$aeron_install_dir
	cmake_build
	cd ..
}

build_thirdparty
build_DolphinDBAPI
