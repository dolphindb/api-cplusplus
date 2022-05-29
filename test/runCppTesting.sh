#!/bin/bash
#1.get libDolphindbAPI.so and includ filles
rm /hdd/hdd1/testing/api/workdir/api-cplusplus/bin/linux_x64/*  /hdd/hdd1/testing/api/workdir/api-cplusplus/include -rf 
cp /hdd/hdd1/testing/api/package/api-cplusplus/libDolphinDBAPI.so  /hdd/hdd1/testing/api/workdir/api-cplusplus/bin/linux_x64 -r
cp /var/lib/jenkins/workspace/api-cplusplus-testing/include   /hdd/hdd1/testing/api/workdir/api-cplusplus -r

#2.compile and run the test
cd /hdd/hdd1/testing/api/workdir/api-cplusplus/test && make clean && make -j  && rm /hdd/hdd1/testing/api/result/cplusplus/output.txt -rf && ./DolphinDBTest /hdd/hdd1/testing/api/result/cplusplus/output.txt && ./DolphinDBTestINDEX_MAX /hdd/hdd1/testing/api/result/cplusplus/output.txt
