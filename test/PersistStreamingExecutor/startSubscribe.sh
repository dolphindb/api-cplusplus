#! /bin/bash
export LD_LIBRARY_PATH=`pwd`:$LD_LIBRARY_PATH
if [ $1 == "normal" ];then
    nohup ./demo7 > output7.log 2>&1 &
elif [ $1 == "HA" ];then
    nohup ./demo7_HA > output7_HA.log 2>&1 &
else
    nohup ./demo7_d > output7_d.log 2>&1 &
fi
