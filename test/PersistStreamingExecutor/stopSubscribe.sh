#! /bin/bash
if [ $1 == "normal" ];then
    ps aux |grep "demo7$" | grep -v grep | awk '{print $2}' | xargs kill -9
elif [ $1 == "HA" ];then
    ps aux |grep demo7_HA | grep -v grep | awk '{print $2}' | xargs kill -9
else
    ps aux |grep demo7_d | grep -v grep | awk '{print $2}' | xargs kill -9
fi
