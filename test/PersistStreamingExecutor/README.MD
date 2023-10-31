
# 长期性流订阅自动化脚本

## 简介

1. [src](./src)目录下为具体的C++ API流订阅客户端源码，其中`_d`结尾的文件测试异构流表的订阅，`_HA`结尾的文件测试高可用流表订阅，无特殊结尾文件测试普通流表的订阅；
2. 数字代表**订阅客户端所在服务器IP**，7、8、9分别代表**192.168.100.7**、**192.168.100.8**、**192.168.100.9**，同时客户端会把订阅到的数据保存在本地dfs表中，命名方式规则例如：

- dfs://test_`7`_`PollingClient`\_sub\_`HA`, 数字7代表被订阅流表所在服务器为100.37，PollingClient代表使用PollingClient接口订阅，尾缀HA代表是高可用订阅
- dfs://test_`9`_`threadedClient`\_sub\_`batch`\_`d`\_`1`, 数字9代表被订阅流表所在服务器为100.39，threadedClient和batch代表使用threadedClient接口的batch handler形式订阅，d代表是异构流表订阅，尾缀1代表两个异构流表中的t1表，即主表

3. 当前目录下3个Python文件分别是对应的流订阅操作脚本，里面包含`建库建表，创建被订阅流表，向流表写入数据，数据正确性断言`等操作，可在任意服务器上运行py文件开启测试。目前python脚本中指定在`100.7上运行普通订阅客户端`，在`100.8上运行高可用订阅客户端`，`100.9上运行异构流表订阅客户端`


## 服务器文件信息

已编译好的C++ API流订阅客户端所在服务器上的路径：

```shell
/hdd/hdd0/yzou/persistStreaming_server/demo  # 100.7
/hdd/hdd0/yzou/persistStreaming_server/demo_HA # 100.8
/hdd/hdd0/yzou/persistStreaming_server/demo_d # 100.9
```

dolphindb数据节点服务器上的路径：

```shell
/hdd/hdd0/yzou/persistStreaming_server/server_p1 # 100.7
/hdd/hdd0/yzou/persistStreaming_server/server_p2 # 100.8
/hdd/hdd0/yzou/persistStreaming_server/server_p3 # 100.9
```

具体测试流程详见confluence链接[C++ API持续性流订阅测试环境](https://dolphindb1.atlassian.net/wiki/spaces/test/pages/647141563/C+API)