本教程主要介绍以下内容：

[DolphinDB C++ API](https://2xdb.net/dolphindb/api-cplusplus#dolphindb-c-api)
- [1. 项目编译](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#1-%E9%A1%B9%E7%9B%AE%E7%BC%96%E8%AF%91)
  - [1.1 在Linux环境下编译项目](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#11-%E5%9C%A8linux%E7%8E%AF%E5%A2%83%E4%B8%8B%E7%BC%96%E8%AF%91%E9%A1%B9%E7%9B%AE)
    - [1.1.1 环境配置](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#111-%E7%8E%AF%E5%A2%83%E9%85%8D%E7%BD%AE)
    - [1.1.2 下载bin文件和头文件](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#112-%E4%B8%8B%E8%BD%BDbin%E6%96%87%E4%BB%B6%E5%92%8C%E5%A4%B4%E6%96%87%E4%BB%B6)
    - [1.1.3 编译main.cpp](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#113-%E7%BC%96%E8%AF%91maincpp)
    - [1.1.4 编译](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#114-%E7%BC%96%E8%AF%91)
    - [1.1.5 运行](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#115-%E8%BF%90%E8%A1%8C)
  - [1.2 Windows环境下编译](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#12-windows%E7%8E%AF%E5%A2%83%E4%B8%8B%E7%BC%96%E8%AF%91)
    - [1.2.1 环境配置](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#121-%E7%8E%AF%E5%A2%83%E9%85%8D%E7%BD%AE)
    - [1.2.2 下载bin文件和头文件](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#122-%E4%B8%8B%E8%BD%BDbin%E6%96%87%E4%BB%B6%E5%92%8C%E5%A4%B4%E6%96%87%E4%BB%B6)
    - [1.2.3 创建Visual Studio项目](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#123-%E5%88%9B%E5%BB%BAvisual-studio%E9%A1%B9%E7%9B%AE)
    - [1.2.4 编译和运行](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#124-%E7%BC%96%E8%AF%91%E5%92%8C%E8%BF%90%E8%A1%8C)
- [2. 建立DolphinDB连接](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#2-%E5%BB%BA%E7%AB%8Bdolphindb%E8%BF%9E%E6%8E%A5)
- [3. 运行DolphinDB脚本](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#3-%E8%BF%90%E8%A1%8Cdolphindb%E8%84%9A%E6%9C%AC)
- [4. 运行DolphinDB函数](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#4-%E8%BF%90%E8%A1%8Cdolphindb%E5%87%BD%E6%95%B0)
- [5. 上传数据对象](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#5-%E4%B8%8A%E4%BC%A0%E6%95%B0%E6%8D%AE%E5%AF%B9%E8%B1%A1)
- [6. 读取数据示例](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#6-%E8%AF%BB%E5%8F%96%E6%95%B0%E6%8D%AE%E7%A4%BA%E4%BE%8B)
  - [6.1 向量](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#61-%E5%90%91%E9%87%8F)
  - [6.2 集合](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#62-%E9%9B%86%E5%90%88)
  - [6.3 矩阵](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#63-%E7%9F%A9%E9%98%B5)
  - [6.4 字典](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#64-%E5%AD%97%E5%85%B8)
  - [6.5 表](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#65-%E8%A1%A8)
    - [6.5.1 getString()方法获取表的内容](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#651-getstring%E6%96%B9%E6%B3%95%E8%8E%B7%E5%8F%96%E8%A1%A8%E7%9A%84%E5%86%85%E5%AE%B9)
    - [6.5.2 getColumn()方法按列获取表的内容](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#652-getcolumn%E6%96%B9%E6%B3%95%E6%8C%89%E5%88%97%E8%8E%B7%E5%8F%96%E8%A1%A8%E7%9A%84%E5%86%85%E5%AE%B9)
    - [6.5.3 getRow()方法按照行获取表的内容](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#653-getrow%E6%96%B9%E6%B3%95%E6%8C%89%E7%85%A7%E8%A1%8C%E8%8E%B7%E5%8F%96%E8%A1%A8%E7%9A%84%E5%86%85%E5%AE%B9)
  - [6.6 AnyVector](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#66-anyvector)
- [7. 保存数据到DolphinDB数据表](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#7-%E4%BF%9D%E5%AD%98%E6%95%B0%E6%8D%AE%E5%88%B0dolphindb%E6%95%B0%E6%8D%AE%E8%A1%A8)
  - [7.1 保存数据到DolphinDB内存表](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#71-%E4%BF%9D%E5%AD%98%E6%95%B0%E6%8D%AE%E5%88%B0dolphindb%E5%86%85%E5%AD%98%E8%A1%A8)
    - [7.1.1 使用insert into语句保存数据](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#711-%E4%BD%BF%E7%94%A8insert-into%E8%AF%AD%E5%8F%A5%E4%BF%9D%E5%AD%98%E6%95%B0%E6%8D%AE)
    - [7.1.2 使用tableInsert函数批量保存多条数据](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#712-%E4%BD%BF%E7%94%A8tableinsert%E5%87%BD%E6%95%B0%E6%89%B9%E9%87%8F%E4%BF%9D%E5%AD%98%E5%A4%9A%E6%9D%A1%E6%95%B0%E6%8D%AE)
    - [7.1.3 使用tableInsert函数保存TableSP对象](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#713-%E4%BD%BF%E7%94%A8tableinsert%E5%87%BD%E6%95%B0%E4%BF%9D%E5%AD%98tablesp%E5%AF%B9%E8%B1%A1)
  - [7.2 保存数据到本地磁盘表](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#72-%E4%BF%9D%E5%AD%98%E6%95%B0%E6%8D%AE%E5%88%B0%E6%9C%AC%E5%9C%B0%E7%A3%81%E7%9B%98%E8%A1%A8)
  - [7.3 保存数据到分布式表](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#73-%E4%BF%9D%E5%AD%98%E6%95%B0%E6%8D%AE%E5%88%B0%E5%88%86%E5%B8%83%E5%BC%8F%E8%A1%A8)
    - [7.3.1 使用tableInsert函数保存TableSP对象](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#731-%E4%BD%BF%E7%94%A8tableinsert%E5%87%BD%E6%95%B0%E4%BF%9D%E5%AD%98tablesp%E5%AF%B9%E8%B1%A1)
    - [7.3.2 分布式表的并发写入](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#732-%E5%88%86%E5%B8%83%E5%BC%8F%E8%A1%A8%E7%9A%84%E5%B9%B6%E5%8F%91%E5%86%99%E5%85%A5)
- [8. 注意事项](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#8-%E6%B3%A8%E6%84%8F%E4%BA%8B%E9%A1%B9)    
[C++ Streaming API](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#c-streaming-api)  
- [9. 编译](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#9-%E7%BC%96%E8%AF%91)
  - [9.1 Linux 64位](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#91-linux-64%E4%BD%8D)
    - [9.1.1 通过cmake](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#911-%E9%80%9A%E8%BF%87cmake)
- [10. 在Windows中使用MinGW编译](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#10-%E5%9C%A8windows%E4%B8%AD%E4%BD%BF%E7%94%A8mingw%E7%BC%96%E8%AF%91)
- [11. Streaming](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#11-api)
  - [11.1 ThreadedClient](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#111-threadedclient)
    - [11.1.1 定义线程客户端](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#1111-%E5%AE%9A%E4%B9%89%E7%BA%BF%E7%A8%8B%E5%AE%A2%E6%88%B7%E7%AB%AF)
    - [11.1.2 调用订阅函数](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#1112-%E8%B0%83%E7%94%A8%E8%AE%A2%E9%98%85%E5%87%BD%E6%95%B0)
    - [11.1.3 取消订阅](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#1123-%E5%8F%96%E6%B6%88%E8%AE%A2%E9%98%85)
  - [11.2 ThreadPooledClient](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#112-threadpooledclient)
    - [11.2.1 定义多线程客户端](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#1121-%E5%AE%9A%E4%B9%89%E5%A4%9A%E7%BA%BF%E7%A8%8B%E5%AE%A2%E6%88%B7%E7%AB%AF)
    - [11.2.2 调用订阅函数](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#1122-%E8%B0%83%E7%94%A8%E8%AE%A2%E9%98%85%E5%87%BD%E6%95%B0)
    - [11.2.3 取消订阅](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#1123-%E5%8F%96%E6%B6%88%E8%AE%A2%E9%98%85)
  - [11.3 PollingClient](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#113-pollingclient)
    - [11.3.1 定义客户端](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#1131-%E5%AE%9A%E4%B9%89%E5%AE%A2%E6%88%B7%E7%AB%AF)
    - [11.3.2 订阅](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#1132-%E8%AE%A2%E9%98%85)
    - [11.3.3 取消订阅](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README_CN.md#1133-%E5%8F%96%E6%B6%88%E8%AE%A2%E9%98%85)

# DolphinDB C++ API

DolphinDB C++ API支持以下开发环境：

* Linux
* Windows Visual Studio
* Windows GNU(MinGW)


### 1. 项目编译

### 1.1 在Linux环境下编译项目

#### 1.1.1 环境配置

C++ API需要使用g++ 4.8.5及以上版本。

#### 1.1.2 下载bin文件和头文件

从本GitHub项目中下载以下文件：

- [bin](./bin) (libDolphinDBAPI.so)
- [include](./include) (DolphinDB.h  Exceptions.h  SmartPointer.h  SysIO.h  Types.h  Util.h)

#### 1.1.3 编译main.cpp

在bin和include的同级目录中创建project目录。进入project目录，并创建文件main.cpp：

```C++
#include "DolphinDB.h"
#include "Util.h"
#include <iostream>
#include <string>
using namespace dolphindb; 
using namespace std; 

int main(int argc, char *argv[]){

    DBConnection conn;
    bool ret = conn.connect("111.222.3.44", 8503);
    if(!ret){
        cout<<"Failed to connect to the server"<<endl;
        return 0;
    }
    ConstantSP vector = conn.run("`IBM`GOOG`YHOO");
    int size = vector->rows();
    for(int i=0; i<size; ++i)
        cout<<vector->getString(i)<<endl;
    return 0;
}
``` 

#### 1.1.4 编译

为了兼容旧的编译器，libDolphinDBAPI.so提供了2个版本，一个版本在编译时使用了-D_GLIBCXX_USE_CXX11_ABI=0的选项，放在[bin/linux_x64/ABI0](./bin/linux_x64/ABI0)目录下，另一个版本未使用-D_GLIBCXX_USE_CXX11_ABI=0，放在[bin/linux_x64/ABI1](./bin/linux_x64/ABI1)目录下。下面是使用第一个动态库版本的g++编译命令：

```
g++ main.cpp -std=c++11 -DLINUX -D_GLIBCXX_USE_CXX11_ABI=0 -DLOGGING_LEVEL_2 -O2 -I../include   -lDolphinDBAPI -lpthread -lssl -L../bin/linux_x64/ABI0  -Wl,-rpath,.:../bin/linux_x64/ABI0 -o main

```
 下面是使用另一个动态库版本的g++编译命令：
```
g++ main.cpp -std=c++11 -DLINUX -D_GLIBCXX_USE_CXX11_ABI=1 -DLOGGING_LEVEL_2 -O2 -I../include   -lDolphinDBAPI -lpthread -lssl -L../bin/linux_x64/ABI1  -Wl,-rpath,.:../bin/linux_x64/ABI1 -o main
```

#### 1.1.5 运行

编译成功后，启动DolphinDB，运行main程序并连接到DolphinDB，连接时需要指定IP地址和端口号，如以上程序中的111.222.3.44:8503。

### 1.2 Windows环境下编译

#### 1.2.1 环境配置

本教程使用了Visual Studio 2017 64位版本。

#### 1.2.2 下载bin文件和头文件

将本GitHub项目下载到本地。

#### 1.2.3 创建Visual Studio项目

创建windows console project，导入[include](./include)目录下头文件，创建1.1.3节中的main.cpp文件，导入libDolphinDBAPI.lib，并且配置lib目录。注意：
> 由于VS里默认定义了min/max两个宏，会与头文件中 `min` 和 `max` 函数冲突。为了解决这个问题，在预处理宏定义中需要加入 `__NOMINMAX__` 。
> api源代码中用宏定义LINUX、WINDOWS等区分不同平台，因此在预处理宏定义中需要加入 `WINDOWS`。
#### 1.2.4 编译和运行

启动编译，将对应的libDolphinDBAPI.dll拷贝到可执行程序的输出目录，即可运行。

Windows gnu开发环境与Linux相似，可以参考上一章的Linux编译。

### 2. 建立DolphinDB连接

DolphinDB C++ API 提供的最核心的对象是DBConnection。C++应用可以通过它在DolphinDB服务器上执行脚本和函数，并在两者之间双向传递数据。DBConnection类提供如下主要方法：

| 方法名        | 详情          |
|:------------- |:-------------|
|connect(host, port, [username, password])|将会话连接到DolphinDB服务器|
|login(username,password,enableEncryption)|登陆服务器|
|run(script)|将脚本在DolphinDB服务器运行|
|run(functionName,args)|调用DolphinDB服务器上的函数|
|upload(variableObjectMap)|将本地数据对象上传到DolphinDB服务器|
|initialize()|初始化连接信息|
|close()|关闭当前会话|

C++ API通过TCP/IP协议连接到DolphinDB。使用 `connect` 方法创建连接时，需要提供DolphinDB server的IP和端口。

```C++
DBConnection conn;
bool ret = conn.connect("127.0.0.1", 8848);
```

我们创建连接时也可以使用用户名和密码登录，默认的管理员名称为"admin"，密码是"123456"。

```C++
DBConnection conn; 
bool ret = conn.connect("127.0.0.1", 8848, "admin", "123456"); 
``` 

若未使用用户名及密码连接成功，则脚本在Guest权限下运行。后续运行中若需要提升权限，可以使用 conn.login('admin','123456',true) 登录获取权限。


声明connection变量的时候，有两个可选参数： enableSSL（支持SSL）, enableAYSN（支持一部分）.这两个参数默认值为false.

下面例子是，建立支持SSL而非支持异步的connection，同时服务器端应该添加参数enableHTTPS=true。

```C++
DBConnection conn(true,false)
```

下面建立即不支持SSL，但支持异步的connection。异步情况下，步只能执行DolphinDB脚本和函数， 且不再有返回值，该功能适用于异步写入数据。

```C++
DBConnection conn(false,true)
```

### 3. 运行DolphinDB脚本

通过 `run` 方法运行DolphinDB脚本：

```C++
ConstantSP v = conn.run(" `IBM` GOOG`YHOO");
cout<<v->getString()<<endl;
```

输出结果为：

> ["IBM", "GOOG", "YHOO"]

需要注意的是，脚本的最大长度为65, 535字节。

### 4. 运行DolphinDB函数

除了运行脚本之外，run命令还可以直接在远程DolphinDB服务器上执行DolphinDB内置或用户自定义函数。若 `run` 方法只有一个参数，则该参数为脚本；若 `run` 方法有两个参数，则第一个参数为DolphinDB中的函数名，第二个参数是该函数的参数，为ConstantSP类型的向量。

下面的示例展示C++程序通过 `run` 调用DolphinDB内置的 [add](http://www.dolphindb.cn/cn/help/add.html) 函数。 [add](http://www.dolphindb.cn/cn/help/add.html) 函数有两个参数 x 和 y。参数的存储位置不同，也会导致调用方式的不同。可能有以下三种情况：

* 所有参数都在DolphinDB server端

若变量 x 和 y 已经通过C++程序在服务器端生成，

```C++
conn.run("x = [1, 3, 5]; y = [2, 4, 6]"); 
``` 

那么在C++端要对这两个向量做加法运算，只需直接使用 `run` 即可。

```C++
ConstantSP result = conn.run("add(x,y)");
cout<<result->getString()<<endl;
```

输出结果为：

> [3, 7, 11]

* 仅有一个参数在DolphinDB server端存在

若变量 x 已经通过C++程序在服务器端生成，

```C++
conn.run("x = [1, 3, 5]"); 
``` 

而参数 y 要在C++客户端生成，这时就需要使用“部分应用”方式，把参数 x 固化在 [add](http://www.dolphindb.cn/cn/help/add.html) 函数内。具体请参考[部分应用文档](https://www.dolphindb.cn/cn/help/PartialApplication.html)。

```C++
vector<ConstantSP> args;
ConstantSP y = Util::createVector(DT_DOUBLE, 3);
double array_y[] = {1.5, 2.5, 7};
y->setDouble(0, 3, array_y);
args.push_back(y);
ConstantSP result = conn.run("add{x,}", args);
cout<<result->getString()<<endl;
```

输出结果为：

> [2.5, 5.5, 12]

* 两个参数都待由C++客户端赋值

```C++
vector<ConstantSP> args; 
ConstantSP x = Util::createVector(DT_DOUBLE, 3); 
double array_x[] = {1.5, 2.5, 7}; 
x->setDouble(0, 3, array_x); 
ConstantSP y = Util::createVector(DT_DOUBLE, 3); 
double array_y[] = {8.5, 7.5, 3}; 
y->setDouble(0, 3, array_y); 
args.push_back(x); 
args.push_back(y); 
ConstantSP result = conn.run("add", args); 
cout<<result->getString()<<endl; 
``` 

输出结果为：
> [10, 10, 10]

### 5. 上传数据对象

C++ API提供 `upload` 方法，将本地对象上传到DolphinDB。

下面的例子在C++定义了一个 `createDemoTable` 函数，该函数创建了一个本地的表对象。

```C++
TableSP createDemoTable(){
    vector<string> colNames = {"name", "date"," price"};
    vector<DATA_TYPE> colTypes = {DT_STRING, DT_DATE, DT_DOUBLE};
    int colNum = 3, rowNum = 10000, indexCapacity=10000;
    ConstantSP table = Util::createTable(colNames, colTypes, rowNum, indexCapacity);
    vector<VectorSP> columnVecs;
    for(int i = 0; i < colNum; ++i)
        columnVecs.push_back(table->getColumn(i));

    for(unsigned int i = 0  i < rowNum; ++i){
        columnVecs[0]->set(i, Util::createString("name_"+std::to_string(i)));
        columnVecs[1]->set(i, Util::createDate(2010, 1, i+1));
        columnVecs[2]->set(i, Util::createDouble((rand()%100)/3.0));
    }
    return table;
}
```

需要注意的是，上述例子中采用的 `set` 方法作为一个虚函数，会产生较大的开销，调用 `set` 方法对表的列向量逐个赋值，在数据量很大的情况下会导致效率低下。此外， `createString` , `createDate` , `createDouble` 等构造方法要求操作系统分配内存，反复调用同样会产生很大的开销。

相对合理的做法是定义一个相应类型的数组，通过诸如 setInt(INDEX start, int len, const int* buf) 的方式一次或者多次地将数据批量传给列向量。

当表对象的数据量较小时，可以采用上述例子中的方式生成 TableSP 对象的数据，但是当数据量较多时，建议采用如下方式来生成数据。

```C++
TableSP createDemoTable(){

    vector<string> colNames = {"name", "date", "price"};
    vector<DATA_TYPE> colTypes = {DT_STRING, DT_DATE, DT_DOUBLE};
    int colNum = 3, rowNum = 10000, indexCapacity=10000;
    ConstantSP table = Util::createTable(colNames, colTypes, rowNum, indexCapacity);
    vector<VectorSP> columnVecs;
    for(int i = 0; i < colNum; ++i)
        columnVecs.push_back(table->getColumn(i));

    int array_dt_buf[Util::BUF_SIZE]; //定义date列缓冲区数组
    double array_db_buf[Util::BUF_SIZE]; //定义price列缓冲区数组

    int start = 0;
    int no=0;
    while (start < rowNum) {
        size_t len = std::min(Util::BUF_SIZE, rowNum - start);
        int *dtp = columnVecs[1]->getIntBuffer(start, len, array_dt_buf); //dtp指向每次通过 `getIntBuffer` 得到的缓冲区的头部
        double *dbp = columnVecs[2]->getDoubleBuffer(start, len, array_db_buf); //dbp指向每次通过 `getDoubleBuffer` 得到的缓冲区的头部
        for (int i = 0; i < len; ++i) {
            columnVecs[0]->setString(i+start, "name_"+std::to_string(++no)); //对string类型的name列直接进行赋值，不采用getbuffer的方式
            dtp[i] = 17898+i;
            dbp[i] = (rand()%100)/3.0;
        }
        columnVecs[1]->setInt(start, len, dtp); //写完后使用 `setInt` 将缓冲区写回数组
        columnVecs[2]->setDouble(start, len, dbp); //写完后使用 `setDouble` 将缓冲区写回数组
        start += len;
    }
    return table;
}
``` 
上述例子采用的诸如 `getIntBuffer` 等方法能够直接获取一个可读写的缓冲区，写完后使用 `setInt` 将缓冲区写回数组，这类函数会检查给定的缓冲区地址和变量底层储存的地址是否一致，如果一致就不会发生数据拷贝。在多数情况下，用 `getIntBuffer` 获得的缓冲区就是变量实际的存储区域，这样能减少数据拷贝，提高性能。

以下利用自定义的 `createDemoTable` 函数创建表对象之后，通过 `upload` 方法把它上传到DolphinDB，再从DolphinDB获取这个表的数据，保存到本地对象result并打印。
```C++
TableSP table = createDemoTable();
conn.upload("myTable", table);
string script = "select * from myTable;";
ConstantSP result = conn.run(script);
cout<<result->getString()<<endl;
```

输出结果为：

``` console
name    date       price
------- ---------- ---------
name_1  2019.01.02 27.666667
name_2  2019.01.03 28.666667
name_3  2019.01.04 25.666667
name_4  2019.01.05 5
name_5  2019.01.06 31
...
```

### 6. 读取数据示例

DolphinDB C++ API 不仅支持Int, Float, String, Date, DataTime等多种数据类型，也支持向量(VectorSP), 集合(SetSP), 矩阵(MatrixSP), 字典(DictionarySP), 表(TableSP）等多种数据形式。下面介绍如何通过DBConnection对象，读取并操作DolphinDB的各种形式的对象。

首先加上必要的头文件:

```C++
#include "DolphinDB.h"
#include "Util.h"
``` 

#### 6.1 向量

创建INT类型的向量：

```C++
VectorSP v = conn.run("1..10");
int size = v->size();
for(int i = 0; i < size; ++i)
    cout<<v->getInt(i)<<endl;
```

创建DATE类型的向量：

```C++
VectorSP v = conn.run("2010.10.01..2010.10.30"); 
int size = v->size(); 
for(int i = 0; i < size; ++i)
    cout<<v->getString(i)<<endl;
``` 

#### 6.2 集合

创建一个集合：

```C++
SetSP set = conn.run("set(4 5 5 2 3 11 6)");
cout<<set->getString()<<endl;
```

#### 6.3 矩阵

创建一个矩阵：

```C++
ConstantSP matrix = conn.run("1..6$2:3"); 
cout<<matrix->getString()<<endl; 
``` 

#### 6.4 字典

创建一个字典：

```C++
DictionarySP dict = conn.run("dict(1 2 3, `IBM` MSFT`GOOG)");
cout << dict->get(Util::createInt(1))->getString()<<endl;
```

上例通过 `Util::createInt` 创建Int类型的值，并使用 `get` 方法来获得key为1对应的值。

#### 6.5 表

在C++客户端中执行以下脚本创建一个表：

```C++
string sb; 
sb.append("n=200\n"); 
sb.append("syms= `IBM`C`MS`MSFT`JPM`ORCL`BIDU`SOHU`GE`EBAY`GOOG`FORD`GS`PEP`USO`GLD`GDX`EEM`FXI`SLV`SINA`BAC`AAPL`PALL`YHOO`KOH`TSLA`CS`CISO`SUN\n"); 
sb.append("mytrades=table(09:30:00+rand(18000, n) as timestamp, rand(syms, n) as sym, 10*(1+rand(100, n)) as qty, 5.0+rand(100.0, n) as price); \n"); 
sb.append("select * from mytrades"); 
TableSP table = conn.run(sb); 
```

#### 6.5.1 `getString()`方法获取表的内容

```C++
cout<<table->getString()<<endl; 
``` 

#### 6.5.2 `getColumn()`方法按列获取表的内容

下面的脚本中，首先定义一个VectorSP类型的动态数组columnVecs，用于存放从表中获取的列，然后依次访问columnVecs处理数据。

对于表的各列，我们可以通过`getString()`方法获得每一列的字符串类型数组，再通过C++的数据类型转换函数将数值类型的数据转换成对应的数据类型，从而进行计算。对于时间类型的数据，则需要以字符串的形式存储。

```C++
vector<VectorSP> columnVecs;
int qty[200],sum[200];
double price[200];
for(int i=0; i<200;++i){
    qty[i]=atoi(columnVecs[2]->getString(i).c_str());
    price[i]=atof(columnVecs[3]->getString(i).c_str());
    sum[i]=qty[i]*price[i];
}

for(int i = 0; i < 200; ++i){
    cout<<columnVecs[0]->getString(i)<<", "<<columnVecs[1]->getString(i)<<", "<<sum[i]<<endl;
}
``` 

#### 6.5.3 `getRow()`方法按照行获取表的内容

例如，打印table的第一行，返回的结果是一个字典。

```C++
cout<<table->getRow(0)->getString()<<endl; 

// output
price->37.811678
qty->410
sym->IBM
timestamp->13:45:15
``` 

若要先按行获取table的内容，再对其中的数据进行操作，则需要调用`getMember()`方法来获取对应列的数据。其中，`getMember()`函数的参数不是C++内置的string类型对象，而是DolphinDB C++ API的string类型Constant对象。

```C++
cout<<table->getRow(0)->getMember(Util::createString("price"))->getDouble()<<endl;

// output
37.811678
```

需要注意的是，按行访问table并逐一进行计算实际上非常低效，为了达到更好的性能，建议参考[6.5.2小节](#652-getcolumn方法按列获取表的内容)的方式按列访问table并批量计算。

#### 6.5.4 使用`BlockReaderSP`对象分段读取表数据

对于大数据量的表，API提供了分段读取方法。(此方法仅适用于DolphinDB 1.20.5, 1.10.16及其以上版本)

在C++客户端中执行以下脚本创建一个大数据量的表：
```C++
string script; 
script.append("n=20000\n"); 
script.append("syms= `IBM`C`MS`MSFT`JPM`ORCL`BIDU`SOHU`GE`EBAY`GOOG`FORD`GS`PEP`USO`GLD`GDX`EEM`FXI`SLV`SINA`BAC`AAPL`PALL`YHOO`KOH`TSLA`CS`CISO`SUN\n"); 
script.append("mytrades=table(09:30:00+rand(18000, n) as timestamp, rand(syms, n) as sym, 10*(1+rand(100, n)) as qty, 5.0+rand(100.0, n) as price); \n"); 
conn.run(script); 
```

分段读取数据并用getString()方法获取表的内容, 需要注意的是fetchSize必须不小于8192。
```C++
string sb = "select * from mytrades";
int fetchSize = 8192;
BlockReaderSP reader = conn.run(sb,4,2,fetchSize);//priority=4, parallelism=2
ConstantSP table;
int total = 0;
while(reader->hasNext()){
    table=reader->read();
    total += table->size();
    cout<< "read" <<table->size()<<endl;
    cout<<table->getString()<<endl; 
}
```

#### 6.6 AnyVector

AnyVector是DolphinDB中一种特殊的数据形式，与常规的向量不同，它的每个元素可以是不同的数据类型或数据形式。

```C++
ConstantSP result = conn.run("{1, 2, {1,3,5}, {0.9, 0.8}}");
cout<<result->getString()<<endl;
```

使用 `get` 方法获取第三个元素：

```C++
VectorSP v = result->get(2); 
cout<<v->getString()<<endl; 
``` 

结果是一个Int类型的向量[1,3,5]。

### 7. 保存数据到DolphinDB数据表

DolphinDB数据表按存储方式分为三种:

* 内存表: 数据仅保存在内存中，存取速度最快，但是节点关闭后数据就不存在了。
* 本地磁盘表：数据保存在本地磁盘上。可以从磁盘加载到内存。
* 分布式表：数据分布在不同的节点，通过DolphinDB的分布式计算引擎，仍然可以像本地表一样做统一查询。

#### 7.1 保存数据到DolphinDB内存表

DolphinDB提供多种方式来保存数据到内存表：

* 通过[insert into](http://www.dolphindb.cn/cn/help/insertinto.html)语句保存单条数据
* 通过[tableInsert](http://www.dolphindb.cn/cn/help/tableInsert.html)函数批量保存多条数据
* 通过[tableInsert](http://www.dolphindb.cn/cn/help/tableInsert.html)函数保存数据表

下面分别介绍三种方式保存数据的实例，在例子中使用到的数据表有3个列，分别是STRING, DATE, DOUBLE类型，列名分别为name, date和price。
在DolphinDB中执行以下脚本创建内存表：

```
t = table(100:0, `name` date`price, [STRING, DATE, DOUBLE]); 
share t as tglobal; 
``` 

上面的例子中，我们通过[table](http://www.dolphindb.cn/cn/help/table.html)函数来创建表，指定了表的容量和初始大小、列名和数据类型。由于内存表是会话隔离的，所以普通内存表只有当前会话可见。为了让多个客户端可以同时访问t，我们使用[share](http://www.dolphindb.cn/cn/help/share1.html)在会话间共享内存表。

#### 7.1.1 使用insert into语句保存数据

我们可以采用如下方式保存单条数据。

```C++
char script[100];
sprintf(script, "insert into tglobal values(%s, date(timestamp(%ld)), %lf)", "`a", 1546300800000, 1.5);
conn.run(script);
```

也可以使用[insert into](http://www.dolphindb.cn/cn/help/insertinto.html) 语句保存多条数据，实现如下:

```C++
string script; 
int rowNum=10000, indexCapacity=10000; 
VectorSP names = Util::createVector(DT_STRING, rowNum, indexCapacity); 
VectorSP dates = Util::createVector(DT_DATE, rowNum, indexCapacity); 
VectorSP prices = Util::createVector(DT_DOUBLE, rowNum, indexCapacity); 

int array_dt_buf[Util:: BUF_SIZE]; //定义date列缓冲区数组
double array_db_buf[Util:: BUF_SIZE]; //定义price列缓冲区数组

int start = 0; 
int no=0; 
while (start < rowNum) {

    size_t len = std::min(Util::BUF_SIZE, rowNum - start);
    int *dtp = dates->getIntBuffer(start, len, array_dt_buf); //dtp指向每次通过 `getIntBuffer` 得到的缓冲区的头部
    double *dbp = prices->getDoubleBuffer(start, len, array_db_buf); //dbp指向每次通过 `getDoubleBuffer` 得到的缓冲区的头部
    for (int i = 0; i < len; i++) {
        names->setString(i+start, "name_"+std::to_string(++no)); //对string类型的name列直接进行赋值，不采用getbuffer的方式
        dtp[i] = 17898+i;
        dbp[i] = (rand()%100)/3.0;
    }
    dates->setInt(start, len, dtp); //写完后使用 `setInt` 将缓冲区写回数组
    prices->setDouble(start, len, dbp); //写完后使用 `setDouble` 将缓冲区写回数组
    start += len;

}
vector<string> allnames = {"names", "dates", "prices"}; 
vector<ConstantSP> allcols = {names, dates, prices}; 
conn.upload(allnames, allcols); 

script += "insert into tglobal values(names, dates, prices); tglobal"; 
TableSP table = conn.run(script); 
``` 

#### 7.1.2 使用tableInsert函数批量保存多条数据

在这个例子中，我们利用索引指定TableSP对象的多行数据，将它们批量保存到DolphinDB server上。

```C++
vector<ConstantSP> args;
TableSP table = createDemoTable();
VectorSP range = Util::createPair(DT_INDEX);
range->setIndex(0, 0);
range->setIndex(1, 10);
cout<<range->getString()<<endl;
args.push_back(table->get(range));
conn.run("tableInsert{tglobal}", args);
```

#### 7.1.3 使用tableInsert函数保存TableSP对象

```C++
vector<ConstantSP> args; 
TableSP table = createDemoTable(); 
args.push_back(table); 
conn.run("tableInsert{tglobal}", args); 
``` 

把数据保存到内存表，还可以使用[append!](http://www.dolphindb.cn/cn/help/append1.html)函数，它可以把一张表追加到另一张表。但是，一般不建议通过[append!](http://www.dolphindb.cn/cn/help/append1.html)函数保存数据，因为[append!](http://www.dolphindb.cn/cn/help/append1.html)函数会返回一个表的schema，增加通信量。

```C++
vector<ConstantSP> args;
TableSP table = createDemoTable();
args.push_back(table);
conn.run("append!(tglobal);", args);
```

#### 7.2 保存数据到本地磁盘表

本地磁盘表通用用于静态数据集的计算分析，既可以用于数据的输入，也可以作为计算的输出。它不支持事务，也不持支并发读写。

在DolphinDB中使用以下脚本创建一个本地磁盘表，使用[database](http://www.dolphindb.cn/cn/help/database1.html)函数创建数据库，调用[saveTable](http://www.dolphindb.cn/cn/help/saveTable.html)命令将内存表保存到磁盘中：


``` 
t = table(100:0, `name` date`price, [STRING,DATE,DOUBLE]);
db=database("/home/dolphindb/demoDB");
saveTable(db, t, `dt);
share t as tDiskGlobal;
```

使用[tableInsert](http://www.dolphindb.cn/cn/help/tableInsert.html)函数是向本地磁盘表追加数据最为常用的方式。这个例子中，我们使用[tableInsert](http://www.dolphindb.cn/cn/help/tableInsert.html)向共享的内存表tDiskGlobal中插入数据，接着调用[saveTable](http://www.dolphindb.cn/cn/help/saveTable.html)把插入的数据保存到磁盘上。

```C++
TableSP table = createDemoTable(); 
vector<ConstantSP> args; 
args.push_back(table); 
conn.run("tableInsert{tDiskGlobal}", args); 
conn.run("saveTable(db, tDiskGlobal, `dt); "); 
``` 

本地磁盘表支持使用[append!](http://www.dolphindb.cn/cn/help/append1.html)函数把数据追加到表中：

```C++
TableSP table = createDemoTable();
conn.upload("mt", table);
string script;
script += "db=database(\"/home/demoTable1\");";
script += "tDiskGlobal.append!(mt);";
script += "saveTable(db,tDiskGlobal,`dt);";
conn.run(script);
```

注意：

1. 对于本地磁盘表，[append!](http://www.dolphindb.cn/cn/help/append1.html)函数只把数据追加到内存，如果要保存到磁盘上，必须再次执行[saveTable](http://www.dolphindb.cn/cn/help/saveTable.html)函数。
2. 除了使用[share](http://www.dolphindb.cn/cn/help/share1.html)让表在其他会话中可见，也可以在C++ API中使用[loadTable](http://www.dolphindb.cn/cn/help/loadTable.html)来加载磁盘表，使用[append!](http://www.dolphindb.cn/cn/help/append1.html)来追加数据。但是，我们不推荐这种方法，因为[loadTable](http://www.dolphindb.cn/cn/help/loadTable.html)函数从磁盘加载数据，会消耗大量时间。如果有多个客户端都使用[loadTable](http://www.dolphindb.cn/cn/help/loadTable.html) ，内存中会有多个表的副本，造成数据不一致。

#### 7.3 保存数据到分布式表

分布式表是DolphinDB推荐在生产环境下使用的数据存储方式，它支持快照级别的事务隔离，保证数据一致性。分布式表支持多副本机制，既提供了数据容错能力，又能作为数据访问的负载均衡。下面的例子通过C++ API把数据保存至分布式表。

#### 7.3.1 使用tableInsert函数保存TableSP对象

在DolphinDB中使用以下脚本创建分布式表。[database](http://www.dolphindb.cn/cn/help/database1.html)函数用于创建数据库，对于分布式数据库，路径必须以 dfs 开头。[createPartitionedTable](http://www.dolphindb.cn/cn/help/createPartitionedTable.html)函数用于创建分区表。
``` 
login( `admin, ` 123456)
dbPath = "dfs://SAMPLE_TRDDB";
tableName = `demoTable
db = database(dbPath, VALUE, 2010.01.01..2010.01.30)
pt=db.createPartitionedTable(table(1000000:0, `name` date `price, [STRING,DATE,DOUBLE]), tableName, ` date)
```

使用[loadTable](http://www.dolphindb.cn/cn/help/loadTable.html)方法加载分布式表，通过[tableInsert](http://www.dolphindb.cn/cn/help/tableInsert.html)方式追加数据：

```C++
TableSP table = createDemoTable(); 
vector<ConstantSP> args; 
args.push_back(table); 
conn.run("tableInsert{loadTable('dfs://SAMPLE_TRDDB', `demoTable)}", args); 
``` 

[append!](http://www.dolphindb.cn/cn/help/append1.html)函数也能向分布式表追加数据，但是性能与[tableInsert](http://www.dolphindb.cn/cn/help/tableInsert.html)相比要差，建议不要轻易使用：

```C++
TableSP table = createDemoTable();
conn.upload("mt", table);
conn.run("loadTable('dfs://SAMPLE_TRDDB', `demoTable).append!(mt);");
conn.run(script);
```

#### 7.3.2 分布式表的并发写入

DolphinDB的分布式表支持并发读写，下面展示如何在C++客户端中将数据并发写入DolphinDB的分布式表。

首先，在DolphinDB服务端执行以下脚本，创建分布式数据库"dfs://natlog"和分布式表"natlogrecords"。其中，数据库按照VALUE-HASH-HASH的组合进行三级分区。

```
dbName="dfs://natlog"
tableName="natlogrecords"
db1 = database("", VALUE, datehour(2019.09.11T00:00:00)..datehour(2019.12.30T00:00:00) )//starttime,  newValuePartitionPolicy=add
db2 = database("", HASH, [IPADDR, 50]) //source_address 
db3 = database("", HASH,  [IPADDR, 50]) //destination_address
db = database(dbName, COMPO, [db1,db2,db3])
data = table(1:0, ["fwname","filename","source_address","source_port","destination_address","destination_port","nat_source_address","nat_source_port","starttime","stoptime","elapsed_time"], [SYMBOL,STRING,IPADDR,INT,IPADDR,INT,IPADDR,INT,DATETIME,DATETIME,INT])
db.createPartitionedTable(data,tableName,`starttime`source_address`destination_address)
```

> 请注意：DolphinDB不允许多个writer同时将数据写入到同一个分区，因此在客户端多线程并行写入数据时，需要确保每个线程分别写入不同的分区。

对于按哈希值进行分区的分布式表， DolphinDB C++ API 提供了`getHash`函数来数据的hash值。在客户端设计多线程并发写入分布式表时，根据哈希分区字段数据的哈希值分组，每组指定一个写线程。这样就能保证每个线程同时将数据写到不同的哈希分区。

```C++
ConstantSP spIP = Util::createConstant(DT_IP);
int key = spIP->getHash(BUCKETS);
```

开启生产数据和消费数据的线程，下面的`genData`用于生成模拟数据，`writeData`用于写数据。

```C++
for (int i = 0; i < tLong; ++i) {
    arg[i].index = i;
    arg[i].count = tLong;
    arg[i].nLong = nLong;
    arg[i].cLong = cLong;
    arg[i].nTime = 0;
    arg[i].nStarttime = sLong;
    genThreads[i] = std::thread(genData, (void *)&arg[i]);
    writeThreads[i] = std::thread(writeData, (void *)&arg[i]);
}
```

每个生产线程首先生成数据，其中`createDemoTable`函数用于产生模拟数据，并返回一个TableSP对象。

```C++
void *genData(void *arg) {
  struct parameter *pParam;
  pParam = (struct parameter *)arg;
  long partitionCount = BUCKETS / pParam->count;

  for (unsigned int i = 0; i < pParam->nLong; i++) {
    TableSP table =
        createDemoTable(pParam->cLong, partitionCount * pParam->index, partitionCount, pParam->nStarttime, i * 5);
    tableQueue[pParam->index]->push(table);
  }
  return NULL;
}
```

每个消费线程开始向DolphinDB并行写入数据。

```C++
void *writeData(void *arg) {
    struct parameter *pParam;
    pParam = (struct parameter *)arg;

    TableSP table;
    for (unsigned int i = 0; i < pParam->nLong; i++) {
        tableQueue[pParam->index]->pop(table);
        long long startTime = Util::getEpochTime();
        vector<ConstantSP> args;
        args.push_back(table);
        conn[pParam->index].run("tableInsert{loadTable('dfs://natlog', `natlogrecords)}", args);
        pParam->nTime += Util::getEpochTime() - startTime;
    }
    printf("Thread %d,insert %ld rows %ld times, used %ld ms.\n", pParam->index, pParam->cLong, pParam->nLong, pParam->nTime);
    return NULL;
}
```

更多分布式表的并发写入案例可以参考样例[MultiThreadDFSWriting.cpp](./example/DFSWritingWithMultiThread/MultiThreadDfsWriting.cpp)。

### 8. 注意事项

1. DBConnection类的所有函数都不是线程安全的，不可以并行调用，否则可能会导致程序崩溃。

关于C++ API的更多信息，可以参考C++ API 头文件[dolphindb.h](./include/DolphinDB.h)。

# C++ Streaming API

C++ API处理流数据的方式有三种：ThreadedClient, ThreadPooledClient 和 PollingClient。

三种实现方式可以参考[test/StreamingThreadedClientTester.cpp](./test/StreamingThreadedClientTester.cpp), [test/StreamingThreadPooledClientTester.cpp](./test/StreamingThreadPooledClientTester.cpp) 和 [test/StreamingPollingClientTester.cpp](./test/StreamingPollingClientTester.cpp)。

### 9 编译

#### 9.1 Linux 64位

#### 9.1.1 通过cmake

安装cmake：

``` bash
sudo apt-get install cmake
```

编译：

``` bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ../path_to_api-cplusplus/
make -j `nproc` 
```

编译成功后，会生成三个可执行文件。

#### 10 在Windows中使用MinGW编译

安装[MinGW](http://www.mingw.org/)和[cmake](https://cmake.org/):

``` bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release `path_to_api-cplusplus` -G "MinGW Makefiles"
mingw32-make -j `nproc` 
```

编译成功后，会生成三个可执行文件。

注意：

1. 编译前，需要把libDolphinDBAPI.dll复制到编译目录。
2. 执行例子前，需要把libDolphinDBAPI.dll和libgcc_s_seh-1.dll复制到可执行文件的相同目录下。

### 11. API

#### 11.1 ThreadedClient

ThreadedClient 产生一个线程。每次新数据从流数据表发布时，该线程去获取和处理数据。

#### 11.1.1 定义线程客户端

``` 
ThreadedClient::ThreadClient(int listeningPort);
```

* listeningPort 是单线程客户端的订阅端口号。

#### 11.1.2 调用订阅函数

``` 
ThreadSP ThreadedClient::subscribe(string host, int port, MessageHandler handler, string tableName, string actionName = DEFAULT_ACTION_NAME, int64_t offset = -1, bool resub = true, VectorSP filter = nullptr);
```

* host是发布端节点的主机名。

* port是发布端节点的端口号。

* handler是用户自定义的回调函数，用于处理每次流入的消息。函数的参数是流入的消息，每条消息就是六数据表的一行。函数的结果必须是void。

* tableName是字符串，表示发布端上共享流数据表的名称。

* actionName是字符串，表示订阅任务的名称。它可以包含字母、数字和下划线。

* offset是整数，表示订阅任务开始后的第一条消息所在的位置。消息是流数据表中的行。如果没有指定offset，或它为负数或超过了流数据表的记录行数，订阅将会从流数据表的当前行开始。offset与流数据表创建时的第一行对应。如果某些行因为内存限制被删除，在决定订阅开始的位置时，这些行仍然考虑在内。

* resub是布尔值，表示订阅中断后，是否会自动重订阅。

* filter是一个向量，表示过滤条件。流数据表过滤列在filter中的数据才会发布到订阅端，不在filter中的数据不会发布。

ThreadSP 指向循环调用handler的线程的指针。该线程在此topic被取消订阅后会退出。

示例：

``` 
auto t = client.subscribe(host, port, [](Message msg) {
    // user-defined routine
    }, tableName);
t->join();
```

#### 11.1.3 取消订阅

``` 
void ThreadClient::unsubscribe(string host, int port, string tableName, string actionName = DEFAULT_ACTION_NAME);
```

* host 是发布端节点的主机名。

* port 是发布端节点的端口号。

* tableName 是字符串，表示发布端上共享流数据表的名称。

* actionName 是字符串，表示订阅任务的名称。它可以包含字母、数字和下划线。

该函数用于停止向发布者订阅数据。

#### 11.2 ThreadPooledClient

ThreadPooledClient 产生用户指定数量的多个线程。每次新数据从流数据表发布时，这些线程同时去获取和处理数据。当数据到达速度超过单个线程所能处理的限度时，ThreadPooledClient 比 ThreadedClient 有优势。

#### 11.2.1 定义多线程客户端

``` 
ThreadPooledClient::ThreadPooledClient(int listeningPort, int threadCount);
```

* listeningPort 是多线程客户端节点的订阅端口号。

* threadCount 是线程池的大小。

#### 11.2.2 调用订阅函数

``` 
vector<ThreadSP> ThreadPooledClient::subscribe(string host, int port, MessageHandler handler, string tableName, string actionName = DEFAULT_ACTION_NAME, int64_t offset = -1, bool resub = true, VectorSP filter = nullptr);
```

参数参见2.1.2节。

返回一个指针向量，每个指针指向循环调用handler的线程。这些线程在此topic被取消订阅后会退出。

示例：

``` 
auto vec = client.subscribe(host, port, [](Message msg) {
    // user-defined routine
    }, tableName);
for(auto& t : vec) {
    t->join();
}
```

#### 11.2.3 取消订阅

``` 
void ThreadPooledClient::unsubscribe(string host, int port, string tableName, string actionName = DEFAULT_ACTION_NAME);
```

参数参见2.1.3节。

#### 11.3 PollingClient

订阅数据时，会返回一个消息队列。用户可以从其中获取和处理数据。

#### 11.3.1 定义客户端

``` 
PollingClient::PollingClient(int listeningPort);
```

* listeningPort 是客户端节点的订阅端口号。

#### 11.3.2 订阅

``` 
MessageQueueSP PollingClient::subscribe(string host, int port, string tableName, string actionName = DEFAULT_ACTION_NAME, int64_t offset = -1);
```

参数参见2.1.2节。

该函数返回指向消息队列的指针。

示例：

``` 
auto queue = client.subscribe(host, port, handler, tableName);
Message msg;
while(true) {
    if(queue->poll(msg, 1000)) {
        if(msg.isNull()) break;
        // handle msg
    }
}
```

#### 11.3.3 取消订阅

``` 
void PollingClient::unsubscribe(string host, int port, string tableName, string actionName = DEFAULT_ACTION_NAME);
```

参数参见2.1.3节。

注意，对于这种订阅模式，取消订阅时，会返回一个空指针，用户需要自行处理。

