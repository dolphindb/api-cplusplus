# DolphinDB C++ API

DolphinDB C++ API支持以下开发环境：

* Linux
* Windows Visual Studio
* Windows GNU(MinGW)

本教程介绍以下内容：
- [1. 编译libDolphinDBAPI](#1-编译libdolphindbapi)
- [2. 项目编译](#2-项目编译)
- [3. 建立DolphinDB连接](#3-建立dolphindb连接)
- [4. 运行DolphinDB脚本](#4-运行dolphindb脚本)
- [5. 运行DolphinDB函数](#5-运行dolphindb函数)
- [6. 上传数据对象](#6-上传数据对象)
- [7. 读取数据示例](#7-读取数据示例)
- [8. 保存数据到DolphinDB数据表](#8-保存数据到dolphindb数据表)
- [9. C++ Streaming API](#9-c-streaming-api)
- [10. openssl 1.0.2版本源码安装](#10-openssl-102版本源码安装)



## 1. 编译libDolphinDBAPI

用户可以使用bin目录下已编译的libDolphinDBAPI, 也可以通过如下方法自己编译libDolphinDBAPI。 

### 1.1 在Linux环境下编译API

#### 编译libuuid

DolphinDB API会用到libuuid，所以要先编译libuuid的静态库。编译方法如下:

* 下载 [libuuid-1.0.3.tar.gz](https://sourceforge.net/projects/libuuid/files/)

* 解压：tar -xvf libuuid-1.0.3.tar.gz

* cd libuuid-1.0.3 && ./configure

* 修改makefile： 添加 '-fPIC' 到CFLAGS和CPPFLAGS

* 如果编译成功， libuuid.a 会生成在目录 '.libs'下

* 将libuuid.a拷贝到目录DolphinDBAPI

#### 编译libDolphinDBAPI

编译命令：

``` 
cd api-cplusplus
make clean & make -j4
```

如果编译成功，会自动生成libDolphinDBAPI.so 


### 1.2 在Windows环境下用MinGW编译API

编译命令：

```
cd api-cplusplus
mingw32-make -f makefile.win32
```


### 1.3 在Windows环境下，用Visual Studio 2017编译API



#### 创建项目 libDolphinDBAPI

Windows Desktop->Dynamic Link Library (DLL) 

#### 配置属性

配置属性 -> 常规 -> 项目默认值 -> 配置类型 -> 动态库(.dll)

#### 下载并将 [Openssl](https://www.npcglib.org/~stathis/blog/precompiled-openssl/)加入include和lib路径：


1. 配置属性页->VC++ 目录 -> 包含目录 ->C:\openssl-1.0.2l-vs2017\include64;
2. 配置属性页->VC++ 目录 -> 库目录 -> C:\openssl-1.0.2l-vs2017\lib64;

#### 添加下面的宏定义

C/C++ -> 预处理器 -> 预处理器定义 ->WIN32_LEAN_AND_MEAN; _WINSOCK_DEPRECATED_NO_WARNINGS;_CRT_SECURE_NO_WARNINGS;WINDOWS;NOMINMAX;NDEBUG;CPPAPI_EXPORTS;_WINDOWS;_USRDLL;



#### 预编译选项（Precompiled header）
C/C++ -> 预编译头 -> 预编译头-> 不使用预编译头


#### 链接（Linker）: 

连接器 -> 输入 -> 附加依赖项
ws2_32.lib
ssleay32MD.lib
libeay32MD.lib

#### 添加源码：

移除项目中代码，并添加src目录的源码到项目中。

#### 编译

编译的时候选择release和x64.如果编译成功，在/username/source/repos/libDolphinDBPAI/x64/Release目录下会生成：libDolphinDBAPI.lib和libDolphinDBAPI.dll。

> 更详细的介绍请参阅[用VS2017编译DolphinDB C++ API动态库](https://github.com/dolphindb/Tutorials_CN/blob/master/cpp_api_vs2017_tutorial.md)

## 2. 项目编译

### 2.1 在Linux环境下编译项目

#### 2.1.1 环境配置

C++ API需要使用g++ 4.8.5及以上版本。

#### 2.1.2 下载bin文件和头文件

从本GitHub项目中下载以下文件：

- [bin](./bin) (libDolphinDBAPI.so)
- [include](./include) (DolphinDB.h, Exceptions.h, SmartPointer.h, SysIO.h, Types.h, Util.h)

#### 2.1.3 编译main.cpp

在bin和include的同级目录中创建project目录。进入project目录，并创建文件main.cpp：

```cpp
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

#### 2.1.4 编译

为了兼容旧的编译器，libDolphinDBAPI.so提供了2个版本，一个版本在编译时使用了-D_GLIBCXX_USE_CXX11_ABI=0的选项，放在[bin/linux_x64/ABI0](./bin/linux_x64/ABI0)目录下，另一个版本未使用-D_GLIBCXX_USE_CXX11_ABI=0，放在[bin/linux_x64/ABI1](./bin/linux_x64/ABI1)目录下。

另外由于DolphinDB添加了(Linux64 稳定版>=1.10.17,最新版>=1.20.6) SSL的支持， 所以编译前需要安装openssl。

>注：当前需要openssl版本为1.0.2，高版本的1.1版本会报错。如果系统自带的openssl版本不是1.0.2的话，可以参考[openssl源码安装](#10-openssl-102版本源码安装)，或者使用已有的二进制包安装。查看openssl版本的命令为：```openssl version```

以下是使用第一个动态库版本的g++编译命令：
```
g++ main.cpp -std=c++11 -DLINUX -D_GLIBCXX_USE_CXX11_ABI=0 -DLOGGING_LEVEL_2 -O2 -I../include   -lDolphinDBAPI -lpthread -lssl -L../bin/linux_x64/ABI0  -Wl,-rpath,.:../bin/linux_x64/ABI0 -o main
```

以下是使用另一个动态库版本的g++编译命令：
```
g++ main.cpp -std=c++11 -DLINUX -D_GLIBCXX_USE_CXX11_ABI=1 -DLOGGING_LEVEL_2 -O2 -I../include   -lDolphinDBAPI -lpthread -lssl -L../bin/linux_x64/ABI1  -Wl,-rpath,.:../bin/linux_x64/ABI1 -o main
```

#### 2.1.5 运行

编译成功后，启动DolphinDB，运行main程序并连接到DolphinDB，连接时需要指定IP地址和端口号，如以上程序中的111.222.3.44:8503。

### 2.2 Windows环境下编译
本节将简要介绍在windows上如何使用C++ API,更详细的介绍请参阅[用VS2017编译DolphinDB C++ API动态库](https://github.com/dolphindb/Tutorials_CN/blob/master/cpp_api_vs2017_tutorial.md#4-%E6%A1%88%E4%BE%8B%E9%AA%8C%E8%AF%81)第4节。

#### 2.2.1 环境配置

本教程使用了Visual Studio 2017 64位版本。

#### 2.2.2 下载bin文件和头文件

将本GitHub项目下载到本地。

#### 2.2.3 创建Visual Studio项目

创建windows console project，导入[include](./include)目录下头文件，创建1.1.3节中的main.cpp文件，导入libDolphinDBAPI.lib，并且配置lib目录。

请注意：
> 由于VS里默认定义了min/max两个宏，会与头文件中 `min` 和 `max` 函数冲突。为了解决这个问题，在预处理宏定义中需要加入 __NOMINMAX__。
> API源代码中用宏定义LINUX、WINDOWS等区分不同平台，因此在预处理宏定义中需要加入 WINDOWS。

#### 2.2.4 编译和运行

启动编译，将对应的libDolphinDBAPI.dll拷贝到可执行程序的输出目录，即可运行。

Windows gnu开发环境与Linux相似，可以参考上一章的Linux编译。

## 3. 建立DolphinDB连接

DolphinDB C++ API 提供的最核心的对象是DBConnection。C++应用可以通过它在DolphinDB服务器上执行脚本和函数，并在两者之间双向传递数据。DBConnection类提供如下主要方法：

| 方法名        | 详情          |
|:------------- |:-------------|
|DBConnection([enableSSL, asynTask,keepAliveTime,compress])|构造对象|
|connect(host, port, [username, password,initialScript,highAvailability,highAvailabilitySites,keepAliveTime])|将会话连接到DolphinDB服务器|
|login(username,password,enableEncryption)|登陆服务器|
|run(script)|将脚本在DolphinDB服务器运行|
|run(functionName,args)|调用DolphinDB服务器上的函数|
|upload(variableObjectMap)|将本地数据对象上传到DolphinDB服务器|
|initialize()|初始化连接信息|
|close()|关闭当前会话|

C++ API通过TCP/IP协议连接到DolphinDB。使用 `connect` 方法创建连接时，需要提供DolphinDB server的IP和端口。

```cpp
DBConnection conn;
bool ret = conn.connect("127.0.0.1", 8848);
```

声明connection变量的时候，有两个可选参数：enableSSL（支持SSL），asynTask（支持一部分）。这两个参数默认值为false。 目前只支持linux, 稳定版>=1.10.17,最新版>=1.20.6。  

下面例子是，建立支持SSL而非支持异步的connection，每30秒做一次心跳检测，要求数据进行压缩。服务器端应该添加参数enableHTTPS=true(单节点部署，需要添加到dolphindb.cfg;集群部署需要添加到cluster.cfg)。

```cpp
DBConnection conn(true,false,30,true)
```

下面建立不支持SSL，但支持异步的connection。异步情况下，只能执行DolphinDB脚本和函数， 且不再有返回值。该功能适用于异步写入数据。

```cpp
DBConnection conn(false,true)
```

创建连接时也可以使用用户名和密码登录，默认的管理员名称为"admin"，密码是"123456"。

```cpp
DBConnection conn; 
bool ret = conn.connect("127.0.0.1", 8848, "admin", "123456"); 
```

若未使用用户名及密码连接成功，则脚本在Guest权限下运行。后续运行中若需要提升权限，可以使用 conn.login('admin','123456',true) 登录获取权限。

请注意，DBConnection类的所有函数都不是线程安全的，不可以并行调用，否则可能会导致程序崩溃。

## 4. 运行DolphinDB脚本

通过 `run` 方法运行DolphinDB脚本：

```cpp
ConstantSP v = conn.run("`IBM`GOOG`YHOO");
cout<<v->getString()<<endl;
```

输出结果为：

> ["IBM", "GOOG", "YHOO"]

## 5. 运行DolphinDB函数

除了运行脚本之外，run命令还可以直接在远程DolphinDB服务器上执行DolphinDB内置或用户自定义函数。若 `run` 方法只有一个参数，则该参数为脚本；若 `run` 方法有两个参数，则第一个参数为DolphinDB中的函数名，第二个参数是该函数的参数，为ConstantSP类型的向量。

下面的示例展示C++程序通过 `run` 调用DolphinDB内置的 [`add`](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/a/add.html) 函数。[`add`](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/a/add.html) 函数有两个参数 x 和 y。参数的存储位置不同，也会导致调用方式的不同。可能有以下三种情况：

* 所有参数都在DolphinDB server端

若变量 x 和 y 已经通过C++程序在服务器端生成，

```cpp
conn.run("x = [1, 3, 5]; y = [2, 4, 6]"); 
```

那么在C++端要对这两个向量做加法运算，只需直接使用 `run` 即可。

```cpp
ConstantSP result = conn.run("add(x,y)");
cout<<result->getString()<<endl;
```

输出结果为：

> [3, 7, 11]

* 仅有一个参数在DolphinDB server端存在

若变量 x 已经通过C++程序在服务器端生成，

```cpp
conn.run("x = [1, 3, 5]"); 
```

而参数 y 要在C++客户端生成，这时就需要使用“部分应用”方式，把参数 x 固化在 [`add`](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/a/add.html) 函数内。具体请参考[部分应用文档](https://www.dolphindb.cn/cn/help/Functionalprogramming/PartialApplication.html)。

```cpp
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

```cpp
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

## 6. 上传数据对象

C++ API提供 `upload` 方法，将本地对象上传到DolphinDB。

### 6.1 上传表对象

下面的例子在 C++ 端定义了一个 `createDemoTable` 函数，该函数创建了一个本地的表对象。

```cpp
TableSP createDemoTable(){
    vector<string> colNames = {"name", "date","price"};
    vector<DATA_TYPE> colTypes = {DT_STRING, DT_DATE, DT_DOUBLE};
    int colNum = 3, rowNum = 10000, indexCapacity=10000;
    ConstantSP table = Util::createTable(colNames, colTypes, rowNum, indexCapacity);
    vector<VectorSP> columnVecs;
    for(int i = 0; i < colNum; ++i)
        columnVecs.push_back(table->getColumn(i));

    for(unsigned int i = 0; i < rowNum; ++i){
        columnVecs[0]->set(i, Util::createString("name_"+std::to_string(i)));
        columnVecs[1]->set(i, Util::createDate(2010, 1, i+1));
        columnVecs[2]->set(i, Util::createDouble((rand()%100)/3.0));
    }
    return table;
}
```

需要注意的是，上述例子中采用的 `set` 方法作为一个虚函数，会产生较大的开销。调用 `set` 方法对表的列向量逐个赋值，在数据量很大的情况下会导致效率低下。此外，`createString`, `createDate` 与 `createDouble` 等构造方法要求操作系统为其分配内存，反复调用同样会产生很大的内存开销。

相对合理的做法是定义一个相应类型的数组，通过诸如 setInt(INDEX start, int len, const int* buf) 的方式一次或者多次地将数据批量传给列向量。

当表对象的数据量较小时，可以采用上述例子中的方式生成 TableSP 对象的数据，但是当数据量较多时，建议采用如下方式来生成数据。

```cpp
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
上述例子采用的诸如 `getIntBuffer` 等方法能够直接获取一个可读写的缓冲区，写完后使用 `setInt` 将缓冲区写回数组。这类函数会检查给定的缓冲区地址和变量底层储存的地址是否一致，如果一致就不会发生数据拷贝。在多数情况下，用 `getIntBuffer` 获得的缓冲区就是变量实际的存储区域，这样能减少数据拷贝，提高性能。

利用上例中自定义的 `createDemoTable` 函数创建一个表对象。通过 `upload` 方法将它上传到DolphinDB，再从DolphinDB获取这个表的数据，保存到本地对象result并打印。
```cpp
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
### 6.1 上传压缩后的表对象
自 1.30.19 版本开始，C++ API 支持通过 `setColumnCompressMethods` 方法，对表数据压缩后上传，以减少网络传输的开销。`setColumnCompressMethods` 方法可以灵活的为表中每列数据分别指定不同的压缩方式。目前支持 lz4 和 delta 两种压缩算法，但 delta 算法仅可用于 SHORT, INT, LONG 与时间或日期类型数据。   
在网络速度较慢的情况下，推荐对表数据进行压缩。使用方法如下：
1. 创建 DBConnection 对象时，需指定 compress=true 以开启压缩下载功能。
2. 上传数据前，通过 `setColumnCompressMethods` 指定 Table 中每一列的压缩方式。

以下为范例代码：

```cpp
//第四个参数指定为 true 表示开启下载压缩
DBConnection conn_compress(false, false, 7200, true);
//连接服务器
conn_compress.connect(hostName, port);
//创建一个共享流表，包含 DATE 和 LONG 两种类型的列
conn_compress.run("share streamTable(1:0, `time`value,[DATE,LONG]) as table1");
//构造上传的数据，约600000条
const int count = 600000;
vector<int> time(count);
vector<long long>value(count);
int basetime = Util::countDays(2012, 1, 1);
for (int i = 0; i<count; i++) {
    time[i] = basetime + (i % 15);
    value[i] = i;
}
VectorSP timeVector = Util::createVector(DT_DATE, count, count);
VectorSP valueVector = Util::createVector(DT_LONG, count, count);
timeVector->setInt(0, count, time.data());
valueVector->setLong(0, count, value.data());
vector<ConstantSP> colVector{ timeVector,valueVector };
//创建Table
vector<string> colName = { "time","value" };
TableSP table = Util::createTable(colName, colVector);
//指定每一列的压缩方式
vector<COMPRESS_METHOD> typeVec{ COMPRESS_DELTA,COMPRESS_LZ4 };
table->setColumnCompressMethods(typeVec);
//压缩上传Table到服务器的流表（table1）
vector<ConstantSP> args{ table };
int insertCount = conn_compress.run("tableInsert{table1}", args)->getInt();
std::cout << insertCount << std::endl;
```
## 7. 读取数据示例

DolphinDB C++ API 不仅支持Int, Float, String, Date, DataTime等多种数据类型，也支持向量(VectorSP)、集合(SetSP)、矩阵(MatrixSP)、字典(DictionarySP)、表(TableSP）等多种数据形式。下面介绍如何通过DBConnection对象，读取并操作DolphinDB的各种形式的对象。

首先加上必要的头文件:

```cpp
#include "DolphinDB.h"
#include "Util.h"
```

### 7.1 向量

创建INT类型的向量：

```cpp
VectorSP v = conn.run("1..10");
int size = v->size();
for(int i = 0; i < size; ++i)
    cout<<v->getInt(i)<<endl;
```

创建DATE类型的向量：

```cpp
VectorSP v = conn.run("2010.10.01..2010.10.30"); 
int size = v->size(); 
for(int i = 0; i < size; ++i)
    cout<<v->getString(i)<<endl;
```

### 7.2 集合

创建一个集合：

```cpp
SetSP set = conn.run("set(4 5 5 2 3 11 6)");
cout<<set->getString()<<endl;
```

### 7.3 矩阵

创建一个矩阵：

```cpp
ConstantSP matrix = conn.run("1..6$2:3"); 
cout<<matrix->getString()<<endl; 
```

### 7.4 字典

创建一个字典：

```cpp
DictionarySP dict = conn.run("dict(1 2 3, `IBM`MSFT`GOOG)");
cout << dict->get(Util::createInt(1))->getString()<<endl;
```

上例通过 `Util::createInt` 创建Int类型的值，并使用 `get` 方法来获得key为1对应的值。

### 7.5 表

在C++客户端中执行以下脚本创建一个表：

```cpp
string sb; 
sb.append("n=200\n"); 
sb.append("syms= `IBM`C`MS`MSFT`JPM`ORCL`BIDU`SOHU`GE`EBAY`GOOG`FORD`GS`PEP`USO`GLD`GDX`EEM`FXI`SLV`SINA`BAC`AAPL`PALL`YHOO`KOH`TSLA`CS`CISO`SUN\n"); 
sb.append("mytrades=table(09:30:00+rand(18000, n) as timestamp, rand(syms, n) as sym, 10*(1+rand(100, n)) as qty, 5.0+rand(100.0, n) as price); \n"); 
sb.append("select * from mytrades"); 
TableSP table = conn.run(sb); 
```

#### 7.5.1 `getString()`方法获取表的内容

```cpp
cout<<table->getString()<<endl; 
```

#### 7.5.2 `getColumn()`方法按列获取表的内容

下面的脚本中，首先定义一个VectorSP类型的动态数组columnVecs，用于存放从表中获取的列，然后依次访问columnVecs处理数据。

对于表的各列，我们可以通过`getString()`方法获得每一列的字符串类型数组，再通过C++的数据类型转换函数将数值类型的数据转换成对应的数据类型，从而进行计算。对于时间类型的数据，则需要以字符串的形式存储。

```cpp
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

#### 7.5.3 `getRow()`方法按照行获取表的内容

例如，打印table的第一行，返回的结果是一个字典。

```cpp
cout<<table->getRow(0)->getString()<<endl; 

// output
price->37.811678
qty->410
sym->IBM
timestamp->13:45:15
```

如果取某一行中的某一列数据可以通过先调用`getRow`，再调用`getMember`的方法，如下例所示。其中，`getMember()`函数的参数不是C++内置的string类型对象，而是DolphinDB C++ API的string类型Constant对象。

```cpp
cout<<table->getRow(0)->getMember(Util::createString("price"))->getDouble()<<endl;

// output
37.811678
```

需要注意的是，按行访问table并逐一进行计算非常低效。为了达到更好的性能，建议参考[6.5.2小节](#652-getcolumn方法按列获取表的内容)的方式按列访问table并批量计算。

#### 7.5.4 使用`BlockReaderSP`对象分段读取表数据

对于大数据量的表，API提供了分段读取方法。(此方法仅适用于DolphinDB 1.20.5, 1.10.16及其以上版本)

在C++客户端中执行以下脚本创建一个大数据量的表：
```cpp
string script; 
script.append("n=20000\n"); 
script.append("syms= `IBM`C`MS`MSFT`JPM`ORCL`BIDU`SOHU`GE`EBAY`GOOG`FORD`GS`PEP`USO`GLD`GDX`EEM`FXI`SLV`SINA`BAC`AAPL`PALL`YHOO`KOH`TSLA`CS`CISO`SUN\n"); 
script.append("mytrades=table(09:30:00+rand(18000, n) as timestamp, rand(syms, n) as sym, 10*(1+rand(100, n)) as qty, 5.0+rand(100.0, n) as price); \n"); 
conn.run(script); 
```

分段读取数据并用getString()方法获取表的内容, 需要注意的是fetchSize必须不小于8192。
```cpp
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

### 7.6 AnyVector

AnyVector是DolphinDB中一种特殊的数据形式，与常规的向量不同，它的每个元素可以是不同的数据类型或数据形式。

```cpp
ConstantSP result = conn.run("[1, 2, [1,3,5], [0.9, 0.8]]");
cout<<result->getString()<<endl;
```

使用 `get` 方法获取第三个元素：

```cpp
VectorSP v = result->get(2); 
cout<<v->getString()<<endl; 
```

结果是一个Int类型的向量[1,3,5]。

## 8. 保存数据到DolphinDB数据表

DolphinDB数据表按存储方式分为两种:

* 内存表: 数据仅保存在内存中，存取速度最快，但是节点关闭后数据就不存在了。
* 分布式表（DFS表）：数据可保存在不同的节点，亦可保存在同一节点，由分布式文件系统统一管理。路径以"dfs://"开头。

### 8.1 保存数据到DolphinDB内存表

DolphinDB提供多种方式来保存数据到内存表：

* 通过[insert into](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/i/insertInto.html)语句保存单条数据
* 通过[tableInsert](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/t/tableInsert.html)函数批量保存多条数据
* 通过[tableInsert](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/t/tableInsert.html)函数保存数据表

下面分别介绍三种方式保存数据的实例，在例子中使用到的数据表有3列，分别是STRING, DATE, DOUBLE类型，列名分别为name, date和price。
在DolphinDB中执行以下脚本创建内存表：

```cpp
t = table(100:0, `name` date`price, [STRING, DATE, DOUBLE]); 
share t as tglobal; 
```

上面的例子中，我们通过[`table`](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/t/table.html)函数来创建表，指定了表的容量和初始大小、列名和数据类型。由于内存表是会话隔离的，所以普通内存表只有当前会话可见。为了让多个客户端可以同时访问t，我们使用[`share`](https://www.dolphindb.cn/cn/help/FunctionsandCommands/CommandsReferences/share.html)在会话间共享内存表。

#### 8.1.1 使用insert into语句保存数据

可以采用如下方式保存单条数据。

```cpp
char script[100];
sprintf(script, "insert into tglobal values(%s, date(timestamp(%ld)), %lf)", "`a", 1546300800000, 1.5);
conn.run(script);
```

也可以使用[insert into](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/i/insertInto.html) 语句保存多条数据:

```cpp
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

#### 8.1.2 使用tableInsert函数批量保存多条数据

在这个例子中，我们利用索引指定TableSP对象的多行数据，将它们批量保存到DolphinDB server上。

```cpp
vector<ConstantSP> args;
TableSP table = createDemoTable();
VectorSP range = Util::createPair(DT_INDEX);
range->setIndex(0, 0);
range->setIndex(1, 10);
cout<<range->getString()<<endl;
args.push_back(table->get(range));
conn.run("tableInsert{tglobal}", args);
```

#### 8.1.3 使用tableInsert函数保存TableSP对象

```cpp
vector<ConstantSP> args; 
TableSP table = createDemoTable(); 
args.push_back(table); 
conn.run("tableInsert{tglobal}", args); 
```

把数据保存到内存表，还可以使用[append!](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/a/append!.html)函数，它可以把一张表追加到另一张表。但是，一般不建议通过[append!](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/a/append!.html)函数保存数据，因为[append!](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/a/append!.html)函数会返回一个空表，不必要地增加通信量。

```cpp
vector<ConstantSP> args;
TableSP table = createDemoTable();
args.push_back(table);
conn.run("append!(tglobal);", args);
```

### 8.2 保存数据到分布式表

分布式表是DolphinDB推荐在生产环境下使用的数据存储方式，它支持快照级别的事务隔离，保证数据一致性。分布式表支持多副本机制，既提供了数据容错能力，又能作为数据访问的负载均衡。下面的例子通过C++ API把数据保存至分布式表。

#### 8.2.1 使用tableInsert函数保存TableSP对象

在DolphinDB中使用以下脚本创建分布式表。[`database`](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/d/database.html)函数用于创建数据库。分布式数据库地路径必须以"dfs://"
开头。[`createPartitionedTable`](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/c/createPartitionedTable.html)函数用于创建分区表。
``` 
login( `admin, ` 123456)
dbPath = "dfs://SAMPLE_TRDDB";
tableName = `demoTable
db = database(dbPath, VALUE, 2010.01.01..2010.01.30)
pt=db.createPartitionedTable(table(1000000:0, `name` date `price, [STRING,DATE,DOUBLE]), tableName, ` date)
```

使用[`loadTable`](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/l/loadTable.html)方法加载分布式表，通过[`tableInsert`](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/t/tableInsert.html)方式追加数据：

```cpp
TableSP table = createDemoTable(); 
vector<ConstantSP> args; 
args.push_back(table); 
conn.run("tableInsert{loadTable('dfs://SAMPLE_TRDDB', `demoTable)}", args); 
```

[`append!`](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/a/append!.html)函数也能向分布式表追加数据，但是性能与[`tableInsert`](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/t/tableInsert.html)相比要差，建议不要轻易使用：

```cpp
TableSP table = createDemoTable();
conn.upload("mt", table);
conn.run("loadTable('dfs://SAMPLE_TRDDB', `demoTable).append!(mt);");
conn.run(script);
```

#### 8.2.2 分布式表的并发写入

DolphinDB的分布式表支持并发读写，下面展示如何在C++客户端中将数据并发写入DolphinDB的分布式表。

首先，在DolphinDB服务端执行以下脚本，创建分布式数据库"dfs://natlog"和分布式表"natlogrecords"。其中，数据库按照VALUE-HASH-HASH的组合进行三级分区。

```cpp
dbName="dfs://natlog"
tableName="natlogrecords"
db1 = database("", VALUE, datehour(2019.09.11T00:00:00)..datehour(2019.12.30T00:00:00) )//starttime,  newValuePartitionPolicy=add
db2 = database("", HASH, [IPADDR, 50]) //source_address 
db3 = database("", HASH,  [IPADDR, 50]) //destination_address
db = database(dbName, COMPO, [db1,db2,db3])
data = table(1:0, ["fwname","filename","source_address","source_port","destination_address","destination_port","nat_source_address","nat_source_port","starttime","stoptime","elapsed_time"], [SYMBOL,STRING,IPADDR,INT,IPADDR,INT,IPADDR,INT,DATETIME,DATETIME,INT])
db.createPartitionedTable(data,tableName,`starttime`source_address`destination_address)
```

DolphinDB不允许多个writer同时将数据写入到同一个分区，因此在客户端多线程并行写入数据时，需要确保每个线程分别写入不同的分区。

对于按哈希值进行分区的分布式表， DolphinDB C++ API 提供了`getHash`函数来数据的hash值。在客户端设计多线程并发写入分布式表时，可根据哈希分区字段数据的哈希值分组，每组指定一个写线程。这样就能保证每个线程同时将数据写到不同的哈希分区。

```cpp
ConstantSP spIP = Util::createConstant(DT_IP);
int key = spIP->getHash(BUCKETS);
```

开启生产数据和消费数据的线程，下面的`genData`用于生成模拟数据，`writeData`用于写数据。

```cpp
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

```cpp
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

```cpp
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

#### 8.2.3 利用PartitionedTableAppender并发写入分布式表

上述方法较为复杂，C++ API提供了更简便地自动按分区分流数据并行写入的方法:

```cpp
PartitionedTableAppender(string dbUrl, string tableName, string partitionColName, DBConnectionPool& pool);
```

- dbUrl: 分布式数据库地址，若为内存表可设为“”
- tableName: 分布式表名
- partitionColName: 分区字段
- DBConnectionPool: 连接池

使用最新的1.30版本及以上的server，可以使用C++ API中的 PartitionedTableAppender对象来写入分布式表。其基本原理是设计一个连接池，然后获取分布式表的分区信息，将分区分配给连接池来并行写入，一个分区在同一时间只能由一个连接写入。

先在服务器端创建一个数据库 "dfs://SAMPLE_TRDDB" 以及一个分布式表 "demoTable"：

```cpp
login( `admin, `123456)
dbPath = "dfs://SAMPLE_TRDDB";
tableName = `demoTable
if(existsDatabase(dbPath)){
	dropDatabase(dbPath)
}
db = database(dbPath, VALUE, 2010.01.01..2010.01.30)
pt=db.createPartitionedTable(table(1000000:0, `name`date `price, [STRING,DATE,DOUBLE]), tableName, `date)
```

然后在C++客户端创建连接池pool并传入PartitionedTableAppender，使用append方法往分布式表并发写入本地数据:

```cpp
DBConnectionPool pool("localhost", 8848, 20, "admin", "123456");
PartitionedTableAppender appender("dfs://SAMPLE_TRDDB", "demoTable", "date", pool);
TableSP table = createDemoTable();
appender.append(table);
ConstantSP result = conn.run("select * from loadTable('dfs://SAMPLE_TRDDB', `demoTable)");
cout <<  result->getString() << endl;
```

<!-- 不再保存本地磁盘表的例子
### 8.3 保存数据到本地磁盘表

本地磁盘表通用用于静态数据集的计算分析。它不支持事务，也不持支并发读写。

在DolphinDB中使用以下脚本创建一个本地磁盘表，使用[`database`](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/d/database.html)函数创建数据库，调用[`saveTable`](https://www.dolphindb.cn/cn/help/FunctionsandCommands/CommandsReferences/saveTable.html)命令将内存表保存到磁盘中：


``` cpp
t = table(100:0, `name` date`price, [STRING,DATE,DOUBLE]);
db=database("/home/dolphindb/demoDB");
saveTable(db, t, `dt);
share t as tDiskGlobal;
```

使用[`tableInsert`](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/t/tableInsert.html)函数是向本地磁盘表追加数据最为常用的方式。这个例子中，我们使用[`tableInsert`](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/t/tableInsert.html)向共享的内存表tDiskGlobal中插入数据，接着调用[`saveTable`](https://www.dolphindb.cn/cn/help/FunctionsandCommands/CommandsReferences/saveTable.html)把插入的数据保存到磁盘上。

```cpp
TableSP table = createDemoTable(); 
vector<ConstantSP> args; 
args.push_back(table); 
conn.run("tableInsert{tDiskGlobal}", args); 
conn.run("saveTable(db, tDiskGlobal, `dt); "); 
```

本地磁盘表支持使用[`append!`](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/a/append!.html)函数把数据追加到表中：

```cpp
TableSP table = createDemoTable();
conn.upload("mt", table);
string script;
script += "db=database(\"/home/demoTable1\");";
script += "tDiskGlobal.append!(mt);";
script += "saveTable(db,tDiskGlobal,`dt);";
conn.run(script);
```

注意：

1. 对于本地磁盘表，[`append!`](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/a/append!.html)函数只把数据追加到内存，如果要保存到磁盘上，必须再次执行[`saveTable`](https://www.dolphindb.cn/cn/help/FunctionsandCommands/CommandsReferences/saveTable.html)函数。
2. 除了使用[`share`](https://www.dolphindb.cn/cn/help/FunctionsandCommands/CommandsReferences/share.html)让表在其他会话中可见，也可以在C++ API中使用[`loadTable`](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/l/loadTable.html)来加载磁盘表，使用[`append!`](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/a/append!.html)来追加数据。但是，我们不推荐这种方法，因为[`loadTable`](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/l/loadTable.html)函数从磁盘加载数据，会消耗大量时间。如果有多个客户端都使用[`loadTable`](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/l/loadTable.html) ，内存中会有多个表的副本，造成数据不一致。

关于C++ API的更多信息，可以参考C++ API 头文件[dolphindb.h](./include/DolphinDB.h)。
-->
### 8.3 批量异步写入数据

针对单条数据批量写入的场景，DolphinDB C++ API 提供 `BatchTableWrite`, `MultithreadedTableWriter` 类对象用于批量异步追加数据，并在客户端维护了一个数据缓冲队列。当服务器端忙于网络 I/O 时，客户端写线程仍然可以将数据持续写入缓冲队列（该队列由客户端维护）。写入队列后即可返回，从而避免了写线程的忙等。目前，`BatchTableWrite` 支持批量写入数据到内存表、分区表；而 `MultithreadedTableWriter` 支持批量写入数据到内存表、分区表和维度表。

注意对于异步写入：

* API 客户端提交任务到缓冲队列，缓冲队列接到任务后，客户端即认为任务已完成。
* 提供 `getStatus` 等接口查看状态。

#### 8.3.1 BatchTableWriter

`BatchTableWriter` 对象及主要方法介绍如下：

```cpp
BatchTableWriter(const std::string& hostName, int port, const std::string& userId, const std::string& password, bool acquireLock=true)
```
* hostName 连接服务器的IP地址。
* port 连接服务器的端口号。
* userId 是字符串，表示连接服务器的用户名。
* password 是字符串，表示连接服务器的密码。
* acquireLock 是布尔值，表示在使用过程中，API内部是否需要加锁。默认为true, 表示需要加锁。在并发调用API的场景下，建议加锁。

以下是BatchTableWriter对象包含的函数方法介绍：
```cpp
addTable(const string& dbName, const string& tableName="", bool partitioned=true);
```
- dbName: 若为分布式表，需填写数据库名。若为内存表，填写表名。
- tableName: 需要写入的分布式表的表名。内存表时该值为空。
- partitioned: 表示添加的表是否为分区表。设置为true表示是分区表。如果添加的表是未分区表，必需设置partitioned为false.

**请注意:**

* 如果添加的是内存表，需要share该表。
* 表名不可重复添加，需要先移除之前添加的表，否则会抛出异常。

```cpp
insert(const string& dbName, const string& tableName, Fargs)
```

- Fargs：是变长参数，代表插入的一行数据。写入的数据可以使用dolphindb的数据类型，也可以使用C++原生数据类型。数据类型和表中列的类型需要一一对应。数据类型对应关系见下表：

C++ 原生数据类型与DolphinDB数据类型对应关系表

| DolphinDB类型 | C++类型         |
| ------------- | --------------- |
| BOOL          | char            |
| CHAR          | char            |
| SHORT         | short           |
| STRING        | const char*     |
| STRING        | string          |
| SYMBOL        | const char*     |
| SYMBOL        | string          |
| LONG          | long long       |
| NANOTIME      | long long       |
| NANOTIMESTAMP | long long       |
| TIMESTAMP     | long long       |
| FLOAT         | float           |
| DOUBLE        | double          |
| DATE          | int             |
| MONTH         | int             |
| TIME          | int             |
| MINUTE        | int             |
| DATETIME      | int             |
| DATEHOUR      | int             |
| UUID          | unsigned char*  |
| UUID          | unsigned char[] |
| IPADDR        | unsigned char*  |
| IPADDR        | unsigned char[] |
| INT128        | unsigned char*  |
| INT128        | unsigned char[] |

**请注意:**

* 调用insert前需先调用addTable添加表，否则会抛出异常。
* 变长参数个数和数据类型需要与insert表的列数及类型匹配。
* 如果插入过程出现异常导致后台线程退出，再次调用insert会抛出异常，可以调用getUnwrittenData来获取之前所有写入缓冲队列但是没有成功写入服务器的数据（不包括本次insert的数据），然后再removeTable。如果需要再次插入数据，需要重新调用 `addTable`.
* 在移除该表的过程中调用本函数，仍然能够插入成功，但这些插入的数据并不会发送到服务器。移除该表的时候调用insert算是未定义行为，不建议这样写程序。

```cpp
removeTable(const string& dbName, const string& tableName="")
```

释放由addTable添加的表所占用的资源。第一次调用该函数，该函数返回即表示后台线程已退出。

```cpp
getUnwrittenData(const string& dbName, const string& tableName="")
```
获取还未写入的数据，主要是用于的时候获取写入出现错误时，剩下未写入的数据。该函数会取出剩下未写入的数据，这些数据将不会被继续写入，如若需要重新写入，需要再次调用插入函数。

```cpp
getStatus(const string& dbName, const string& tableName="")
```

返回值是由一个整型和两个布尔型组合的元组，分别表示当前写入队列的深度、当前表是否被移除（true: 表示正在被移除），以及后台写入线程是否因为出错而退出。

```cpp
getAllStatus()
```

获取所有当前存在的表的信息，不包含被移除的表。

返回值是一个表，共有六列，对应列的说明如下：

| 列名        | 详情          |
|:------------- |:-------------|
|DatabaseName|数据库名称/内存表名称|
|TableName|表名称/空字符串|
|WriteQueueDepth|当前写入队列深度|
|SendedRows|已成功发送到服务器的行数|
|Removing|表是否正在被移除|
|Finished|后台线程是否因为出错退出|

示例：

```cpp
#include "BatchTableWriter.h"
using namespace dolphindb;
using namespace std;
int main(){
  shared_ptr<BatchTableWriter> btw = make_shared<BatchTableWriter>(host, port, userId, password, true);
  btw->addTable("dfs://demoDB", "demoTable");
  for(int i = 0; i < 1000; i+=3)
    btw->insert("dfs://demoDB", "demoTable", i,i+1,i+2);
  btw->removeTable("dfs://demoDB", "demoTable");
}
```

更多批量异步写入案例，请参考[BatchTableWriterDemo.cpp](./example/BatchTableWriter/BatchTableWriterDemo.cpp)。

#### 8.3.2 MultithreadedTableWriter

`MultithreadedTableWriter` 是对 `BatchTableWriter` 的升级，它的默认功能和 `BatchTableWriter` 一致，但 `MultithreadedTableWriter` 支持多线程的并发写入。

`MultithreadedTableWriter` 对象及主要方法介绍如下：

```cpp
MultithreadedTableWriter(const std::string& host, int port, const std::string& userId, const std::string& password,
                            const string& dbPath, const string& tableName, bool useSSL, bool enableHighAvailability = false, const vector<string> *pHighAvailabilitySites = nullptr,
							int batchSize = 1, float throttle = 0.01f,int threadCount = 1, const string& partitionCol ="",
							const vector<COMPRESS_METHOD> *pCompressMethods = nullptr, Mode mode = M_Append,
                            vector<string> *pModeOption = nullptr);
```

参数说明：

* **host** 字符串，表示所连接的服务器的地址
* **port** 整数，表示服务器端口。 
* **userId** / **password**: 字符串，登录时的用户名和密码。
* **dbPath** 字符串，表示分布式数据库地址。内存表时该参数为空。请注意，1.30.17及以下版本 API，向内存表写入数据时，该参数需填写内存表表名。
* **tableName** 字符串，表示分布式表或内存表的表名。请注意，1.30.17及以下版本 API，向内存表写入数据时，该参数需为空。
* **useSSL** 布尔值，默认值为 False。表示是否启用加密通讯。
* **enableHighAvailability** 布尔值，默认为 False。若要开启 API 高可用，则需要指定 *enableHighAvailability* 参数为 True。
* **pHighAvailabilitySites** 列表类型，表示所有可用节点的 ip:port 构成的 list。
* **batchSize** 整数，表示批处理的消息的数量，默认值是 1，表示客户端写入数据后就立即发送给服务器。如果该参数大于 1，表示数据量达到 *batchSize* 时，*客户端*才会将数据发送给服务器。
* **throttle** 大于 0 的数，单位为秒。若客户端有数据写入，但数据量不足 batchSize，则等待 throttle的时间再发送数据。
* **threadCount** 整数，表示创建的工作线程数量，默认为 1，表示单线程。对于维度表，其值必须为1。
* **partitionCol** 字符串类型，默认为空，仅在 threadCount 大于1时起效。对于分区表，必须指定为分区字段名；如果是流表，必须指定为表的字段名；对于维度表，该参数不起效。
* **pCompressMethods** 列表类型，用于指定每一列采用的压缩传输方式，为空表示不压缩。每一列可选的压缩方式包括：
  * COMPRESS_LZ4: LZ4 压缩
  * COMPRESS_DELTA: DELTAOFDELTA 压缩
* **mode** 表示数据写入的方式，可选值为：M_Append 或 M_Upsert。M_Upsert 表示以 [upsert!](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/u/upsert!.html) 方式追加或更新表数据；M_Append 表示以 [append!](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/a/append!.html) 方式追加表数据。
* **modeOption** 字符串数组，表示不同模式下的扩展选项，目前，仅当 *mode* 指定为 M_Upsert 时有效，表示由 [upsert!](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/u/upsert!.html) 可选参数组成的字符串数组。

以下是 `MultithreadedTableWriter` 对象包含的函数方法介绍：

```cpp
bool insert(ErrorCodeInfo &errorInfo, TArgs... args)
```

函数说明：

插入单行数据。返回一个bool类型，true表示插入成功，false表示失败。


参数说明：

* **errorInfo**：是 ErrorCodeInfo 类，包含 errorCode 和 errorInfo，分别表示错误代码和错误信息。当 errorCode 不为空时，表示 MTW 写入失败，此时，errorInfo 会显示失败的详细信息。之后的版本中会对错误信息进行详细说明，给出错误信息的代码、错误原因及解决办法。另外，ErrorCodeInfo 类提供了 hasError() 和 succeed() 方法用于获取数据插入的结果。hasError() 返回 true，则表示存在错误，否则表示无错误。succeed() 返回 true，则表示插入成功，否则表示插入失败。
* **args**：是变长参数，代表插入的一行数据。


```cpp
void getUnwrittenData(std::vector<std::vector<ConstantSP>*> &unwrittenData);
```

函数说明：

返回一个嵌套列表，表示未写入服务器的数据。

注意：该方法获取到数据资源后， `MultithreadedTableWriter` 将释放这些数据资源。

参数说明：

* **unwrittenData**：嵌套列表，表示未写入服务器的数据，包含发送失败的数据以及待发送的数据两部分



```cpp
bool insertUnwrittenData(std::vector<std::vector<ConstantSP>*> &records, ErrorCodeInfo &errorInfo)
```

函数说明：

将数据插入数据表。返回值同 insert 方法。与 insert 方法的区别在于，insert 只能插入单行数据，而 insertUnwrittenData 可以同时插入多行数据。

参数说明：

* **records**：需要再次写入的数据。可以通过方法 getUnwrittenData 获取该对象。
* **errorInfo**：是ErrorCodeInfo 类，包含 errorCode 和 errorInfo，分别表示错误代码和错误信息。当 errorCode 不为空时，表示 MTW 写入失败，此时，errorInfo 会显示失败的详细信息。之后的版本中会对错误信息进行详细说明，给出错误信息的代码、错误原因及解决办法。另外，ErrorCodeInfo 类提供了 hasError() 和 succeed() 方法用于获取数据插入的结果。hasError() 返回 true，则表示存在错误，否则表示无错误。succeed() 返回 true，则表示插入成功，否则表示插入失败。


```cpp
void getStatus(Status &status);
```

函数说明：

获取 `MultithreadedTableWriter` 对象当前的运行状态。

参数说明：

* **status**：是MultithreadedTableWriter::Status 类，具有以下属性和方法


属性：

* isExiting：写入线程是否正在退出。
* errorCode：错误码。
* errorInfo：错误信息。
* sentRows：成功发送的总记录数。
* unsentRows：待发送的总记录数。
* sendFailedRows：发送失败的总记录数。
* threadStatus：写入线程状态列表。
  - threadId：线程 Id。
  - sentRows：该线程成功发送的记录数。
  - unsentRows：该线程待发送的记录数。
  - sendFailedRows：该线程发送失败的记录数。

方法：

* hasError()：true 表示数据写入存在错误；false 表示数据写入无错误。
* succeed()：true 表示数据写入成功；false 表示数据写入失败。

```cpp
waitForThreadCompletion()
```

函数说明：

调用此方法后，MTW 会进入等待状态，待后台工作线程全部完成后退出等待状态。

`MultithreadedTableWriter` 常规处理流程如下：

```cpp
	//创建连接，并初始化测试环境
	DBConnection conn;
	conn.connect("192.168.1.182", 8848, "admin", "123456");
	conn.run("dbName = 'dfs://valuedb3'\
                if(exists(dbName)){\
                dropDatabase(dbName);\
                }\
                datetest=table(1000:0,`date`symbol`id,[DATE,SYMBOL,LONG]);\
                db = database(directory=dbName, partitionType=HASH, partitionScheme=[INT, 10]);\
                pt=db.createPartitionedTable(datetest,'pdatetest','id');");
    //创建 MTW 对象前，先记录当前时间戳 mtwCreateTime
	conn.run("mtwCreateTime=now()");
	vector<COMPRESS_METHOD> compress;
	compress.push_back(COMPRESS_LZ4);
	compress.push_back(COMPRESS_LZ4);
	compress.push_back(COMPRESS_DELTA);
	MultithreadedTableWriter writer("192.168.1.182", 8848, "admin", "123456", "dfs://valuedb3", "pdatetest", false, false, NULL, 10000, 1, 5, "id", &compress);
	thread t([&]() {
		try {
			ErrorCodeInfo errorInfo;
			//插入100行正确数据 （类型和列数都正确），MTW正常运行
			for (int i = 0; i < 100; i++) {
				if (writer.insert(errorInfo, rand() % 10000, "AAAAAAAB", rand() % 10000) == false) {
					//此处不会执行到
					cout << "insert failed: " << errorInfo.errorInfo << endl;
					break;
				}
			}
			//插入1行数据(类型不匹配)，MTW 立刻发现待插入数据类型不匹配，立刻返回错误信息
			if (writer.insert(errorInfo, rand() % 10000, 222, rand() % 10000) == false) {
				//数据错误，插入列数不匹配数据
				cout << "insert failed: " << errorInfo.errorInfo << endl;// insert failed: Column counts don't match 2

//输出：insert failed: Cannot convert int to SYMBOL

			}
			//插入1行数据(列数不匹配)，MTW 立刻发现待插入数据列数与待插入表的列数不匹配，立刻返回错误信息
			if (writer.insert(errorInfo, rand() % 10000, "AAAAAAAB") == false) {
				cout << "insert failed: " << errorInfo.errorInfo << endl;

//输出：insert failed: Column counts don't match 2

			}
			//制造一次MTW内部无法处理的异常事件：删除在mtwCreateTime之后建立的连接
			conn.run("id = exec sessionid from getSessionMemoryStat() where temporalAdd(gmtime(createTime), 16, 'h') > mtwCreateTime; for(closeid in id)closeSessions(closeid);");
			Util::sleep(2000);//等待2秒，等待MTW检测到这个异常。此时 MTW 立刻终止所有工作线程，并修改状态为错误状态
							  //再插入1行正确数据，MTW 会因为工作线程终止而抛出异常，且不会写入该行数据
			if (writer.insert(errorInfo, rand() % 10000, "AAAAAAAB", rand() % 10000) == false) {
				//这里不会执行
				cout << "insert failed: " << errorInfo.errorInfo << endl;
			}
			//这里不会执行
			cout << "Never run here.";
		}
		catch (exception &e) {
			//MTW 抛出异常
			cerr << "MTW exit with exception: " << e.what() << endl;

//输出：MTW exit with exception: Thread is exiting.

		}
	});
	//检查目前MTW的状态
	MultithreadedTableWriter::Status status;
	writer.getStatus(status);
	if (status.hasError()) {
		cout << "error in writing: " << status.errorInfo << endl;
	}
	//等待插入线程结束
	t.join();
	//等待MTW完全退出
	writer.waitForThreadCompletion();
	//再次检查完成后的MTW状态
	writer.getStatus(status);
	if (status.hasError()) {
		cout << "error after write complete: " << status.errorInfo << endl;

error after write complete: Failed to save the inserted data: Failed to read response header from the

		//获取未写入的数据
		std::vector<std::vector<ConstantSP>*> unwrittenData;
		writer.getUnwrittenData(unwrittenData);
		cout << "unwriterdata length " << unwrittenData.size() << endl;

unwriterdata length 100

		if (!unwrittenData.empty()) {
			try {
				//重新写入这些数据，原有的MTW因为异常退出已经不能用了，需要创建新的MTW
				cout << "create new MTW and write again." << endl;
				MultithreadedTableWriter newWriter("192.168.1.182", 8848, "admin", "123456", "dfs://valuedb3", "pdatetest", false, false, NULL, 10000, 1, 2, "id", &compress);
				ErrorCodeInfo errorInfo;
				//插入未写入的数据
				if (newWriter.insertUnwrittenData(unwrittenData, errorInfo)) {
					//等待写入完成后检查状态
					newWriter.waitForThreadCompletion();
					newWriter.getStatus(status);
					if (status.hasError()) {
						cout << "error in write again: " << status.errorInfo << endl;
					}
				}
				else {
					cout << "error in write again: " << errorInfo.errorInfo << endl;
				}
			}
			catch (exception &e) {
				cerr << "new MTW exit with exception: " << e.what() << endl;
			}
		}
	}
	//检查最后写入结果
	cout << conn.run("select count(*) from pt")->getString() << endl;

count
-----
100

```

调用 writer.insert() 方法向 writer 中写入数据，并通过 writer.getStatus() 获取 writer 的状态。
注意，使用 writer.waitForThreadCompletion() 方法等待 MTW 写入完毕，会终止 MTW 所有工作线程，保留最后一次写入信息。此时如果需要再次将数据写入 MTW，需要重新获取新的 MTW 对象，才能继续写入数据。

由上例可以看出，MTW 内部使用多线程完成数据转换和写入任务。但在 MTW 外部，API 客户端同样支持以多线程方式将数据写入 MTW，且保证了多线程安全。

## 9. C++ Streaming API

C++ API处理流数据的方式有三种：ThreadedClient, ThreadPooledClient 和 PollingClient。这三种实现方式的细节请见[test/StreamingThreadedClientTester.cpp](./test/StreamingThreadedClientTester.cpp), [test/StreamingThreadPooledClientTester.cpp](./test/StreamingThreadPooledClientTester.cpp) 和 [test/StreamingPollingClientTester.cpp](./test/StreamingPollingClientTester.cpp)。

### 9.1 编译

#### 9.1.1 Linux 64位

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

#### 9.1.2 在Windows中使用MinGW编译

安装[MinGW](http://www.mingw.org/)和[cmake](https://cmake.org/):

``` bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release `path_to_api-cplusplus` -G "MinGW Makefiles"
mingw32-make -j `nproc` 
```

编译成功后，会生成三个可执行文件。

注意：
- 1. 编译前，需要把libDolphinDBAPI.dll复制到编译目录。
- 2. 执行例子前，需要把libDolphinDBAPI.dll和libgcc_s_seh-1.dll复制到可执行文件的相同目录下。

### 9.2 API

#### 9.2.1 ThreadedClient

ThreadedClient 产生一个线程。每次新数据从流数据表发布时，该线程去获取和处理数据。

##### 9.2.1.1 定义线程客户端

``` 
ThreadedClient::ThreadClient(int listeningPort);
```

* listeningPort 是单线程客户端的订阅端口号。

##### 9.2.1.2 调用订阅函数

``` 
ThreadSP ThreadedClient::subscribe(string host, int port, MessageHandler handler, string tableName, string actionName = DEFAULT_ACTION_NAME, int64_t offset = -1, bool resub = true, VectorSP filter = nullptr, bool msgAsTable = false, bool allowExists = false, int batchSize = 1, double throttle = 1, string userName="", string password="", const StreamDeserializerSP blobDeserializer = nullptr);
```

* host是发布端节点的主机名。
* port是发布端节点的端口号。
* handler是用户自定义的回调函数，用于处理每次流入的消息。函数的参数是流入的消息，每条消息就是流数据表的一行。函数的结果必须是void。
* tableName是字符串，表示发布端上共享流数据表的名称。
* actionName是字符串，表示订阅任务的名称。它可以包含字母、数字和下划线。
* offset是整数，表示订阅任务开始后的第一条消息所在的位置。消息是流数据表中的行。如果没有指定offset，或它为负数或超过了流数据表的记录行数，订阅将会从流数据表的当前行开始。offset与流数据表创建时的第一行对应。如果某些行因为内存限制被删除，在决定订阅开始的位置时，这些行仍然考虑在内。
* resub是布尔值，表示订阅中断后，是否会自动重订阅。
* filter是一个向量，表示过滤条件。流数据表过滤列在filter中的数据才会发布到订阅端，不在filter中的数据不会发布。
* msgAsTable是布尔值。只有设置了 *batchSize* 参数，才会生效。设置为 True，订阅的数据会转换为 table。设置为 False，订阅的数据会转换成 vector。
* allowExists是布尔值。若设置为 True，则支持对同一个订阅流表使用多个 handler 处理数据。设置为 False，则不支持同一个订阅流表使用多个 handler 处理数据。
* batchSize 是一个整数，表示批处理的消息的数量。如果它是正数，直到消息的数量达到 *batchSize* 时，*handler* 才会处理进来的消息。如果它没有指定或者是非正数，消息到达之后，*handler* 就会马上处理消息。
* throttle是一个整数，表示 *handler* 处理到达的消息之前等待的时间，以秒为单位。默认值为 1。如果没有指定 *batchSize*，*throttle* 将不会起作用。
* userName是一个字符串，表示 API 所连接服务器的登录用户名。
* password是一个字符串，表示 API 所连接服务器的登录密码。
* blobDeserializer是订阅的异构流表对应的反序列化器。

ThreadSP 指向循环调用handler的线程的指针。该线程在此topic被取消订阅后会退出。

示例：

```cpp
auto t = client.subscribe(host, port, [](Message msg) {
    // user-defined routine
    }, tableName);
t->join();
```

##### 9.2.1.3 取消订阅

```cpp
void ThreadClient::unsubscribe(string host, int port, string tableName, string actionName = DEFAULT_ACTION_NAME);
```

* host 是发布端节点的主机名。

* port 是发布端节点的端口号。

* tableName 是字符串，表示发布端上共享流数据表的名称。

* actionName 是字符串，表示订阅任务的名称。它可以包含字母、数字和下划线。

该函数用于停止向发布者订阅数据。

#### 9.2.2 ThreadPooledClient

ThreadPooledClient 产生用户指定数量的多个线程。每次新数据从流数据表发布时，这些线程同时去获取和处理数据。当数据到达速度超过单个线程所能处理的限度时，ThreadPooledClient 比 ThreadedClient 有优势。

##### 9.2.2.1 定义多线程客户端

```cpp 
ThreadPooledClient::ThreadPooledClient(int listeningPort, int threadCount);
```
* listeningPort 是多线程客户端节点的订阅端口号。
* threadCount 是线程池的大小。

##### 9.2.2.2 调用订阅函数

```cpp 
vector<ThreadSP> ThreadPooledClient::subscribe(string host, int port, MessageHandler handler, string tableName, string actionName = DEFAULT_ACTION_NAME, int64_t offset = -1, bool resub = true, VectorSP filter = nullptr);
```

参数参见9.2.1.2节。

返回一个指针向量，每个指针指向循环调用handler的线程。这些线程在此topic被取消订阅后会退出。

示例：

```cpp 
auto vec = client.subscribe(host, port, [](Message msg) {
    // user-defined routine
    }, tableName);
for(auto& t : vec) {
    t->join();
}
```

##### 9.2.2.3 取消订阅

```cpp 
void ThreadPooledClient::unsubscribe(string host, int port, string tableName, string actionName = DEFAULT_ACTION_NAME);
```

参数参见9.2.1.3节。

#### 9.2.3 PollingClient

订阅数据时，会返回一个消息队列。用户可以从其中获取和处理数据。

##### 9.2.3.1 定义客户端

```cpp 
PollingClient::PollingClient(int listeningPort);
```

* listeningPort 是客户端节点的订阅端口号。

##### 9.2.3.2 订阅

```cpp 
MessageQueueSP PollingClient::subscribe(string host, int port, string tableName, string actionName = DEFAULT_ACTION_NAME, int64_t offset = -1);
```

参数参见9.2.1.2节。

该函数返回指向消息队列的指针。

示例：

```cpp 
auto queue = client.subscribe(host, port, handler, tableName);
Message msg;
while(true) {
    if(queue->poll(msg, 1000)) {
        if(msg.isNull()) break;
        // handle msg
    }
}
```

##### 9.2.3.3 取消订阅

```cpp 
void PollingClient::unsubscribe(string host, int port, string tableName, string actionName = DEFAULT_ACTION_NAME);
```

参数参见9.2.1.3节。

注意，对于这种订阅模式，若返回一个空指针，说明已取消订阅。

#### 9.2.4 异构流表反序列化器
DolphinDB server 自 1.30.17 及 2.00.5 版本开始，支持通过 [replay](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/r/replay.html) 函数将多个结构不同的流数据表，回放（序列化）到一个流表里，这个流表被称为异构流表。C++ API 自 1.30.19 版本开始，新增 `StreamDeserializer` 类，用于构造异构流表反序列化器，以实现对异构流表的订阅和反序列化操作。

C++ API 支持通过两种方式构造异构流表反序列化器：

通过 key->schema 的映射表创建
```cpp
StreamDeserializerSP StreamDeserializer(sym2schema)
```
通过 key->table 的映射表创建
```cpp
StreamDeserializerSP StreamDeserializer(sym2schema, [conn])
```
* sym2table 是一个字典对象，其结构与 replay 回放到异构流表的输入表结构保持一致。`StreamDeserializer` 将根据 *sym2table* 指定的结构对注入的数据进行反序列化。
* conn 是已经连接 DolphinDB server 的 DBConnection 对象。若不指定该参数，则 sym2table 中的指定的表必须是 dfs 表或者共享内存表。

##### 9.2.4.1 订阅异构流表

订阅示例：

```cpp
//假设异构流表回放时inputTables如下：
//d = dict(['msg1', 'msg2'], [table1, table2]); \
//replay(inputTables = d, outputTables = `outTables, dateColumn = `timestampv, timeColumn = `timestampv)";
//异构流表解析器的创建方法如下：
StreamDeserializerSP sdsp;
{//使用key->schema的映射表创建
    unordered_map<string, DictionarySP> sym2schema;
    DictionarySP t1s = conn.run("schema(table1)");
    DictionarySP t2s = conn.run("schema(table2)");
    sym2schema["msg1"] = t1s;
    sym2schema["msg2"] = t2s;
    sdsp = new StreamDeserializer(sym2schema);
}
{//使用key->table的映射表创建
    unordered_map<string, pair<string, string>> sym2table;
    //因为是内存表，第一个参数dbpath指定为空，第二个参数指定为表名
    sym2table["msg1"] = std::make_pair("", "table1");
    sym2table["msg2"] = std::make_pair("", "table2");
    //传入map和conn，&conn是可选参数，如果不传入，sym2table中的表必须是dfs表或者共享内存表。
    sdsp = new StreamDeserializer(sym2table, &conn);
}
//sdsp可以作为订阅时的一个参数传入，譬如：
//ThreadedClient threadedClient(listenport);
//auto thread1 = threadedClient.subscribe(hostName, port, onehandler, "outTables", "mutiSchemaOne", 0, true, nullptr, false, false, "admin", "123456", sdsp);
```
#### 9.2.5 订阅跨进程共享内存表
DolphinDB server 自2.00.7/1.30.19版本开始支持创建跨进程共享内存表（[createIPCInMemoryTable](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/c/createIPCInMemoryTable.html)）。C++ API 提供订阅函数 `subscribe` 以实现订阅跨进程共享内存表的功能，来满足对订阅数据时延性要求较高的场景需求。通过订阅跨进程共享内存表，API 端可以直接通过共享内存获取由 server 端发布的流数据，极大地减少了网络传输的延时。因为进程间会访问同一个共享内存，所以要求发布端和订阅端必须位于同一台服务器。本节主要介绍如何通过 C++ API 提供的 `IPCInMemoryStreamClient` 类实现订阅共享内存表的功能。


示例：
1. server 端通过 GUI 创建一个跨进程内存表，并向该表实时写入数据
```cpp
//创建流表
share streamTable(10000:0,`timestamp`temperature, [TIMESTAMP,DOUBLE]) as pubTable;
//创建跨进程共享内存表
share createIPCInMemoryTable(1000000, "pubTable", `timestamp`temperature, [TIMESTAMP, DOUBLE]) as shm_test;
//自定义订阅处理函数
def shm_append(msg) {
    shm_test.append!(msg)
}
//订阅流表pubTable的数据写入跨进程内存表
topic2 = subscribeTable(tableName="pubTable", actionName="act3", offset=0, handler=shm_append, msgAsTable=true)
```
2. C++ API 端
```cpp
string tableName = "pubTable";
//构造对象
IPCInMemoryStreamClient memTable;

//创建一个存储数据的 table，要求和 createIPCInMemoryTable 中列的类型和名称一一对应
vector<string> colNames = {"timestamp", "temperature"};
vector<DATA_TYPE> colTypes = {DT_TIMESTAMP, DT_DOUBLE};
int rowNum = 0, indexCapacity=10000;
TableSP outputTable = Util::createTable(colNames, colTypes, rowNum, indexCapacity); // 创建一个和共享内存表结构相同的表

//overwrite 是否覆盖前面旧的数据
bool overwrite = true;
ThreadSP thread = memTable.subscribe(tableName, print, outputTable, overwrite);

//传入 subscribe 中处理数据的回调函数
void print(TableSP table) {
    //处理收到的数据
}

//最后取消订阅，结束回调
memTable.unsubscribe(tableName);
```

## 10. openssl 1.0.2版本源码安装
这部分主要是介绍下没有1.0.2版本openssl的，从源码编译安装的过程。已有的话忽略本节。


首先创建一个自定义目录，给自己编译的openssl使用。

这个示例里，我们使用/newssl目录，实际可以自己修改。
```console
demo@ddb:~# mkdir /newssl
```

下载openssl源码
```console
demo@ddb:~# wget https://www.openssl.org/source/old/1.0.2/openssl-1.0.2u.tar.gz
```

解压缩后，进入源码目录，配置编译结果存放目录为刚才创建的newssl目录
```console
demo@ddb:~/openssl-1.0.2u# ./config shared --prefix=/newssl
```


编译安装
```console
demo@ddb:~/openssl-1.0.2u# make install
```

因为这里编译结果存放的目录是/newssl，在实际编译时，链接的头文件和库文件目录也需要添加上我们存放的目录/newssl

比如[1.1.4编译](#114-编译)这个例子里，原先的g++编译命令是:

```
g++ main.cpp -std=c++11 -DLINUX -D_GLIBCXX_USE_CXX11_ABI=1 -DLOGGING_LEVEL_2 -O2 -I../include   -lDolphinDBAPI -lpthread -lssl -L../bin/linux_x64/ABI1  -Wl,-rpath,.:../bin/linux_x64/ABI1 -o main
```
要使用我们自己编译的openssl库，需要改成
```
g++ main.cpp -std=c++11 -DLINUX -D_GLIBCXX_USE_CXX11_ABI=1 -DLOGGING_LEVEL_2 -O2 -I../include   -lDolphinDBAPI -lpthread -L../bin/linux_x64/ABI1  -Wl,-rpath,.:../bin/linux_x64/ABI1 -L /newssl/lib/ -I /newssl/include -lssl -lcrypto -luuid -o main
```
本例中，编译文件后，直接运行main会报错，是由于我们的/newssl不在系统路径里，所以在运行main前，可以设置变量
```
export LD_LIBRARY_PATH=/newssl/lib
```
然后再运行./main就可以运行了

## 11. 常见问题及解决办法

1. 编译时出现报错 "undefined reference to 'uuid_generate'"。
   
   **原因：** 未安装libuuid-devel库或者未添加```-luuid```参数。  
   **解决办法：** 通过命令 ```yum install libuuid-devel``` 安装libuuid-devel库，或者在引用它的模块后添加 ```-luuid```参数。