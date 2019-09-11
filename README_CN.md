# DolphinDB C++ API 概述

DolphinDB C++ API支持以下开发环境：

- Linux
- Windows Visual Studio
- Windows GNU(MingW)

本教程主要介绍以下内容：

- 项目编译
- 建立DolphinDB连接
- 运行DolphinDB脚本
- 运行函数
- 上传本地对象到DolphinDB服务器
- 读取数据示例
- 追加数据到DolphinDB数据表

### 1. 项目编译

#### 1.1 在Linux环境下编译项目

#### 1.1.1 环境配置

C++ API需要使用g++ 6.2及以上版本。

#### 1.1.2 下载bin文件和头文件

从这个GitHub项目中下载bin和include目录到自己的项目。

> bin (libDolphinDBAPI.so) 
include (DolphinDB.h  Exceptions.h  SmartPointer.h  SysIO.h  Types.h  Util.h)

#### 1.1.3 编译main.cpp

在bin和include的同级目录中创建project目录，进入project项目，并创建文件main.cpp：

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

g++编译命令：

> g++ main.cpp -std=c++11 -DLINUX -DLOGGING_LEVEL_2 -O2 -I../include -lDolphinDBAPI -lssl  -lpthread -luuid -L../bin  -Wl,-rpath ../bin/ -o main

#### 1.1.5 运行

编译成功后，启动DolphinDB，运行main程序并连接到DolphinDB，连接时需要指定IP地址和端口号，如111.222.3.44:8503。

### 1.2 Windows环境下编译

#### 1.2.1 环境配置

本教程使用了Visual Studio 2017 64位版本。

#### 1.2.2 下载bin和头文件

将本GitHub项目下载到本地。

#### 1.2.3 创建Visual Studio项目

创建win32 console project，导入头文件，创建1.3节中的main.cpp文件，导入libDolphinDBAPI.lib，并且配置lib目录。

> 注意：由于VS里默认定义了min/max两个宏，与头文件中`min`、`max`函数冲突。为了解决这个问题，在预处理宏定义中需要加入 `__NOMINMAX__` 。

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
|initialize()|初始化链接信息|
|close()|关闭当前会话|

C++ API通过TCP/IP协议连接到DolphinDB。使用`connect`方法创建连接时，需要提供DolphinDB server的IP和端口。

```C++
DBConnection conn;
bool ret = conn.connect("111.222.3.44", 8503);
```

我们创建连接时也可以使用用户名和密码登录，默认的管理员名称为“admin”，密码是“123456”。

```C++
DBConnection conn;
bool ret = conn.connect("111.222.3.44", 8503,"admin","123456");
```

若未使用用户名及密码连接成功，则脚本在Guest权限下运行。后续运行中若需要提升权限，可以通过`conn.login('admin','123456',true)`登录获取权限。

### 3. 运行DolphinDB脚本

通过`run(script)`方法运行DolphinDB脚本：

```C++
ConstantSP v = conn.run("`IBM`GOOG`YHOO");
cout<<v->getString()<<endl;
```

输出结果为：

> ["IBM","GOOG","YHOO"]

需要注意的是，脚本的最大长度为65,535字节。

### 4. 运行函数

除了运行脚本之外，run命令可以直接在远程DolphinDB服务器上执行DolphinDB内置或用户自定义函数。`run`方法的第一个参数DolphinDB中的函数名，第二个参数是ConstantSP类型的向量。

下面的示例展示C++程序通过`run`调用DolphinDB内置的`add`函数。`add`函数有两个参数x和y。参数的存储位置不同，也会导致调用方式的不同。可能有以下三种情况：

* 所有参数都在DolphinDB server端

若变量x和y已经通过C++程序在服务器端生成，

```C++
conn.run("x = [1,3,5];y = [2,4,6]");
```

那么在C++端要对这两个向量做加法运算，只需直接使用`run(script)`即可。

```C++
ConstantSP result = conn.run("add(x,y)");
cout<<result->getString()<<endl;
```
输出结果为：
> [3,7,11]

* 仅有一个参数在DolphinDB server端存在

若变量x已经通过C++程序在服务器端生成，

```C++
conn.run("x = [1,3,5]");
```

而参数y要在C++客户端生成，这时就需要使用“部分应用”方式，把参数x固化在`add`函数内。具体请参考[部分应用文档](https://www.dolphindb.com/cn/help/PartialApplication.html)。

```C++
vector<ConstantSP> args;
ConstantSP y = Util::createVector(DT_DOUBLE, 3); 
y->setDouble(0, 1.5);
y->setDouble(1, 2.5);
y->setDouble(2, 7);
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

C++ API提供`upload`方法，将本地对象上传到DolphinDB。

下面的例子首先创建了一个本地的表对象，然后把它上传到DolphinDB，再从DolphinDB获取这个表的数据，保存到本地对象。

```C++
TableSP createDemoTable(){
    vector<string> colNames = {"name","date","price"};
    vector<DATA_TYPE> colTypes = {DT_STRING,DT_DATE,DT_DOUBLE};
    int colNum = 3,rowNum = 20,indexCapacity=20;
    ConstantSP table = Util::createTable(colNames,colTypes,rowNum,100);
    vector<VectorSP> columnVecs;
    for(int i = 0 ;i < colNum ;i ++)
        columnVecs.push_back(table->getColumn(i));

    for(unsigned int i =  0 ;i < rowNum; i++){
        columnVecs[0]->set(i,Util::createString("name_"+std::to_string(i)));
        columnVecs[1]->set(i,Util::createDate(2010,1,i+1));
        columnVecs[2]->set(i,Util::createDouble(i*i));
    }
    return table;
}

TableSP table = createDemoTable();
conn.upload("myTable", table);
string script = "select * from myTable;";
ConstantSP result = conn.run(script);
cout<<result->getString()<<endl;
```

输出结果为：

```console
name    date       price
------- ---------- -----
name_0  2010.01.01 0    
name_1  2010.01.02 1    
name_2  2010.01.03 4    
name_3  2010.01.04 9    
name_4  2010.01.05 16   
name_5  2010.01.06 25   
...
```   

### 6. 读取数据示例

DolphinDB C++ API支持多种数据类型，包括Int, Float, String, Date, DataTime等，以及多种数据形式，包括向量（VectorSP），集合（SetSP），矩阵（MatrixSP），字典（DictionarySP），表（TableSP）等。下面介绍通过DBConnection对象，读取DolphinDB不同类型的数据。

首先加上必要的头文件:

```C++
#include "DolphinDB.h"
#include "Util.h"
```

- 向量

```C++
VectorSP v = conn.run("1..10");
int size = v->size();
for(int i = 0; i < size; i++)
    cout<<v->getInt(i)<<endl;
```

```C++
VectorSP v = conn.run("2010.10.01..2010.10.30");
int size = v->size();
for(int i = 0; i < size; i++)
    cout<<v->getString(i)<<endl;
```

- 集合

```C++
SetSP set = conn.run("set(4 5 5 2 3 11 6)");
cout<<set->getString()<<endl;
```

- 矩阵

```C++
ConstantSP matrix = conn.run("1..6$2:3");
cout<<matrix->getString()<<endl;
```

- 字典

```C++
DictionarySP dict = conn.run("dict(1 2 3,`IBM`MSFT`GOOG)");
cout << dict->get(Util::createInt(1))->getString()<<endl;
```

我们可以用`get`方法来获得key对应的值。上例通过`Util::createInt()`创建Int类型的值，并找到字典中key为1的值。

- 表

```C++
string sb;
sb.append("n=20000\n");
sb.append("syms=`IBM`C`MS`MSFT`JPM`ORCL`BIDU`SOHU`GE`EBAY`GOOG`FORD`GS`PEP`USO`GLD`GDX`EEM`FXI`SLV`SINA`BAC`AAPL`PALL`YHOO`KOH`TSLA`CS`CISO`SUN\n");
sb.append("mytrades=table(09:30:00+rand(18000,n) as timestamp,rand(syms,n) as sym, 10*(1+rand(100,n)) as qty,5.0+rand(100.0,n) as price);\n");
sb.append("select qty,price from mytrades where sym==`IBM;");
TableSP table = conn.run(sb);
cout<<table->getString()<<endl;
```

- AnyVector

AnyVector是DolphinDB中一种特殊的数据形式，与常规的向量不同，它的每个元素可以是不同的数据类型或数据形式。

```C++
ConstantSP result = conn.run("{1, 2, {1,3,5},{0.9, 0.8}}");
cout<<result->getString()<<endl;
```

使用`get`方法获取第三个元素：

```C++
VectorSP v = result->get(2);
cout<<v->getString()<<endl;
```

结果是一个Int类型的向量[1,3,5]。

### 7. 读写DolphinDB数据表
使用C++ API的一个重要场景是，用户从其他数据库系统或是第三方Web API中取得数据后存入DolphinDB数据库中。本节将介绍通过C++ API将取到的数据上传并保存到DolphinDB的数据表中。

DolphinDB数据表按存储方式分为三种:

- 内存表: 数据仅保存在内存中，存取速度最快，但是节点关闭后数据就不存在了。
- 本地磁盘表：数据保存在本地磁盘上。可以从磁盘加载到内存。
- 分布式表：数据分布在不同的节点，通过DolphinDB的分布式计算引擎，仍然可以像本地表一样做统一查询。

#### 7.1 保存数据到DolphinDB内存表

DolphinDB提供多种方式来保存数据到内存表：

- 通过`insert into`语句保存单条数据
- 通过`tableInsert`函数批量保存多条数据
- 通过`tableInsert`函数保存数据表

下面分别介绍三种方式保存数据的实例，在例子中使用到的数据表有3个列，分别是STRING, DATE, DOUBLE类型，列名分别为name, date和price。
在DolphinDB中执行以下脚本创建内存表：

```
t = table(100:0, `name`date`price, [STRING, DATE, DOUBLE]);
share t as tglobal;
```

上面的例子中，我们通过`table`函数来创建表，指定了表的容量和初始大小、列名和数据类型。由于内存表是会话隔离的，所以普通内存表只有当前会话可见。为了让多个客户端可以同时访问t，我们使用`share`在会话间共享内存表。

#### 7.1.1 使用`INSERT INTO`语句保存数据

我们可以采用如下方式保存单条数据。

```C++
char script[100];
sprintf(script, "insert into tglobal values(%s, date(timestamp(%ld)), %lf)", "`a", 1546300800000, 1.5);
conn.run(script);
```

也可以使用`INSERT INTO`语句保存多条数据，实现如下:

```C++
string script;
VectorSP names = Util::createVector(DT_STRING,5,100);
VectorSP dates = Util::createVector(DT_DATE,5,100);
VectorSP prices = Util::createVector(DT_DOUBLE,5,100);
for(int i = 0 ;i < 5;i++){
    names->set(i,Util::createString("name_"+std::to_string(i)));
    dates->set(i,Util::createDate(2010,1,i+1));
    prices->set(i,Util::createDouble(i*i));
}
vector<string> allnames = {"names","dates","prices"};
vector<ConstantSP> allcols = {names,dates,prices};
conn.upload(allnames,allcols); 

script += "insert into tglobal values(names,dates,prices);"; 
TableSP table = conn.run(script); 
```

#### 7.1.2 使用`tableInsert`函数批量保存多条数据

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

#### 7.1.3 使用`tableInsert`函数保存TableSP对象

```C++
vector<ConstantSP> args;
TableSP table = createDemoTable();
args.push_back(table);
conn.run("tableInsert{tglobal}", args); 
```

把数据保存到内存表，还可以使用`append!`函数，它可以把一张表追加到另一张表。但是，一般不建议通过append!函数保存数据，因为`append!`函数会返回一个表结构，增加通信量。

```C++
vector<ConstantSP> args;
TableSP table = createDemoTable();
args.push_back(table);
conn.run("append!(tglobal);", args);
```

#### 7.2 保存数据到本地磁盘表

本地磁盘表通用用于静态数据集的计算分析，既可以用于数据的输入，也可以作为计算的输出。它不支持事务，也不持支并发读写。

在DolphinDB中使用以下脚本创建一个本地磁盘表，使用`database`函数创建数据库，调用`saveTable`函数将内存表保存到磁盘中：

```C++
t = table(100:0, `name`date`price, [STRING, DATE, DOUBLE]); 
db=database("/home/dolphindb/demoDB"); 
saveTable(db,t,`dt); 
share t as tDiskGlobal;
```

使用`tableInsert`函数是向本地磁盘表追加数据最为常用的方式。这个例子中，我们使用`tableInsert`向共享的内存表tDiskGlobal中插入数据，接着调用`saveTable`使插入的数据保存到磁盘上。

```C++
TableSP table = createDemoTable();
vector<ConstantSP> args;
args.push_back(table);
conn.run("tableInsert{tDiskGlobal}",args);
conn.run("saveTable(db,tDiskGlobal,`dt);");
```

本地磁盘表也支持使用`append!`函数把数据追加到表中：

```C++
TableSP table = createDemoTable();
conn.upload("mt",table);
string script;
script += "db=database(\"/home/demoTable1\");";
script += "tDiskGlobal.append!(mt);";
script += "saveTable(db,tDiskGlobal,`dt);";
conn.run(script);
```

注意：

1. 对于本地磁盘表，`append!`函数只把数据追加到内存，如果要保存到磁盘上，必须再次执行`saveTable`函数。
2. 除了使用`share`让表在其他会话中可见，也可以在C++ API中使用`loadTable`来加载磁盘表，使用`append!`来追加数据。但是，我们不推荐这种方法，因为`loadTable`函数从磁盘加载数据，会消耗大量时间。如果有多个客户端都使用`loadTable`，内存中会有多个表的副本，造成数据不一致。

#### 7.3 保存数据到分布式表

分布式表是DolphinDB推荐在生产环境下使用的数据存储方式，它支持快照级别的事务隔离，保证数据一致性。分布式表支持多副本机制，既提供了数据容错能力，又能作为数据访问的负载均衡。下面的例子通过C++ API把数据保存至分布式表。

请注意只有启用enableDFS=1的集群环境才能使用分布式表。

在DolphinDB中使用以下脚本创建分布式表，脚本中，`database`函数用于创建数据库，对于分布式数据库，路径必须以“dfs”开头。`createPartitionedTable`函数用于创建分区表。

```
login(`admin,`123456)
dbPath = "dfs://SAMPLE_TRDDB";
tableName = `demoTable
db = database(dbPath, VALUE, 2010.01.01..2010.01.30)
pt=db.createPartitionedTable(table(1000000:0,`name`date`price,[STRING,DATE,DOUBLE]),tableName,`date)
```

DolphinDB提供的`loadTable`方法同样可以加载分布式表，通过`tableInsert`方式追加数据，具体的脚本示例如下：

```C++
TableSP table = createDemoTable();
vector<ConstantSP> args;
args.push_back(table);
conn.run("tableInsert{loadTable('dfs://SAMPLE_TRDDB', `demoTable)}",args);
```

同样地，`append!`函数也能向分布式表追加数据，但是性能与`tableInsert`相比要差一些，建议不要轻易使用：

```C++
TableSP table = createDemoTable();
conn.upload("mt",table);
conn.run("loadTable('dfs://SAMPLE_TRDDB', `demoTable).append!(mt);");
conn.run(script);
```

关于C++ API的更多信息，可以参考C++ API 头文件`dolphindb.h`。

---

# C++ Streaming API

C++ API处理流数据的方式有三种：ThreadedClient, ThreadPooledClient和PollingClient。

三种实现方式可以参考`test/StreamingThreadedClientTester.cpp`, `test/StreamingThreadPooledClientTester.cpp`和`test/StreamingPollingClientTester.cpp`。

### 1 编译

通过`cmake`和`CMakeLists.txt`，可以在Windows或Linux上编译3种处理流数据的例子。

> [cmake](https://cmake.org/)是非常流行的编译工具。

#### 1.1 Linux 64位

#### 1.1.1 通过cmake

安装cmake：

```bash
sudo apt-get install cmake
```

编译：

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ../path_to_api-cplusplus/
make -j`nproc`
```

编译成功后，会生成三个可执行文件。

#### 1.2 在Windows中使用MinGW编译

安装[MinGW](http://www.mingw.org/)和[cmake](https://cmake.org/):

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release `path_to_api-cplusplus` -G "MinGW Makefiles"
mingw32-make -j `nproc`
```

编译成功后，会生成三个可执行文件。

注意：
1. 编译前，需要把`libDolphinDBAPI.dll`复制到编译目录。
2. 执行例子前，需要把`libDolphinDBAPI.dll`和`libgcc_s_seh-1.dll`复制到可执行文件的相同目录下。

### 2. API

#### 2.1 ThreadedClient

每次流数据表发布数据时，单个线程去获取和处理数据。

#### 2.1.1 定义线程客户端

```
ThreadedClient::ThreadClient(int listeningPort);
```

- `listeningPort`是客户端节点的订阅端口号。

#### 2.1.2 调用订阅函数

```
ThreadSP ThreadedClient::subscribe(string host, int port, MessageHandler handler, string tableName, string actionName = DEFAULT_ACTION_NAME, int64_t offset = -1);
```

- `host`是发布端节点的主机名。

- `port`是发布端节点的端口号。

- `handler`是用户自定义的回调函数，用于处理每次流入的数据。流入的数据都有标志：`void(Message)`,`Message`是一行。

- `tableName`是字符串，表示发布端上共享流数据表的名称。

- `actionName`是字符串，表示订阅任务的名称。它可以包含字母、数字和下划线。

- `offset`是整数，表示订阅任务开始后的第一条消息所在的位置。消息是流数据表中的行。如果没有指定offset，或它为负数或超过了流数据表的记录行数，订阅将会从流数据表的当前行开始。offset与流数据表创建时的第一行对应。如果某些行因为内存限制被删除，在决定订阅开始的位置时，这些行仍然考虑在内。

- `resub`是布尔值，表示订阅中断后，是否会自动重订阅。

- `filter`是一个向量，表示过滤条件。流数据表过滤列在filter中的数据才会发布到订阅端，不在filter中的数据不会发布。

该函数会返回指向循环调用handler的线程的指针。

示例：

```
auto t = client.subscribe(host, port, [](Message msg) {
    // user-defined routine
    }, tableName);
t->join();
```

#### 2.1.3 取消订阅

```
void ThreadClient::unsubscribe(string host, int port, string tableName, string actionName = DEFAULT_ACTION_NAME);
```

- `host`是发布端节点的主机名。

- `port`是发布端节点的端口号。

- `tableName`是字符串，表示发布端上共享流数据表的名称。

- `actionName`是字符串，表示订阅任务的名称。它可以包含字母、数字和下划线。

该函数用于停止向发布者订阅数据。

#### 2.2 ThreadPooledClient

每次流数据表发布数据时，多个线程同时去获取和处理数据。

#### 2.2.1 定义多线程客户端

```
ThreadPooledClient::ThreadPooledClient(int listeningPort, int threadCount);
```
- `listeningPort`是客户端节点的订阅端口号。

- `threadCount`是线程池的大小。

#### 2.2.2 调用订阅函数

```
vector\<ThreadSP\> ThreadPooledClient::subscribe(string host, int port, MessageHandler handler, string tableName, string actionName = DEFAULT_ACTION_NAME, int64_t offset = -1);
```

参数同单线程的订阅函数。

返回一个指针向量，每个指针指向循环调用handler的线程。

示例：

```
auto vec = client.subscribe(host, port, [](Message msg) {
    // user-defined routine
    }, tableName);
for(auto& t : vec) {
    t->join();
}
```

#### 2.2.3 取消订阅

```
void ThreadPooledClient::unsubscribe(string host, int port, string tableName, string actionName = DEFAULT_ACTION_NAME);
```
参数用单线程的取消订阅函数。

#### 2.3 PollingClient

订阅数据时，会返回一个消息队列。用户可以通过轮询的方式来获取和处理数据。

#### 2.3.1 定义客户端

```
PollingClient::PollingClient(int listeningPort);
```

- `listeningPort`是客户端节点的订阅端口号。

#### 2.3.2 订阅

```
MessageQueueSP PollingClient::subscribe(string host, int port, string tableName, string actionName = DEFAULT_ACTION_NAME, int64_t offset = -1);
```

参数用单线程的订阅函数。

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

#### 2.3.3 取消订阅

```
void PollingClient::unsubscribe(string host, int port, string tableName, string actionName = DEFAULT_ACTION_NAME);
```

参数同单线程的取消订阅函数。

注意，对于这种订阅模式，取消订阅时，会返回一个空指针，用户需要自行处理。