# DolphinDB C++ API

DolphinDB C++ API支持以下开发环境：

- Linux
- Windows Visual Studio
- Windows GNU(MingW)

本教程主要介绍以下内容：

- 在Linux环境下编译项目
- 在Windows Visual Studio环境下编译项目
- 通过C++ API执行DolphinDB脚本
- 调用DolphinDB内置函数
- 上传本地对象到DolphinDB服务器
- 追加数据到DolphinDB数据表

### 1. 在Linux环境下编译项目

#### 1.1 环境配置

C++ API需要使用g++ 6.2。

#### 1.2 下载bin文件和头文件

从这个GitHub项目中下载bin和include目录到自己的项目。

> bin (libDolphinDBAPI.so)
  include (DolphinDB.h  Exceptions.h  SmartPointer.h  SysIO.h  Types.h  Util.h)

#### 1.3 编译main.cpp

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

#### 1.4 编译

g++编译命令：

> g++ main.cpp -std=c++11 -DLINUX -DLOGGING_LEVEL_2 -O2 -I../include -lDolphinDBAPI -lssl  -lpthread -luuid -L../bin  -Wl,-rpath ../bin/ -o main

#### 1.5 运行

编译成功后，启动DolphinDB，运行main程序并连接到DolphinDB，连接时需要指定IP地址和端口号，如111.222.3.44:8503。

### 2. Windows环境下编译

#### 2.1 环境配置

本教程使用了Visual Studio 2017 64位版本。

#### 2.2 下载bin和头文件

将本GitHub项目下载到本地。

#### 2.3 创建Visual Studio项目

创建win32 console project，导入头文件，创建1.3节中的main.cpp文件，导入libDolphinDBAPI.lib，并且配置lib目录。

> 注意：由于VS里默认定义了min/max两个宏，与头文件中`min`、`max`函数冲突。为了解决这个问题，在预处理宏定义中需要加入 `__NOMINMAX__` 。

#### 2.4 编译和运行

启动编译，将对应的libDolphinDBAPI.dll拷贝到可执行程序的输出目录，即可运行。

Windows gnu开发环境与Linux相似，可以参考上一章的Linux编译。

### 3 通过C++ API执行DolphinDB脚本

#### 3.1 连接DolphinDB

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

#### 3.2 通过C++ API执行DolphinDB脚本

通过`run`方法执行DolphinDB脚本：

```C++
ConstantSP v = conn.run("`IBM`GOOG`YHOO");
int size = v->size();
for(int i = 0; i < size; i++)
    cout<<v->getString(i)<<endl;
```

输出结果为：

> IBM GOOG YHOO

### 3.3 支持多种数据类型和数据形式

DolphinDB支持多种数据类型，包括Int, Float, String, Date, DataTime等，以及多种数据形式，包括向量，集合，矩阵，字典，表等。

#### 3.3.1 向量

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

#### 3.3.2 集合

```C++
VectorSP set = conn.run("set(4 5 5 2 3 11 6)");
cout<<set->getString()<<endl;
```

#### 3.3.3 矩阵

```C++
ConstantSP matrix = conn.run("1..6$2:3");
cout<<matrix->getString()<<endl;
```

#### 3.3.4 字典

```C++
ConstantSP dict = conn.run("dict(1 2 3,`IBM`MSFT`GOOG)");
cout << dict->get(Util::createInt(1))->getString()<<endl;
```

我们可以用`get`方法来获得key对应的值。上例通过`Util::createInt()`创建Int类型的值，并找到字典中key为1的值。

#### 3.3.5 表

```C++
string sb;
sb.append("n=20000\n");
sb.append("syms=`IBM`C`MS`MSFT`JPM`ORCL`BIDU`SOHU`GE`EBAY`GOOG`FORD`GS`PEP`USO`GLD`GDX`EEM`FXI`SLV`SINA`BAC`AAPL`PALL`YHOO`KOH`TSLA`CS`CISO`SUN\n");
sb.append("mytrades=table(09:30:00+rand(18000,n) as timestamp,rand(syms,n) as sym, 10*(1+rand(100,n)) as qty,5.0+rand(100.0,n) as price);\n");
sb.append("select qty,price from mytrades where sym==`IBM;");
ConstantSP table = conn.run(sb);
```

#### 3.3.6 AnyVector

AnyVector是DolphinDB中一种特殊的数据形式，与常规的向量不同，它的每个元素可以是不同的数据类型或数据形式。

```C++
ConstantSP result = conn.run("{1, 2, {1,3,5},{0.9, 0.8}}");
cout<<result->getString()<<endl;
```

使用`get`方法获取第三个元素：

```C++
VectorSP v =  result->get(2);
cout<<v->getString()<<endl;
```

结果是一个Int类型的向量[1,3,5]。

### 4 调用DolphinDB内置函数

C++ API提供了调用DolphinDB内置函数的接口：

```C++
vector<ConstantSP> args;
double array[] = {1.5, 2.5, 7};
ConstantSP vec = Util::createVector(DT_DOUBLE, 3); // build a Double Vector with size of 3.
vec->setDouble(0, 3, array); // assign values
args.push_back(vec);
ConstantSP result = conn.run("sum", args); // call built-in function "sum".
cout<<result->getString()<<endl;
```

上面的例子中，`run`方法调用了DolphinDB函数`sum`，参数是Double类型的向量`Util::createVector(DT_DOUBLE, 3)`。

`run`方法的第一个参数DolphinDB中的函数名，第二个参数是ConstantSP类型的向量。

### 5 上传本地对象到DolphinDB服务器

C++ API提供了灵活的接口来创建本地对象。通过`upload`方法，可以方便地把本地对象上传到DolphinDB。

下面的例子首先创建了一个本地的表对象，然后把它上传到DolphinDB，再从DolphinDB获取这个表。

```C++
// Create a local table object with 3 columns
TableSP createDemoTable(){
    vector<string> colNames = {"name","date","price"};
    vector<DATA_TYPE> colTypes = {DT_STRING,DT_DATE,DT_DOUBLE};
    int colNum = 3,rowNum = 3;
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

// Upload the local table object to DolphinDB server，and then get back the object from the server through method run.
table = createDemoTable();
conn.upload("myTable", table);
string script = "select * from myTable;";
ConstantSP result = conn.run(script);
cout<<result->getString()<<endl;
```

### 6 追加数据到DolphinDB表

通过C++ API，我们可以把数据追加到DolphinDB的表中。DolphinDB支持3种类型表：内存表、本地磁盘表和分布式表。

#### 6.1 内存表

#### 6.1.1 创建内存表

在DolphinDB中执行以下脚本创建内存表：

```
t = table(100:0, `name`date`price, [STRING, DATE, DOUBLE]);
share t as tglobal
```

上面的例子中，我们通过`table`函数来创建表，指定了表的容量和初始大小、列名和数据类型。`share`函数可以把表共享到各个会话，因此多个客户端可以同时访问t。

#### 6.1.2 保存数据到内存表

```C++
string script;
//simulate data
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
conn.upload(allnames,allcols); // upload data to a DolphinDB server
script += "insert into tglobal values(names,dates,prices);"; // insert data to table
script += "select * from tglobal;";
TableSP table = conn.run(script); // return the updated in-memory table
cout<<table->getString()<<endl;
```

把数据追加到内存表，除了可以使用`insert into`语句，还可以使用`append!`函数，它可以把一张表追加到另一张表。

```C++
table = createDemoTable();
script += "t.append!(table);";
```

#### 6.2 本地磁盘表

#### 6.2.1 创建本地磁盘表

在DolphinDB中使用以下脚本创建本地磁盘表：

```
t = table(100:0, `name`date`price, [STRING, DATE, DOUBLE]); // create an in-memory table
db=database("/home/dolphindb/demoDB"); // create database "demoDB"
saveTable(db,t,`dt); // save the in-memory table to the database
share t as tDiskGlobal
```

`database`函数用于创建数据库，`saveTable`函数把内存表保存到磁盘中。

#### 6.2.2 把数据保存到本地磁盘表

```C++
TableSP table = createDemoTable();
conn.upload("mt",table);
string script;
script += "db=database(\"/home/demoTable1\");";
script += "tDiskGlobal.append!(mt);";
script += "saveTable(db,tDiskGlobal,`dt);";
script += "select * from tDiskGlobal;";
TableSP result = conn.run(script); // load an in-memory table from the database
cout<<result->getString()<<endl;
```

`loadTable`函数把数据库中的表加载到内存；`append!`函数把数据追加到表中。

注意：

1. 对于本地磁盘表，`append!`函数只把数据追加到内存，如果要保存到磁盘上，必须再次执行`saveTable`函数。
2. 除了使用`share`让表在其他会话中可见，也可以在C++ API中使用`loadTable`来加载表，使用`append!`来追加数据。但是，我们不推荐这种方法，因为`loadTable`函数从磁盘加载数据，会消耗大量时间。如果有多个客户端都使用`loadTable`，内存中会有多个表的副本，造成数据不一致。

#### 6.3 分布式表

分布式表保存在集群的多个节点上。下面的例子通过C++ API把数据保存至分布式表。

#### 6.3.1 创建分布式表

在DolphinDB中使用以下脚本创建分布式表：

```
login(`admin,`123456)
dbPath = "dfs://SAMPLE_TRDDB";
tableName = `demoTable
db = database(dbPath, VALUE, 2010.01.01..2010.01.30)
pt=db.createPartitionedTable(table(1000000:0,`name`date`price,[STRING,DATE,DOUBLE]),tableName,`date)
```

`database`函数创建数据库，对于分布式数据库，路径必须以“dfs”开头。`createPartitionedTable`函数用于创建分区表。

#### 6.3.2 把数据保存到分布式表

```C++
string script;
TableSP table = createDemoTable();
conn.upload("mt",table);
script += "login(`admin,`123456);";
script += "dbPath = \"dfs://SAMPLE_TRDDB\";";
script += "tableName = `demoTable;";
script += "database(dbPath).loadTable(tableName).append!(mt);";
script += "select * from database(dbPath).loadTable(tableName);";
TableSP result = conn.run(script);
cout<<result->getString()<<endl;
```

`append!`函数把数据追加到分布式表。

关于C++ API的更多信息，可以参考C++ API头文件。

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

