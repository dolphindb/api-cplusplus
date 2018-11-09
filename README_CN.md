# DolphinDB C++ API
DolphinDB C++ API提供了以下三种开发环境:
* Linux
* Windows Visual Stdio
* Windows gnu

本教程主要包括以下内容：
* Linux 开发环境工程编译  
* Windows Visual Studio 开发环境工程编译
* 执行DolphinDB Script  
* 调用内置函数  
* 上传本地对象到Server  
* 数据表添加数据  

### 1 Linux 开发环境工程编译
#### 1.1 环境需求
* linux 编程环境；  
* g++ 6.2编译器；（由于libDolphinDBAPI.so是由g++6.2编译的，为了保证ABI兼容，建议使用该版本的编译器）

#### 1.2 下载bin文件和头文件
下载api-cplusplus，包括bin和include文件夹，如下：

> bin (libDolphinDBAPI.so)  
  include (DolphinDB.h  Exceptions.h  SmartPointer.h  SysIO.h  Types.h  Util.h) 
  
#### 1.3 编写main.cpp文件
在与bin和include平级目录创建目录project，进入project并创建文件main.cpp，内容如下：
```
#include "DolphinDB.h"
#include "Util.h"
#include <iostream>
#include <string>
using namespace dolphindb;
using namespace std;

int main(int argc, char *argv[]){
    DBConnection conn;
    bool ret = conn.connect("192.168.1.25", 8503);
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
g++ 编译命令如下：
> g++ main.cpp -std=c++11 -DLINUX -DLOGGING_LEVEL_2 -O2 -I../include -lDolphinDBAPI -lssl  -lpthread -luuid -L../bin  -Wl,-rpath ../bin/ -o main

#### 1.5 运行
运行main之前，需要启动DolphinDB server，本例中连接到IP为192.168.1.25，端口为8503的DolphinDB Server。然后运行main程序，成功连接到DolphinDB Server。

### 2 Windows Visual Stdio 开发环境工程编译
#### 2.1 环境需求
本教程用例在Visual Stdio 2017 下测试通过，建议用户使用该版本。
> 注意：只支持 __x64__ 位，因此需要将工程配置为x64。

#### 2.2 下载库文件和头文件
下载api-cplusplus。

#### 2.3 创建VS工程
创建win32控制台程序，并导入头文件；
编写cpp文件（如上）；
导入libDolphinDBAPI.lib，并配置附加库目录为该lib的目录；
> 注意：由于VS里默认定义了min/max两个宏，与头文件中`min`、`max`函数冲突。为了解决这个问题，在预处理宏定义中需要加入 `__NOMINMAX__` ；

#### 2.4 编译执行
启动编译，把libDolphinDBAPI.dll拷贝到可执行程序输出目录，执行编译出的可执行程序。

Windows gnu开发环境与Linux类似，可参考Linux下工程编译。

### 3 执行DolphinDB scriptf
#### 3.1 创建连接
C++ API通过TCP/IP连接DolphinDB server，`connect`方法通过`ip`和`port`两个参数来连接，代码如下：
```
DBConnection conn;
bool ret = conn.connect("192.168.1.25", 8503);
```

连接服务器时，还可以同时指定用户名和密码进行登录，代码如下：
```
DBConnection conn;
bool ret = conn.connect("192.168.1.25", 8503,"admin","123456");
```

#### 3.2 执行脚本
通过 run 方法来执行脚本：
```
ConstantSP v = conn.run("`IBM`GOOG`YHOO");
int size = v->size();
for(int i = 0; i < size; i++)
    cout<<v->getString(i)<<endl;
```

输出如下：
>IBM  
GOOG  
YHOO  

如果脚本只包含一个语句，如上代码，则返回该语句的返回值；如果脚本包含多个语句，则只返回最后一个语句的返回值；如果脚本中有语法错误或者遇到网络问题，则抛出异常。

#### 3.3 支持的多种数据样式
DolphinDB支持多种数据类型（Int、Long、String、Date、DataTime等）和多种数据样式（Vector、Set、Matrix、Dictionary、Table、AnyVector等），下面举例介绍。

##### 3.3.1 Vector
Vector类似C++中的vector，可以支持各种不同的数据类型，Int类型Vector如下：
```
VectorSP v = conn.run("1..10");
int size = v->size();
for(int i = 0; i < size; i++)
    cout<<v->getInt(i)<<endl;
```
`run`方法返回Int类型的Vector，输出为1到10；

输出DateTime类型的Vector：
```
VectorSP v = conn.run("2010.10.01..2010.10.30");
int size = v->size();
for(int i = 0; i < size; i++)
    cout<<v->getString(i)<<endl;
```
`run`方法返回DataTime类型的Vector。

##### 3.3.2 Set
```
VectorSP set = conn.run("set(4 5 5 2 3 11 6)");
cout<<set->getString()<<endl;
```
`run`方法返回Int类型的Set，输出：set(5,2,3,4,11,6)

##### 3.3.3 Matrix
```
ConstantSP matrix = conn.run("1..6$2:3");
cout<<matrix->getString()<<endl;
```
`run`方法返回Int类型的Matrix。

##### 3.3.4 Dictionary
```
ConstantSP dict = conn.run("dict(1 2 3,`IBM`MSFT`GOOG)");
cout << dict->get(Util::createInt(1))->getString()<<endl;
```
`run`方法返回Dictionary，输出：IBM。
另外，Dictionary的`get`方法，接受一个词典的key类型的参数（本例中，keys为 1 2 3，为Int类型），通过`Util::createInt()`函数来创建一个Int类型的对象。

##### 3.3.5 Table
```
string sb;
sb.append("n=20000\n");
sb.append("syms=`IBM`C`MS`MSFT`JPM`ORCL`BIDU`SOHU`GE`EBAY`GOOG`FORD`GS`PEP`USO`GLD`GDX`EEM`FXI`SLV`SINA`BAC`AAPL`PALL`YHOO`KOH`TSLA`CS`CISO`SUN\n");
sb.append("mytrades=table(09:30:00+rand(18000,n) as timestamp,rand(syms,n) as sym, 10*(1+rand(100,n)) as qty,5.0+rand(100.0,n) as price);\n");
sb.append("select qty,price from mytrades where sym==`IBM;");
ConstantSP table = conn.run(sb);
```
`run`方法返回table，包含两列 qty 和 price。

##### 3.3.6 AnyVector
AnyVector中可以包含不同的数据类型，如下：
```
ConstantSP result = conn.run("{1, 2, {1,3,5},{0.9, 0.8}}");
cout<<result->getString()<<endl;
```
`run`方法返回AnyVector，可以通过result->get(2) 获取第2个元素{1,3,5}，如下：
```
VectorSP v =  result->get(2);
cout<<v->getString()<<endl;
```

输出Int类型Vector：[1,3,5]。

### 4 调用DolphinDB内置函数
DolphinDB C++ API提供了在C++层面调用DolphinDB内置函数的接口，代码如下：
```
vector<ConstantSP> args;
double array[] = {1.5, 2.5, 7};
ConstantSP vec = Util::createVector(DT_DOUBLE, 3);//创建Double类型的Vector，包含3个元素
vec->setDouble(0, 3, array);//给vec赋值
args.push_back(vec);
ConstantSP result = conn.run("sum", args);//调用DolphinDB内置函数sum
cout<<result->getString()<<endl;
 ```
 
`run`方法返回`sum`函数的结果，`sum`函数接受一个Double类型的Vector，通过Util::createVector(DT_DOUBLE, 3)来创建Double Vector。

`run`方法的第一个参数为string类型的函数名，第二个参数为ConstantSP类型的vector（Constant类为DolphinDB中所有类型的基类），`sum`输出为Double类型。

### 5 上传本地对象到DolphinDB server
通过C++ API可以把本地的对象上传到DolphinDB server中，下面用例先在本地创建table对象，然后上传到server，再从server中获取该对象，完整代码如下：
```
//本地创建table对象，包含3列
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
   
//将table对象上传到Server，并再从Server获取该对象
table = createDemoTable();
conn.upload("myTable", table);
string script = "select * from myTable;";
ConstantSP result = conn.run(script);
cout<<result->getString()<<endl;
```
C++ API提供了在本地灵活的创建各种对象的接口，利用 `__upload__` 方法，可以方便的实现本地对象和server对象的转换交互。

### 6 数据表添加数据
利用C++ API可以方便的将第三方系统业务数据添加到DolphinDB数据表中。DolphinDB支持三种类型的表: 内存表、本地磁盘表及分布式表。下面介绍如何利用C++ API将模拟的业务数据保存到不同类型的表中。  

#### 6.1 内存表
数据仅保存在本节点内存，存取速度最快，但是节点关闭后数据就不存在了，适用于临时快速计算、临时保存计算结果等场景。由于全部保存在内存中，内存表不应太大。

##### 6.1.1 通过DolphinDB客户端创建内存表
```
t = table(100:0, `name`date`price, [STRING, DATE, DOUBLE]);
share  t as tglobal
```
`__table__` 创建内存表，指定capacity，size，列名以及类型；  
`__share__` 将表设置为跨session可见；  

##### 6.1.2 通过C++ API保存数据到内存表
```
string script;
//模拟生成需要保存到内存表的数据
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
conn.upload(allnames,allcols);//将数据上传到server
script += "insert into tglobal values(names,dates,prices);"; //通过insert into 方法将数据保存到内存表中
script += "select * from tglobal;";
TableSP table = conn.run(script); 
cout<<table->getString()<<endl;
```
`run`方法返回的table为内存表。  
内存表中添加数据除了使用 `__insert into__` 语法外，还可以使用 `__append!__`，其接受一个table作为参数，C++ API创建table的实例参考（5、上传本地对象到DolphinDB Server)，如下：
```
table = createDemoTable();
script += "t.append!(table);";
```
注意:  
>本例中使用 `__share__` 来把内存表设置为跨session可见，这样多个客户端可以同时对表t进行写入访问，`share`详细介绍请参考manual。

#### 6.2 本地磁盘表
数据保存在本地磁盘上，即使节点关闭，再启动后，可以方便的将数据加载到内存。适用于数据量不是特别大，并且需要持久化到本地磁盘的数据。下面例子介绍，本地磁盘表tglobal已经创建，如何通过C++ API保存数据。

##### 6.2.1 通过DolphinDB客户端创建本地磁盘表
可以通过DolphinDB的任何客户端（GUI、Web notebook、console）创建本地磁盘表，代码如下
```
t = table(100:0, `name`date`price, [STRING, DATE, DOUBLE]); //创建内存表
db=database("/home/psui/demoTable"); //创建本地数据库
saveTable(db,t,`dt); //保存本地表
share t as tDiskGlobal
```
`__database__` 接受一个本地路径，创建一个本地数据库
`__saveTable__` 将内存内存表保存到本地数据库中，并存盘； 

##### 6.2.2 通过C++ API保存数据到本地磁盘表
```
TableSP table = createDemoTable();
conn.upload("mt",table);
string script;
script += "db=database(\"/home/psui/demoTable1\");";
script += "tDiskGlobal.append!(mt);";
script += "saveTable(db,tDiskGlobal,`dt);";
script += "select * from tDiskGlobal;";
TableSP result = conn.run(script); 
cout<<result->getString()<<endl;
```
`__loadTable__` 从本地数据库中加载一个table到内存。   
`__append!__` 保存数据到内存表。
最后，`run`方法返回从磁盘载入内存的table。  
注意:  
>1、对于本地磁盘表， `__append!__` 仅仅将数据添加到内存表，要将数据保存到磁盘，还必须使用 `__saveTable__` 函数。  
2、除了使用`share`函数外，还可以在C++ API中使用`loadTable`函数加载表内容，然后使用`append！`。不过这种方法不推荐使用，原因是，`loadTable`需要读磁盘，耗时很大；如果多个客户端使用`loadTable`的话，内存中会存在表的多个拷贝，容易造成数据不一致；

#### 6.3 分布式表
利用DolphinDB底层提供的分布式文件系统DFS，将数据保存在不同的节点上，逻辑上仍然可以像本地表一样做统一查询。适用于保存企业级历史数据，作为数据仓库使用，提供查询、分析等功能。本例介绍如何通过C++ API保存数据到分布式表中。

##### 6.3.1 创建分布式表
可以通过DolphinDB的任何客户端（GUI、Web notebook、console）创建分布式表，代码如下：
```
login(`admin,`123456)
dbPath = "dfs://SAMPLE_TRDDB";
tableName = `demoTable
db = database(dbPath, VALUE, 2010.01.01..2010.01.30)
pt=db.createPartitionedTable(table(1000000:0,`name`date`price,[STRING,DATE,DOUBLE]),tableName,`date)
```
`__database__` 创建分区数据库，并指定分区类型；  
`__createPartitionedTable__` 创建分布式表，并指定表类型和分区字段；

##### 6.3.2 保存数据到分布式表

```
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
`__append!__` 保存数据到分布式表中，并且保存到磁盘；  

关于C++ API的更多内容请参考头文件中提供的接口。
