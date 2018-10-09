# DolphinDB C++ API
本教程介绍了在linux环境下，如何使用DolphinDB提供的C++ API进行应用开发。主要包括以下内容：
* 工程编译  
* 执行DolphinDB Script  
* 调用内置函数  
* 上传本地对象到Server  
* 数据导入  

### 1、环境需求
* linux 编程环境；  
* g++ 6.2编译器；（由于libDolphinDBAPI.so是由g++6.2编译的，为了保证ABI兼容，建议使用该版本的编译器）

### 2、编译工程
#### 2.1 下载bin文件和头文件
下载api-cplusplus，包括bin和include文件夹，如下：

> bin (libDolphinDBAPI.so)  
  include (DolphinDB.h  Exceptions.h  SmartPointer.h  SysIO.h  Types.h  Util.h) 
  
#### 2.2 编写main.cpp文件
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
    ConstantSP vector = conn.run("\`IBM\`GOOG\`YHOO");
    int size = vector->rows();
    for(int i=0; i<size; ++i)
        cout<<vector->getString(i)<<endl;
    return 0;
}
```

#### 2.3 编译
g++ 编译命令如下：
> g++ main.cpp -std=c++11 -DLINUX -DLOGGING_LEVEL_2 -O2 -I../include -lDolphinDBAPI -lssl  -lpthread -luuid -L../bin  -Wl,-rpath ../bin/ -o main

#### 2.4 运行
运行main之前，需要启动DolphinDB Server，本例中连接到IP为192.168.1.25，端口为8503的DolphinDB Server。然后运行main程序，成功连接到DolphinDB Server。

### 3、执行DolphinDB Script
#### 3.1 创建连接
C++ API通过TCP/IP连接DolphinDB Server，connect方法通过ip和port两个参数来连接，代码如下：
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
通过 run 方法来执行脚本，脚本的最大长度是65535bytes，如下：
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

run方法返回Int类型的Vector，输出为1到10；
下面输出DateTime类型的Vector，如下：
```
VectorSP v = conn.run("2010.10.01..2010.10.30");
int size = v->size();
for(int i = 0; i < size; i++)
    cout<<v->getString(i)<<endl;
```

run方法返回DataTime类型的Vector。
##### 3.3.2 Set
```
VectorSP set = conn.run("set(4 5 5 2 3 11 6)");
cout<<set->getString()<<endl;
```

run方法返回Int类型的Set，输出：set(5,2,3,4,11,6)
##### 3.3.3 Matrix
```
ConstantSP matrix = conn.run("1..6$2:3");
cout<<matrix->getString()<<endl;
```

run方法返回Int类型的Matrix。

##### 3.3.4 Dictionary
```
ConstantSP dict = conn.run("dict(1 2 3,`IBM`MSFT`GOOG)");
cout << dict->get(Util::createInt(1))->getString()<<endl;
```

run方法返回Dictionary，输出：IBM；
另外，Dictionary的get方法，接受一个词典的key类型的参数（本例中，keys为 1 2 3，为Int类型），通过Util::createInt()函数来创建一个Int类型的对象。
##### 3.3.5 Table
```
string sb;
sb.append("n=20000\n");
sb.append("syms=`IBM`C`MS`MSFT`JPM`ORCL`BIDU`SOHU`GE`EBAY`GOOG`FORD`GS`PEP`USO`GLD`GDX`EEM`FXI`SLV`SINA`BAC`AAPL`PALL`YHOO`KOH`TSLA`CS`CISO`SUN\n");
sb.append("mytrades=table(09:30:00+rand(18000,n) as timestamp,rand(syms,n) as sym, 10*(1+rand(100,n)) as qty,5.0+rand(100.0,n) as price);\n");
sb.append("select qty,price from mytrades where sym==`IBM;");
ConstantSP table = conn.run(sb);
```

run方法返回table，包含两列 qty 和 price。
##### 3.3.6 AnyVector
AnyVector中可以包含不同的数据类型，如下：
```
ConstantSP result = conn.run("{1, 2, {1,3,5},{0.9, 0.8}}");
cout<<result->getString()<<endl;
```

run方法返回AnyVector，可以通过result->get(2) 获取第2个元素{1,3,5}，如下：
```
VectorSP v =  result->get(2);
cout<<v->getString()<<endl;
```

输出Int类型Vector：[1,3,5]。

### 4、调用DolphinDB内置函数
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
 
run方法返回sum函数的结果，sum函数接受一个Double类型的Vector，通过Util::createVector(DT_DOUBLE, 3)来创建Double Vector；
run方法的第一个参数为string类型的函数名，第二个参数为ConstantSP类型的vector（Constant类为DolphinDB中所有类型的基类），sum输出为Double类型。

### 5、上传本地对象到DolphinDB Server
通过C++ API可以把本地的对象上传到DolphinDB Server中，下面用例先在本地创建table对象，然后上传到Server，再从Server中获取该对象，完整代码如下：
```
//本地创建table对象，包含3列
vector<string> colNames = {"name","price","qty"};
vector<DATA_TYPE> colTypes = {DT_STRING,DT_DOUBLE,DT_INT};
int colNum = 3,rowNum = 10;
    ConstantSP table = Util::createTable(colNames,colTypes,rowNum,100);
vector<VectorSP> columnVecs;
for(int i = 0 ;i < colNum ;i ++)
    columnVecs.push_back(table->getColumn(i));
    
for(unsigned int i =  0 ;i < rowNum; i++){
    columnVecs[0]->setString(i,"name"+std::to_string(i));
    columnVecs[1]->setDouble(i,i*10.0);
    columnVecs[2]->setInt(i,i*100);
}
   
//将table对象上传到Server，并再从Server获取该对象
conn.upload("myTalbe", table);
string script = "myTalbe";
ConstantSP result = conn.run(script);
cout<<result->getString()<<endl;
```
C++ API提供了在本地灵活的创建各种对象的接口，利用upload方法，可以方便的实现本地对象和Server对象的转换交互。

### 6、数据导入
利用C++ API可以方便的进行数据导入，DolphinDB用表来存储数据。有三种类型的表，内存表、本地磁盘表及分布式表。下面介绍如何利用Ｃ++ API 将一个csv文件导入DolphinDB中，并保存到不同类型的表中。  
candle_201801.csv 保存的是2018年２月份股市交易部分数据。

#### 6.1 内存表
数据仅保存在本节点内存，存取速度最快，但是节点关闭数据就不存在了，适用于临时快速计算、临时保存计算结果等场景。由于全部保存在内存中，内存表不应太大。本例将csv文件的数据保存到内存表中，代码如下：
```
string script;
script += "t=loadText(\"/home/psui/C++API/api-cplusplus/test/candle_1.csv\");";
script += "select * from t;";
TableSP table = conn.run(script); 
cout<<table->getString()<<endl;
```

run方法返回的table为内存表。  
另外，除了使用loadText，还可以用ploadText、loadTextEx来导入csv文件，并且在导入的时候，可以指定各个字段的类型。具体参考[数据导入教程](https://github.com/dolphindb/Tutorials_CN/edit/master/import_data.md)

#### 6.2 本地磁盘表
数据保存在本地磁盘上，即使节点关闭，再启动后，可以方便的将数据加载到内存。适用于数据量不是特别大，并且需要持久化到本地磁盘的数据。本例将csv文件保存到本地磁盘表，并从本地磁盘表再加载到内存。代码如下：
```
string script;
script += "t=loadText(\"/home/psui/C++API/api-cplusplus/test/candle_1.csv\");";
script += "db=database(\"/home/psui/localDiskTable\");";
script += "saveTable(db,t,`t1);";
script += "tdisk=loadTable(db,`t1);";
script += "select * from tdisk;";
TableSP table = conn.run(script); 
cout<<table->getString()<<endl;
```
database 方法接受一个本地路径，创建一个本地数据库；  
saveTable 方法将内存内存表保存到本地数据库中，并存盘；  
loadTable 方法从本地数据库中加载一个table到内存；  
最后，run方法返回从磁盘载入内存的table。  

#### 6.3 分布式表
利用DolphinDB底层提供的分布式文件系统DFS，将数据保存在不同的节点上，逻辑上仍然可以像本地表一样做统一查询。适用于保存企业级历史数据，作为数据仓库使用，提供查询、分析等功能。
```
string script;
script += "login(`admin,`123456);";
script += "dbPath = \"dfs://SAMPLE_TRDDB\";";
script += "tableName = `tradingDay;";
script += "allDays = 2018.01.01..2018.01.30;";
script += "db = database(dbPath, VALUE, allDays);"; //创建分布式数据库
script += "pt=db.createPartitionedTable(table(1000000:0, `symbol`exchange`cycle`tradingDay`date`time`open`high`low`close`volume`turnover`unixTime, [SYMBOL,SYMBOL,INT,DATE,DATE,TIME,DOUBLE,DOUBLE,DOUBLE,DOUBLE,LONG,DOUBLE,LONG]), tableName, `tradingDay);"; //创建分布式表
script += "t=loadText(\"/home/psui/C++API/api-cplusplus/test/candle_1.csv\");"; //加载csv文件
script += "database(dbPath).loadTable(tableName).append!(select symbol, exchange, cycle, tradingDay,date, datetimeParse(format(time,\"000000000\"),\"HHmmssSSS\"), open, high, low, close, volume, turnover,unixTime from t );";//将内存数据表保存到分布式表中
script += "tradTable= database(dbPath).loadTable(tableName);";
script += "select count(*) from tradTable;";
TableSP table = conn.run(script); 
cout<<table->getString()<<endl;
```
database 方法创建分布式数据库；  
createPartitionedTable 方法创建分布式表；  
关于DolphinDB数据导入的更多信息信息，请参考[数据导入教程](https://github.com/dolphindb/Tutorials_CN/edit/master/import_data.md)
