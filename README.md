# DolphinDB C++ API
This tutorial introduces how to use DolphinDB C++ API for application development in the Linux environment. It includes the following contents:
* Compiling a project
* Executing DolphinDB Script  
* Calling built in function
* Uploading local objects to DolphinDB server
* Appending data to a DolphindB table

### 1、Environment setup
* linux environment；  
* g++ 6.2；（Since libDolphinDBAPI.so is compiled by g++6.2, it is recommended to use this version of the compiler to ensure ABI compatibility.）

### 2、Compiling a project
#### 2.1 Download bin file and header files
Download api-cplusplus，including "bin" and "include" folders as the following：

> bin (libDolphinDBAPI.so)  
  include (DolphinDB.h  Exceptions.h  SmartPointer.h  SysIO.h  Types.h  Util.h) 
  
#### 2.2 编写main.cpp文件
Create project 

Create a directory called "project" at the same level as the bin and include folders, enter the project folder, and then create the file main.cpp, as follows:
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

#### 2.3 Compiling
g++ compiling command:

> g++ main.cpp -std=c++11 -DLINUX -DLOGGING_LEVEL_2 -O2 -I../include -lDolphinDBAPI -lssl  -lpthread -luuid -L../bin  -Wl,-rpath ../bin/ -o main

#### 2.4 Run

After successfully compiled the program main,  you need to start DolphinDB Server, then you can run the program "main", which connects to DolphinDB Server with IP as 192.168.1.25 and port number as 8503. 

### 3、Executing DolphinDB Script
#### 3.1 Connecting

The C++ API connects to DolphinDB Server via TCP/IP. The connect method connects through two parameters, ip and port. The code is as follows:
```
DBConnection conn;
bool ret = conn.connect("192.168.1.25", 8503);
```

When connecting to the server, you can also log in with the username and password. The default administrator username and password are "admin" and "123456" respectively. The code is as follows:
```
DBConnection conn;
bool ret = conn.connect("192.168.1.25", 8503,"admin","123456");
```

#### 3.2 执行脚本

By calling method "run", you can execute a dolphindb script(maximum 65535 bytes).  

```
ConstantSP v = conn.run("`IBM`GOOG`YHOO");
int size = v->size();
for(int i = 0; i < size; i++)
    cout<<v->getString(i)<<endl;
```

Output:
>IBM  
GOOG  
YHOO  

If the script contains only one statement, the return value of the statement is returned; if the script contains multiple statements, only the return value of the last statement is returned; if there is a syntax error in the script or a network problem is thrown abnormal.

#### 3.3 Support multiple data types and forms
DolphinDB supports multiple data types（Int、Long、String、Date、DataTime, etc.）and multiple data forms（Vector、Set、Matrix、Dictionary、Table、AnyVector, etc.)

##### 3.3.1 Vector
Vector is similar to the vector in C++. It supports different data data types. 

Return an int vector
```
VectorSP v = conn.run("1..10");
int size = v->size();
for(int i = 0; i < size; i++)
    cout<<v->getInt(i)<<endl;
```

Return a date vector
```
VectorSP v = conn.run("2010.10.01..2010.10.30");
int size = v->size();
for(int i = 0; i < size; i++)
    cout<<v->getString(i)<<endl;
```

##### 3.3.2 Set

Return an int set
```
VectorSP set = conn.run("set(4 5 5 2 3 11 6)");
cout<<set->getString()<<endl;
```

##### 3.3.3 Matrix

Return an int matrix

```
ConstantSP matrix = conn.run("1..6$2:3");
cout<<matrix->getString()<<endl;
```

##### 3.3.4 Dictionary

Return a dictionary wtih Int objects as keys and string objects as values. 
```
ConstantSP dict = conn.run("dict(1 2 3,`IBM`MSFT`GOOG)");
cout << dict->get(Util::createInt(1))->getString()<<endl;
```

In addition, you can use get method to retrieve a value. Note that you need to create an Int value through function Util::createInt() in order to do so. 


##### 3.3.5 Table

Return a table including two columns: qty and price.
```
string sb;
sb.append("n=20000\n");
sb.append("syms=`IBM`C`MS`MSFT`JPM`ORCL`BIDU`SOHU`GE`EBAY`GOOG`FORD`GS`PEP`USO`GLD`GDX`EEM`FXI`SLV`SINA`BAC`AAPL`PALL`YHOO`KOH`TSLA`CS`CISO`SUN\n");
sb.append("mytrades=table(09:30:00+rand(18000,n) as timestamp,rand(syms,n) as sym, 10*(1+rand(100,n)) as qty,5.0+rand(100.0,n) as price);\n");
sb.append("select qty,price from mytrades where sym==`IBM;");
ConstantSP table = conn.run(sb);
```


##### 3.3.6 AnyVector
Return an AnyVector 
```
ConstantSP result = conn.run("{1, 2, {1,3,5},{0.9, 0.8}}");
cout<<result->getString()<<endl;
```


Get the second element({1,3,5}) through method get.
```
VectorSP v =  result->get(2);
cout<<v->getString()<<endl;
```

Output an int Vector：[1,3,5]。

### 4、Calling DolphinDB built-in functions

The DolphinDB C++ API provides an interface to the DolphinDB built-in functions as follows:
```
vector<ConstantSP> args;
double array[] = {1.5, 2.5, 7};
ConstantSP vec = Util::createVector(DT_DOUBLE, 3);// build a Double Vector with size as 3.
vec->setDouble(0, 3, array);// assign values
args.push_back(vec);
ConstantSP result = conn.run("sum", args);//call built-in function "sum".
cout<<result->getString()<<endl;
 ```
 
The run method above returns the result of the sum function, which accepts a Vector of type Double and creates a Double Vector via Util::createVector(DT_DOUBLE, 3).

The first parameter of the run method is the function name of string type, the second parameter is the vector of ConstantSP type (the Constant class is the base class of all types in DolphinDB), and the output of sum is Double type.

### 5、Uploading local objects to DolphinDB Server
The C++ API provides a flexible interface for creating various objects locally. With the __upload__ method, you can easily implement the conversion between local objects and Server objects.

The local object can be uploaded to the DolphinDB Server through the C++ API. The following example first creates the table object locally, then uploads it to the server, and then gets the object from the server. The complete code is as follows:
```
//Create a local table object with 3 columns.
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
   
//Upload the local table object to DolphinDB server，and then get back the object from the server through method run.
table = createDemoTable();
conn.upload("myTable", table);
string script = "select * from myTable;";
ConstantSP result = conn.run(script);
cout<<result->getString()<<endl;
```


### 6、Appending data to DolphinDB table

Data can be easily added to the DolphinDB data table using the C++ API. DolphinDB supports three types of tables, __memory table__, __local disk table__ and __distributed table__. Here's how to use the C++ API to save simulated business data to different types of tables.

#### 6.1 Memory table

The data is only stored in the memory of a data node, and the access speed is the fastest, but data will not exist after the session is closed. It is suitable for scenarios such as temporary fast calculation and temporary storage of calculation results. Since all data is stored in memory, the memory table should not be too large.

##### 6.1.1 Build a memory table
```
t = table(100:0, `name`date`price, [STRING, DATE, DOUBLE]);
share  t as tglobal
```
__table__ : create a memory table, specifying capacity, size, column name, and type;
__share__ : set the table to be visible across sessions;

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
run方法返回的table为内存表。  
内存表中添加数据除了使用 __insert into__ 语法外，还可以使用 __append!__ ，其接受一个table作为参数，C++ API创建table的实例参考（5、上传本地对象到DolphinDB Server)，如下：
```
table = createDemoTable();
script += "t.append!(table);";
```
注意:  
>本例中使用 __share__ 来把内存表设置为跨session可见，这样多个客户端可以同时对t进行写入访问，share详细介绍请参考manual。


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
__database__ 接受一个本地路径，创建一个本地数据库；  
__saveTable__ 将内存内存表保存到本地数据库中，并存盘； 

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
__loadTable__ 从本地数据库中加载一个table到内存；   
__append!__ 保存数据到内存表；  
最后，run方法返回从磁盘载入内存的table。  
注意:  
>1、对于本地磁盘表， __append!__ 仅仅将数据添加到内存表，要将数据保存到磁盘，还必须使用 __saveTable__ 函数。  
2、除了使用share函数外，还可以在C++ API中使用loadTable函数加载表内容，然后append！。不过这种方法不推荐使用，原因是，loadTable需要读磁盘，耗时很大；如果多个客户端loadTable的话，内存中会存在表的多个拷贝，容易造成数据不一致；

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
__database__ 创建分区数据库，并指定分区类型；  
__createPartitionedTable__ 创建分布式表，并指定表类型和分区字段；

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
__append!__ 保存数据到分布式表中，并且保存到磁盘；  

关于C++ API的更多内容请参考头文件中提供的接口。

