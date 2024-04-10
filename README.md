This tutorial includes the following topics about how to use DolphinDB C++ API in Linux:

[DolphinDB C++ API](https://2xdb.net/dolphindb/api-cplusplus#dolphindb-c-api)
- [1. Compilation](https://2xdb.net/dolphindb/api-cplusplus/-/blob/master/README.md#1-compilation)
  - [1.1 Compile under Linux](https://2xdb.net/dolphindb/api-cplusplus#11-compile-under-linux)
    - [1.1.1 Environment Setup](https://2xdb.net/dolphindb/api-cplusplus#111-environment-setup)
    - [1.1.2 Download bin file and header files](https://2xdb.net/dolphindb/api-cplusplus#112-download-bin-file-and-header-files)
    - [1.1.3 Compile main.cpp](https://2xdb.net/dolphindb/api-cplusplus#113-compile-maincpp)
    - [1.1.4 Compile](https://2xdb.net/dolphindb/api-cplusplus#114-compile)
    - [1.1.5 Run](https://2xdb.net/dolphindb/api-cplusplus#115-run)
  - [1.2 Compile under Linux](https://2xdb.net/dolphindb/api-cplusplus#12-compile-under-windows)
    - [1.2.1 Environment Setup](https://2xdb.net/dolphindb/api-cplusplus#121-environment-setup)
    - [1.2.2 Download bin file and header files](https://2xdb.net/dolphindb/api-cplusplus#122-download-bin-file-and-header-files)
    - [1.2.3 Environment Setup](https://2xdb.net/dolphindb/api-cplusplus#123-build-visual-studio-project)
    - [1.2.4 Environment Setup](https://2xdb.net/dolphindb/api-cplusplus#124-compile-and-run)
- [2. Establish DolphinDB connection](https://2xdb.net/dolphindb/api-cplusplus#2-establish-dolphindb-connection)
- [3. Execute DolphinDB script](https://2xdb.net/dolphindb/api-cplusplus#3-execute-dolphindb-script)
- [4. Call DolphinDB functions](https://2xdb.net/dolphindb/api-cplusplus#4-call-dolphindb-functions)
- [5. Upload local objects to DolphinDB Server](https://2xdb.net/dolphindb/api-cplusplus#5-upload-local-objects-to-dolphindb-server)
- [6. Read data](https://2xdb.net/dolphindb/api-cplusplus#6-read-data)
- [7. Write to DolphinDB tables](https://2xdb.net/dolphindb/api-cplusplus#7-write-to-dolphindb-tables)
  - [7.1 Save data to a DolphinDB in-memory table](https://2xdb.net/dolphindb/api-cplusplus#71-save-data-to-a-dolphindb-in-memory-table)
    - [7.1.1 Save data to an in-memory table with insert into](https://2xdb.net/dolphindb/api-cplusplus#711-save-data-to-an-in-memory-table-with-insert-into)
    - [7.1.2 Save data in batches with tableInsert](https://2xdb.net/dolphindb/api-cplusplus#712-save-data-in-batches-with-tableinsert)
    - [7.1.3 Use function tableInsert to save TableSP objects](https://2xdb.net/dolphindb/api-cplusplus#713-use-function-tableinsert-to-save-tablesp-objects)
  - [7.2 Save data to a distributed table](https://2xdb.net/dolphindb/api-cplusplus#72-save-data-to-a-distributed-table)
  - [7.3 Save data to a local disk table](https://2xdb.net/dolphindb/api-cplusplus#73-save-data-to-a-local-disk-table)
[C++ Streaming API](https://2xdb.net/dolphindb/api-cplusplus#c-streaming-api)
- [8. Build](https://2xdb.net/dolphindb/api-cplusplus#8-build)
  -  [8.1 Linux64](https://2xdb.net/dolphindb/api-cplusplus#81-linux64)
    - [8.1.1 Build with cmake](https://2xdb.net/dolphindb/api-cplusplus#811-build-with-cmake)
  -  [8.2 Build under Windows with MinGW](https://2xdb.net/dolphindb/api-cplusplus#82-build-under-windows-with-mingw)
- [9. Streaming](https://2xdb.net/dolphindb/api-cplusplus#9-streaming)
  - [9.1 ThreadedClient](https://2xdb.net/dolphindb/api-cplusplus#91-threadedclient)
    - [9.1.1 Define the threaded client](https://2xdb.net/dolphindb/api-cplusplus#911-define-the-threaded-client)
    - [9.1.2  Subscribe](https://2xdb.net/dolphindb/api-cplusplus#912-subscribe)
    - [9.1.3 Unsubscribe](https://2xdb.net/dolphindb/api-cplusplus#913-unsubscribe)
  - [9.2 ThreadPooledClient](https://2xdb.net/dolphindb/api-cplusplus#92-threadpooledclient)
    - [9.2.1 Define ThreadPooledClient](https://2xdb.net/dolphindb/api-cplusplus#921-define-threadpooledclient)
    - [9.2.2 Subscribe](https://2xdb.net/dolphindb/api-cplusplus#922-subscribe)
    - [9.2.3 Unsubscribe](https://2xdb.net/dolphindb/api-cplusplus#923-unsubscribe)
  -  [9.3 PollingClient](https://2xdb.net/dolphindb/api-cplusplus#93-pollingclient)
    - [9.3.1 Define PollingClient](https://2xdb.net/dolphindb/api-cplusplus#931-define-pollingclient)
    - [9.3.2 Unsubscribe](https://2xdb.net/dolphindb/api-cplusplus#932-unsubscribe)
# DolphinDB C++ API
DolphinDB C++ API supports the following development environments:
* Linux
* Windows Visual Studio
* Windows GNU (MingW)



## 1. Compilation

### 1.1 Compile under Linux

#### 1.1.1 Environment Setup

DolphinDB C++ API requires g++ 6.2 or later versions in Linux.

#### 1.1.2 Download bin file and header files

Download the following files:

> bin (libDolphinDBAPI.so)
  include (DolphinDB.h  Exceptions.h  SmartPointer.h  SysIO.h  Types.h  Util.h)

#### 1.1.3 Compile main.cpp

Create a directory "project" on the same level as "bin" and "include" folders. Create the file main.cpp in the "project" folder. 
```
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

#### 1.1.4 Compile

DolphinDB added support for SSL connection since stable version>=1.10.17 and current version>=1.20.6. So openssl should be installed before compilation using DolphinDB API.

g++ compiling command if -D_GLIBCXX_USE_CXX11_ABI=0 added：

```
g++ main.cpp -std=c++11 -DLINUX -D_GLIBCXX_USE_CXX11_ABI=0 -DLOGGING_LEVEL_2 -O2 -I../include   -lDolphinDBAPI -lpthread -lssl -L../bin/linux_x64/ABI0  -Wl,-rpath,.:../bin/linux_x64/ABI0 -o main

```
g++ compiling command if -D_GLIBCXX_USE_CXX11_ABI=0 not added：
```
g++ main.cpp -std=c++11 -DLINUX -DLOGGING_LEVEL_2 -O2 -I../include   -lDolphinDBAPI -lpthread -lssl -L../bin/linux_x64/ABI1  -Wl,-rpath,.:../bin/linux_x64/ABI1 -o main
```

#### 1.1.5 Run

After successfully compiling the program "main", start a DolphinDB server, then run the program "main", which connects to a DolphinDB server with IP address 111.222.3.44 and port number 8503 as specified in the program.

### 1.2 Compile under Windows

#### 1.2.1 Environment Setup

This tutorial uses Visual Studio 2017 64 bit version.

#### 1.2.2 Download bin file and header files

#### 1.2.3 Build Visual Studio Project

Build win32 console project and import header files, create main.cpp as in section 1.1.3, import libDolphinDBAPI.lib and configure the additional library directory as the lib directory.

>Note 1: The min/max macros are defined by default in VS. To avoid conflicts with functions `min` and `max` in the header file, `__NOMINMAX__` needs to be added to the macro definition.
>Note 2:  `WINDOWS` needs to be added to the macro definition.

#### 1.2.4 Compile and Run

Start the compilation, copy libDolphinDBAPI.dll to the output directory of the executable program. Now the compiled executable program is ready to be exuected.

The Windows gnu development environment is similar to Linux.


### 2. Establish DolphinDB connection

The most important object provided by DolphinDB C++ API is DBConnection. It allows C++ applications to execute script and functions on DolphinDB servers and transfer data between C++ applications and DolphinDB servers in both directions. The DBConnection class provides the following main methods:

| Method Name | Details |
|:------------- |:-------------|
|connect(host, port, [username, password])|Connect the session to DolphinDB server|
|login(username,password,enableEncryption)|Log in to DolphinDB server|
|run(script)|Run script on DolphinDB server|
|run(functionName,args)|Call a function on DolphinDB server|
|upload(variableObjectMap)|Upload local data to DolphinDB server|
|initialize()|Initialize the connection|
|close()|Close the current session|

The C++ API connects to a DolphinDB server via TCP/IP. The `connect` method uses parameters 'ip' and 'port'.
```
DBConnection conn;
bool ret = conn.connect("111.222.3.44", 8503);
```

To connect to a cluster, we need to log in with a username and password. The default administrator username and password are "admin" and "123456" respectively.
```
DBConnection conn;
bool ret = conn.connect("111.222.3.44", 8503,"admin","123456");

```

When declaring the connection variable, there are two optional parameters: enableSSL (supports SSL), enableAYSN (supports asynchronous communication). The default value of these two parameters is false.

The following example is to establish a connection that supports SSL instead of asynchronous, and the server side should add the parameter `enableHTTPS=true` in dolphindb.cfg of single mode and cluster.cfg of cluster mode.

```C++
DBConnection conn(true,false)
```

The following establishment does not support SSL, but supports asynchronous connection. In the case of asynchronous, only DolphinDB scripts and functions through function `run` are allowed to be executed, and no values are returned. This function is suitable for asynchronous writing of data.

```C++
DBConnection conn(false,true)


#### 3. Execute DolphinDB script

Execute Dolphindb script with method `run`.

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

The maximum length of the script is 65,535 bytes. If the script contains multiple statements, only the result of the last statement is returned. If there is a syntax error in the script or there is a network problem, an exception will be thrown.

### 4. Call DolphinDB functions

Other than running script, method `run` can also execute DolphinDB built-in functions or user-defined functions on a remote DolphinDB server. If method `run` has only one parameter, the parameter is script. If method `run` has 2 parameters, the first parameter is a DolphinDB function name and the second parameter is the function's parameters.

The following examples illustrate 3 ways to call DolphinDB's built-in function `add` in C++, depending on the locations of the parameters "x" and "y" of function `add`.

* Both parameters are on DolphinDB server

If both variables "x" and "y" have been generated on DolphinDB server by C++ applications,
```
conn.run("x = [1,3,5];y = [2,4,6]");
```
then we can execute run("script") directly.
```
ConstantSP result = conn.run("add(x,y)");
cout<<result->getString()<<endl;
```
Output:
> [3,7,11]

* Only 1 parameter exists on DolphinDB server

Parameter "x" was generated on DolphinDB server by the C++ program, and parameter "y" is to be generated by the C++ program.
```
conn.run("x = [1,3,5]");
```
In this case, we need to use "partial application" to embed parameter "x" in function `add`. For details, please refer to [Partial Application Documentation](https://www.dolphindb.com/help/PartialApplication.html)。

```
vector<ConstantSP> args;
ConstantSP y = Util::createVector(DT_DOUBLE, 3); 
double array_y[] = {1.5, 2.5, 7};
y->setDouble(0, 3, array_y); 
args.push_back(y);
ConstantSP result = conn.run("add{x,}", args);
cout<<result->getString()<<endl;
```
Output:
> [2.5, 5.5, 12]

* Both parameters are to be generated by C++ program

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

Output:
> [10, 10, 10]

### 5. Upload local objects to DolphinDB Server

We can use method `upload` to upload local data to DolphinDB server. 

In the following example, we define function `createDemoTable` in C++ to create a local table. 

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

Please note that the example above, as the method `set` is a virtual function, it comes with costly overhead. It is quite inefficient to assign value to the table's columns one by one with the `set` method with large data volumes. In addition, as `createString`, `createDate`, `createDouble` methods require the operating system to allocate memory, repeated calls will also incur a lot of overhead.

A more reasonable way is to define an array of the corresponding data type. For example: setInt(INDEX start, int len, const int* buf). Then pass the data to the array one or more times in batches. For example: .  

Therefore, if the table has a small amount of data, we can use the method in the example above. If the table has a large number of data, we recommended to use the method in the example below.

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
        int *dtp = columnVecs[1]->getIntBuffer(start, len, array_dt_buf); //dtp points to the buffer head generated by `getIntBuffer` each time
        double *dbp = columnVecs[2]->getDoubleBuffer(start, len, array_db_buf); //dbp points to the buffer head generated by `getDoubleBuffer` each time
        for (int i = 0; i < len; ++i) {
            columnVecs[0]->setString(i+start, "name_"+std::to_string(++no)); //assign value to column 'name' of string type instead of with method `getbuffer`
            dtp[i] = 17898+i; 
            dbp[i] = (rand()%100)/3.0;
        }
        columnVecs[1]->setInt(start, len, dtp); // write the contents of the buffer to the array with `setInt` method
        columnVecs[2]->setDouble(start, len, dbp); //write the contents of the buffer to the array with `setDouble` method
        start += len;
    }
    return table;
}
```
The example above uses methods such as `getIntBuffer` to directly fetch a readable and writable buffer. After writing, it use methods such as `setInt` to write the buffer back to the array. Methods like `setInt` check the buffer address and address of the object. If the addresses are the same, no data copy will occur. In most cases, the two addresses are identical, which avoids unnecessary data copy and improves performance.

In the following script, we create a table object with the function `createDemoTable()`, upload it to DolphinDB with method `upload`, then assign the data of this table to the local object 'result' and print it out.

```C++
TableSP table = createDemoTable();
conn.upload("myTable", table);
string script = "select * from myTable;";
ConstantSP result = conn.run(script);
cout<<result->getString()<<endl;
```

Output:
```console
name    date       price    
------- ---------- ---------
name_1  2019.01.02 27.666667
name_2  2019.01.03 28.666667
name_3  2019.01.04 25.666667
name_4  2019.01.05 5        
name_5  2019.01.06 31       
...
```

### 6. Read data

DolphinDB not only supports multiple data types (Int, Float, String, Date, DataTime, etc), but also multiple data forms (Vector, Set, Matrix, Dictionary, Table, AnyVector, etc). This section introduces how to read different data forms in DolphinDB with the DBConnection object.

Required header files:

```C++
#include "DolphinDB.h"
#include "Util.h"
```
- Vector

```C++
VectorSP v = conn.run("1..10");
int size = v->size();
for(int i = 0; i < size; ++i)
    cout<<v->getInt(i)<<endl;
```

```C++
VectorSP v = conn.run("2010.10.01..2010.10.30");
int size = v->size();
for(int i = 0; i < size; ++i)
    cout<<v->getString(i)<<endl;
```

- Set

```
SetSP set = conn.run("set(4 5 5 2 3 11 6)");
cout<<set->getString()<<endl;
```

- Matrix

```
ConstantSP matrix = conn.run("1..6$2:3");
cout<<matrix->getString()<<endl;
```

- Dictionary

```
DictionarySP dict = conn.run("dict(1 2 3,`IBM`MSFT`GOOG)");
cout << dict->get(Util::createInt(1))->getString()<<endl;
```

In the example above, we create an INT value with function `Util::createInt()` and use `get` method to retrieve a value for a key.

- Table

```
string sb;
sb.append("n=20000\n");
sb.append("syms=`IBM`C`MS`MSFT`JPM`ORCL`BIDU`SOHU`GE`EBAY`GOOG`FORD`GS`PEP`USO`GLD`GDX`EEM`FXI`SLV`SINA`BAC`AAPL`PALL`YHOO`KOH`TSLA`CS`CISO`SUN\n");
sb.append("mytrades=table(09:30:00+rand(18000,n) as timestamp,rand(syms,n) as sym, 10*(1+rand(100,n)) as qty,5.0+rand(100.0,n) as price);\n");
sb.append("select qty,price from mytrades where sym==`IBM;");
TableSP table = conn.run(sb);
cout<<table->getString()<<endl;
```

- AnyVector

Unlike a regular vector, the elements of an AnyVector can have different data types or data forms.

```
ConstantSP result = conn.run("{1, 2, {1,3,5},{0.9, 0.8}}");
cout<<result->getString()<<endl;
```

Get the third element with method `get`:
```
VectorSP v = result->get(2);
cout<<v->getString()<<endl;
```
The result is an Int Vector [1,3,5].


### 7. Write to DolphinDB tables

There are 3 types of DolphinDB tables:

- In-memory table: it has the fastest access speed, but if the node shuts down the data will be lost.
- Local disk table: data are saved on the local disk and can be loaded into memory.
- Distributed table: data are distributed across disks of multiple nodes. Users can query the table as if it is a local disk table.

#### 7.1 Save data to a DolphinDB in-memory table

DolphinDB offers several ways to save data to an in-memory table:
- Save a single row of data with `insert into`
- Save multiple rows of data in bulk with function `tableInsert`
- Save a table object with function `tableInsert`

It is not recommended to save data with function `append!`, as `append!` returns all records of a table and unnecessarily increases the network traffic.

The table in the following examples has 3 columns. Their data types are STRING, DATE and DOUBLE. The column names are name, date and price, respectively.
```
t = table(100:0, `name`date`price, [STRING,DATE,DOUBLE]);
share t as tglobal;
```
By default, an in-memory table is not shared among sessions. To access it in a different session, we need to share it among sessions with `share`.

##### 7.1.1 Save data to an in-memory table with `insert into`

To save a single record to an in-memory table with `insert into`:
```
char script[100];
sprintf(script, "insert into tglobal values(%s, date(timestamp(%ld)), %lf)", "`a", 1546300800000, 1.5);
conn.run(script);
```

To save multiple records to an in-memory table with `insert into`:
```C++
string script;
int rowNum=10000, indexCapacity=10000;
VectorSP names = Util::createVector(DT_STRING, rowNum, indexCapacity);
VectorSP dates = Util::createVector(DT_DATE, rowNum, indexCapacity);
VectorSP prices = Util::createVector(DT_DOUBLE, rowNum, indexCapacity);

int array_dt_buf[Util::BUF_SIZE]; 
double array_db_buf[Util::BUF_SIZE]; 

int start = 0;
int no=0;
while (start < rowNum) {
    size_t len = std::min(Util::BUF_SIZE, rowNum - start);
    int *dtp = dates->getIntBuffer(start, len, array_dt_buf); 
    double *dbp = prices->getDoubleBuffer(start, len, array_db_buf); 
    for (int i = 0; i < len; i++) {
        names->setString(i+start, "name_"+std::to_string(++no)); 
        dtp[i] = 17898+i; 
        dbp[i] = (rand()%100)/3.0;
    }
    dates->setInt(start, len, dtp); 
    prices->setDouble(start, len, dbp); 
    start += len;
}
vector<string> allnames = {"names", "dates", "prices"};
vector<ConstantSP> allcols = {names, dates, prices};
conn.upload(allnames, allcols); 

script += "insert into tglobal values(names,dates,prices); tglobal"; 
TableSP table = conn.run(script); 
```

##### 7.1.2 Save data in batches with `tableInsert`

```
vector<ConstantSP> args;
TableSP table = createDemoTable();
VectorSP range = Util::createPair(DT_INDEX);
range->setIndex(0, 0);
range->setIndex(1, 10);
cout<<range->getString()<<endl;
args.push_back(table->get(range));
conn.run("tableInsert{tglobal}", args); 
```

The example above uses partial application in DolphinDB to embed a table in `tableInsert{tglobal}` as a function. For details about partial application, please refer to [Partial Application Documentation](https://www.dolphindb.com/help/PartialApplication.html).

##### 7.1.3 Use function `tableInsert` to save TableSP objects

```
vector<ConstantSP> args;
TableSP table = createDemoTable();
args.push_back(table);
conn.run("tableInsert{tglobal}", args); 
```

#### 7.2 Save data to a distributed table

Distributed table is recommended by DolphinDB in production environment. It supports snapshot isolation and ensures data consistency. With data replication, Distributed tables offers fault tolerance and load balancing.

Use the following script in DolphinDB to create a distributed table. Function `database` creates a database. The path of a distributed database must start with "dfs". Function `createPartitionedTable` creates a distributed table. 

```
login(`admin, `123456)
dbPath = "dfs://SAMPLE_TRDDB";
tableName = `demoTable
db = database(dbPath, VALUE, 2010.01.01..2010.01.30)
pt=db.createPartitionedTable(table(1000000:0, `name`date`price, [STRING,DATE,DOUBLE]), tableName, `date)
```

Use function `loadTable` to load a distributed table. Use function `tableInsert` to append data to the table.

```C++
TableSP table = createDemoTable();
vector<ConstantSP> args;
args.push_back(table);
conn.run("tableInsert{loadTable('dfs://SAMPLE_TRDDB', `demoTable)}", args);
```

We can also use function `append!` to append data to a distributed table. However, its performance is worse than `tableInsert`. We recommend to use `tableInsert` instead of `append!`. 

```C++
TableSP table = createDemoTable();
conn.upload("mt", table);
conn.run("loadTable('dfs://SAMPLE_TRDDB', `demoTable).append!(mt);");
conn.run(script);
```

#### 7.3 Save data to a local disk table

Local disk tables can be used for data analysis on historical data sets. They do not support transactions, nor do they support concurrent read and write.

Use the following script in DolphinDB to create a local disk table. Function `database` creates a database. Function `saveTable` saves an in-memory table to disk. 

```
t = table(100:0, `name`date`price, [STRING,DATE,DOUBLE]); 
db=database("/home/dolphindb/demoDB"); 
saveTable(db, t, `dt); 
share t as tDiskGlobal;
```

Next, use `tableInsert` to append data to a shared in-memory table tDiskGlobal, then use `saveTable` to save the inserted data on disk.

```C++
TableSP table = createDemoTable();
vector<ConstantSP> args;
args.push_back(table);
conn.run("tableInsert{tDiskGlobal}", args);
conn.run("saveTable(db,tDiskGlobal,`dt);");
```

We can also use `append!` to append data to a local disk table. 

```C++
TableSP table = createDemoTable();
conn.upload("mt", table);
string script;
script += "db=database(\"/home/demoTable1\");";
script += "tDiskGlobal.append!(mt);";
script += "saveTable(db,tDiskGlobal,`dt);";
conn.run(script);
```

Note:
For a local disk table, function `append!` appends data to memory only. To save the new data on disk, we must execute command `saveTable` afterwards. 

For more information about DolphinDB C++ API, please refer to C++ API header file dolphindb.h. 

---
# C++ Streaming API

DolphinDB C++ Streaming API processes streaming data in 3 ways: ThreadedClient, ThreadPooledClient and PollingClient.

For details, please refer to test/StreamingThreadedClientTester.cpp, test/StreamingThreadPooledClientTester.cpp and test/StreamingPollingClientTester.cpp.

### 8. Build

#### 8.1 Linux64

##### 8.1.1 Build with cmake

Install cmake

```bash
sudo apt-get install cmake
```

Build the three examples:

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ../path_to_api-cplusplus/
make -j`nproc`
```

3 executables will be generated after the compilation.

#### 8.2 Build under Windows with MinGW

Install [MinGW](http://www.mingw.org/) and [CMake](https://cmake.org/)

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release `path_to_api-cplusplus` -G "MinGW Makefiles"
mingw32-make -j `nproc`
```

3 executables will be generated after the compilation.

**Note:** Before compiling, copy libDolphinDBAPI.dll to the build directory.

**Note:** Before running, copy libDolphinDBAPI.dll and libgcc_s_seh-1.dll to the directory of your executable file.

### 9. Streaming

#### 9.1 ThreadedClient

ThreadedClient produces a single thread that calls for a user-defined handler on each incoming message.

##### 9.1.1 Define the threaded client

```
ThreadedClient::ThreadClient(int listeningPort);
```
###### Parameters

- listeningPort: the subscription port number of the threaded client.

##### 9.1.2  Subscribe

```
ThreadSP ThreadedClient::subscribe(string host, int port, MessageHandler handler, string tableName, string actionName = DEFAULT_ACTION_NAME, int64_t offset = -1, bool resub = true, VectorSP filter = nullptr);
```
- host: the hostname of the server.
- port: the port number of the server.
- handler: the user-defined function that is called on every incoming message. The input of the function is a message and function result must be void. Each message is a row of the streaming table. 
- tableName: a string indicating the name of the shared streaming table on the server.
- actionName: a string indicating the name assigned to the subscription task. It can have letters, digits, and underscores.
- offset: the position of the first message to subscribe. If 'offset' is unspecified, or negative, or beyond the number of rows in the streaming table, the subscription starts from the first row. 'offset' is relative to the first row of the streaming table when it is created. If some rows were cleared from memory due to cache size limit, they are still considered in determining where the subscription starts.
- resub: a bool indicating whether to resubscribe after the subscription is interrupted.
- filter: a vector of selected values in the filtering column. Only the messages with the specified filtering column values are subscribed. The filtering column is set with function `setStreamTableFilterColumn`.

ThreadSP points to the handler loop thread, which will stop when function `unsubscribe` on the same topic is called.

###### Example

```c++
auto t = client.subscribe(host, port, [](Message msg) {
    // user-defined routine
    }, tableName);
t->join();
```

##### 9.1.3 Unsubscribe
```
void ThreadClient::unsubscribe(string host, int port, string tableName, string actionName = DEFAULT_ACTION_NAME);
```
Unsubscribe from a topic.

- host: the hostname of the server.
- port: the port number of the server.
- tableName: a string indicating the name of the shared streaming table on the server.
- actionName: a string indicating the name assigned to the subscription task. It can have letters, digits and underscores.

<!-- /////////////////////////////////////////////////////////////////////// -->

#### 9.2 ThreadPooledClient

ThreadPooledClient produces multiple threads that poll and call a user-defined handler simultaneously on each incoming message.

##### 9.2.1 Define ThreadPooledClient
```
ThreadPooledClient::ThreadPooledClient(int listeningPort, int threadCount);
```
- listeningPort: the subscription port number of the threadpooled client node.
- threadCount: the size of the thread pool.

##### 9.2.2 Subscribe
```
vector<ThreadSP> ThreadPooledClient::subscribe(string host, int port, MessageHandler handler, string tableName, string actionName = DEFAULT_ACTION_NAME, int64_t offset = -1, bool resub = true, VectorSP filter = nullptr);
```
For explanation of the parameters, please check section 2.1.2.

Return a vector of ThreadSP pointers, each of them points to a handler loop thread that will stop when `unsubscribe` on the same topic is called.

###### Example

```c++
auto vec = client.subscribe(host, port, [](Message msg) {
    // user-defined routine
    }, tableName);
for(auto& t : vec) {
    t->join();
}
```

##### 9.2.3 Unsubscribe
```
void ThreadPooledClient::unsubscribe(string host, int port, string tableName, string actionName = DEFAULT_ACTION_NAME);
```
Unsubscribe from a topic. 

For explanation of the parameters, please check section 2.1.3.


<!-- /////////////////////////////////////////////////////////////////////// -->

#### 9.3 PollingClient

PollingClient returns a message queue, from which user can retrieve and process the messages.

##### 9.3.1 Define PollingClient
```
PollingClient::PollingClient(int listeningPort);
```
- listeningPort: the subscription port number of the polling client node.

##### Subscribe
```
2.3.2 MessageQueueSP PollingClient::subscribe(string host, int port, string tableName, string actionName = DEFAULT_ACTION_NAME, int64_t offset = -1, bool resub = true, VectorSP filter = nullptr);
```
For explanation of the parameters, please check section 2.1.2.

MessageQueueSP points to a MessageQueue, where user can poll messages from the server.


###### Example

```c++
auto queue = client.subscribe(host, port, handler, tableName);
Message msg;
while(true) {
    if(queue->poll(msg, 1000)) {
        if(msg.isNull()) break;
        // handle msg
    }
}
```

##### 9.3.2 Unsubscribe
```
void PollingClient::unsubscribe(string host, int port, string tableName, string actionName = DEFAULT_ACTION_NAME);
```
Unsubscribe from a topic.

For explanation of the parameters, please check section 2.1.3.

**Note**: when `unsubscribe` is called, a NULL pointer will be pushed into the queue. Users need to handle this situation.
