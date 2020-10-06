# DolphinDB C++ API

DolphinDB C++ API supports the following development environments:
* Linux
* Windows Visual Studio
* Windows GNU (MingW)

This tutorial covers the following topics about how to use DolphinDB C++ API in Linux:

- [1. Compilation](#1-compilation)
- [2. Establish DolphinDB connection](#2-establish-dolphindb-connection)
- [3. Execute DolphinDB script](#3-execute-dolphindb-script)
- [4. Call DolphinDB functions](#4-call-dolphindb-functions)
- [5. Upload local objects to DolphinDB Server](#5-upload-local-objects-to-dolphindb-server)
- [6. Read data](#6-read-data)
- [7. Write to DolphinDB tables](#7-write-to-dolphindb-tables)
- [8. C++ Streaming API](#8-c-streaming-api)

## 1. Compilation

### 1.1 Compile under Linux

#### 1.1.1 Environment Setup

DolphinDB C++ API requires g++ 6.2 or later versions in Linux.

#### 1.1.2 Download bin file and header files

Download the following files:

- [bin](./bin) (libDolphinDBAPI.so)
- [include](./include) (DolphinDB.h, Exceptions.h, SmartPointer.h, SysIO.h, Types.h, Util.h)

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

In order to be compatible with the old compiler, there are 2 versions of libDolphinDBAPI.so. 

One version uses the -D_GLIBCXX_USE_CXX11_ABI=0 option when compiling, and it is under the [bin/linux_x64/ABI0](./bin/linux_x64/ABI0) directory. 

The g++ compiling command：
```
g++ main.cpp -std=c++11 -DLINUX -D_GLIBCXX_USE_CXX11_ABI=0 -DLOGGING_LEVEL_2 -O2 -I../include   -lDolphinDBAPI -lpthread -L../bin/linux_x64/ABI0  -Wl,-rpath,.:../bin/linux_x64/ABI0 -o main
```

The other version does not use -D_GLIBCXX_USE_CXX11_ABI=0, and it is under the [bin/linux_x64/ABI1](./bin/linux_x64/ABI1) directory.

The g++ compiling command：
```
g++ main.cpp -std=c++11 -DLINUX -DLOGGING_LEVEL_2 -O2 -I../include   -lDolphinDBAPI -lpthread -L../bin/linux_x64/ABI1  -Wl,-rpath,.:../bin/linux_x64/ABI1 -o main
```

#### 1.1.5 Run

After successfully compiling the program "main", start a DolphinDB server, then run the program "main", which connects to a DolphinDB server with the IP address and port number as specified in the program.

### 1.2 Compile under Windows

#### 1.2.1 Environment Setup

This tutorial uses Visual Studio 2017 64-bit version.

#### 1.2.2 Download bin file and header files

Download this GitHub project.

#### 1.2.3 Build Visual Studio Project

Build Windows console project and import header files, create main.cpp as in section 1.1.3, import libDolphinDBAPI.lib and configure the additional library directory as the lib directory.

> Note 1: The min/max macros are defined by default in VS. To avoid conflicts with functions `min` and `max` in the header file, `__NOMINMAX__` needs to be added to the macro definition.
> Note 2:  `WINDOWS` needs to be added to the macro definition.

#### 1.2.4 Compile and Run

Start the compilation, copy libDolphinDBAPI.dll to the output directory of the executable program. Now the compiled executable program is ready to be exuected.

The Windows gnu development environment is similar to Linux.


## 2. Establish DolphinDB connection

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

The C++ API connects to a DolphinDB server via TCP/IP. The `connect` method needs the IP address and port number of the DolphinDB server.
```
DBConnection conn;
bool ret = conn.connect("111.222.3.44", 8503);
```

To connect to a cluster, we need to log in with a username and password. The default administrator username and password are "admin" and "123456" respectively.
```
DBConnection conn;
bool ret = conn.connect("111.222.3.44", 8503,"admin","123456");
```

If a connection is established without username and password, it has guest privileges. To elevate permissions later, you can use conn.login('admin','123456',true) to log in.

Please note that all functions of the DBConnection class are not thread-safe and therefore cannot be called in parallel, otherwise the program may crash.

## 3. Execute DolphinDB script

Execute DolphinDB script with method `run`.

```
ConstantSP v = conn.run("`IBM`GOOG`YHOO");
int size = v->size();
for(int i = 0; i < size; i++)
    cout<<v->getString(i)<<endl;
```

Output:
> ["IBM", "GOOG", "YHOO"]

The maximum length of the script is 65,535 bytes. If the script contains multiple statements, only the result of the last statement is returned. If there is a syntax error in the script or there is a network problem, an exception will be thrown.

## 4. Call DolphinDB functions

Other than running script, method `run` can also execute DolphinDB built-in functions or user-defined functions on a remote DolphinDB server. If method `run` has only one parameter, the parameter is DolphinDB script. If method `run` has 2 parameters, the first parameter is a DolphinDB function name and the second parameter is the function's parameters.

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

If the parameter "x" has been generated on DolphinDB server by the C++ program, and parameter "y" is to be generated by the C++ program,
```
conn.run("x = [1,3,5]");
```
then we need to use "partial application" to embed parameter "x" in function `add`. For details, please refer to [Partial Application Documentation](https://www.dolphindb.com/help/PartialApplication.html).

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

## 5. Upload local objects to DolphinDB Server

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

Please note that the example above, as the method `set` is a virtual function, it comes with costly overhead. With large volumes of data, it is inefficient to assign values to the table's columns one by one with the `set` method. In addition, as `createString`, `createDate`, `createDouble` methods require the operating system to allocate memory, repeated calls will also incur a lot of overhead.

A more efficient way is to define an array of the corresponding data type. For example: setInt(INDEX start, int len, const int* buf). Then pass the data to the array one or more times in batches.    

Therefore, if the table is small, we can use the method in the example above. With a large table, we recommended to use the method in the example below.

```C++
TableSP createDemoTable(){
    vector<string> colNames = {"name", "date", "price"};
    vector<DATA_TYPE> colTypes = {DT_STRING, DT_DATE, DT_DOUBLE};
    int colNum = 3, rowNum = 10000, indexCapacity=10000;
    ConstantSP table = Util::createTable(colNames, colTypes, rowNum, indexCapacity);
    vector<VectorSP> columnVecs;
    for(int i = 0; i < colNum; ++i)
        columnVecs.push_back(table->getColumn(i));

    int array_dt_buf[Util::BUF_SIZE]; 
    double array_db_buf[Util::BUF_SIZE]; 

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
The example above uses methods such as `getIntBuffer` to directly fetch a readable and writable buffer. After writing, it uses methods such as `setInt` to write the buffer back to the array. Methods like `setInt` check the buffer address and address of the object. If the addresses are the same, no data copy will occur. In most cases, the two addresses are identical. It avoids unnecessary data copy and improves performance.

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

## 6. Read data

DolphinDB supports multiple data types (Int, Float, String, Date, DataTime, etc) as well as multiple data forms (Vector, Set, Matrix, Dictionary, Table, AnyVector, etc). This section introduces how to read various data forms in DolphinDB with the DBConnection object.

Required header files:
```C++
#include "DolphinDB.h"
#include "Util.h"
```
### 6.1 Vector

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

### 6.2 Set

```
SetSP set = conn.run("set(4 5 5 2 3 11 6)");
cout<<set->getString()<<endl;
```

### 6.3 Matrix

```
ConstantSP matrix = conn.run("1..6$2:3");
cout<<matrix->getString()<<endl;
```

### 6.4 Dictionary

```
DictionarySP dict = conn.run("dict(1 2 3,`IBM`MSFT`GOOG)");
cout << dict->get(Util::createInt(1))->getString()<<endl;
```

### 6.5 Table

Create a table:
```
string sb;
sb.append("n=20000\n");
sb.append("syms=`IBM`C`MS`MSFT`JPM`ORCL`BIDU`SOHU`GE`EBAY`GOOG`FORD`GS`PEP`USO`GLD`GDX`EEM`FXI`SLV`SINA`BAC`AAPL`PALL`YHOO`KOH`TSLA`CS`CISO`SUN\n");
sb.append("mytrades=table(09:30:00+rand(18000,n) as timestamp,rand(syms,n) as sym, 10*(1+rand(100,n)) as qty,5.0+rand(100.0,n) as price);\n");
sb.append("select qty,price from mytrades where sym==`IBM;");
TableSP table = conn.run(sb);
cout<<table->getString()<<endl;
```

#### 6.5.1 method `getString()`
```C++
cout<<table->getString()<<endl; 
``` 

#### 6.5.2 method `getColumn()`

In the following script, first define a dynamic array columnVecs of type VectorSP to store the columns obtained from the table, then sequentially access columnVecs to process the data.

For each column of the table, we can use the `getString()` method to obtain the string type array of each column, and then use the C++ data type conversion function to convert the numeric type data into the corresponding data type. Temporal data needs to be stored as strings.

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

#### 6.5.3 method `getRow()`

For example, print the first row of the table. The result is a dictionary.
```C++
cout<<table->getRow(0)->getString()<<endl; 

// output
price->37.811678
qty->410
sym->IBM
timestamp->13:45:15
``` 

To fetch a column in a row, we can call method `getRow` first, and then call method `getMember`, as shown in the following example. The parameter of `getMember()` is a string type Constant object of the DolphinDB C++ API instead of a C++ built-in string type object.

```C++
cout<<table->getRow(0)->getMember(Util::createString("price"))->getDouble()<<endl;

// output
37.811678
```

It should be noted that accessing a table by rows is very inefficient. For better performance, it is recommended to access a table by columns as in section 6.5.2 and calculate in batches.


### 6.6 AnyVector

Unlike a regular vector, the elements of an AnyVector can have different data types or data forms.

```C++
ConstantSP result = conn.run("{1, 2, {1,3,5},{0.9, 0.8}}");
cout<<result->getString()<<endl;
```

Get the third element with method `get`:
```
VectorSP v = result->get(2);
cout<<v->getString()<<endl;
```
The result is an Int Vector [1,3,5].


## 7. Write to DolphinDB tables

There are 3 types of DolphinDB tables:

- In-memory table: it has the fastest access speed, but if the node shuts down the data will be lost.
- Distributed table (DFS table): data are saved across disks of multiple nodes or on the same node and management by the distributed file system. The path to a distributed table starts with "dfs://".
- Local disk table: data are saved on the local disk. It is not recommend for production use. 

### 7.1 Save data to a DolphinDB in-memory table

DolphinDB offers the following ways to save data to an in-memory table:
- Save a single row of data with `insert into`
- Save multiple rows of data in bulk with function `tableInsert`
- Save a table object with function `tableInsert`

It is not recommended to save data with function `append!`, as `append!` returns an empty table and unnecessarily increases the network traffic.

The table in the following examples has 3 columns: name, date and price. Their data types are STRING, DATE and DOUBLE, respectively.
```
t = table(100:0, `name`date`price, [STRING,DATE,DOUBLE]);
share t as tglobal;
```
By default, an in-memory table is not shared among sessions. To access it in a different session, we need to share it among sessions with command `share`.

#### 7.1.1 Save data to an in-memory table with `insert into`

Save a single record to an in-memory table with `insert into`:
```
char script[100];
sprintf(script, "insert into tglobal values(%s, date(timestamp(%ld)), %lf)", "`a", 1546300800000, 1.5);
conn.run(script);
```

Save multiple records to an in-memory table with `insert into`:
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

#### 7.1.2 Save data in batches with `tableInsert`

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

#### 7.1.3 Use function `tableInsert` to save TableSP objects

```
vector<ConstantSP> args;
TableSP table = createDemoTable();
args.push_back(table);
conn.run("tableInsert{tglobal}", args); 
```

### 7.2 Save data to a distributed table

Distributed table is recommended by DolphinDB for production use. It supports snapshot isolation and ensures data consistency. With data replication, Distributed tables offers fault tolerance and load balancing.

#### 7.2.1 Save TableSP objects with `tableInsert`

Use the following script in DolphinDB to create a distributed table. Function `database` creates a database. The path of a distributed database must start with "dfs://". Function `createPartitionedTable` creates a distributed table. 

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

#### 7.2.2 Concurrent writes to distributed tables

First, execute the following script on the DolphinDB server to create a distributed database "dfs://natlog" and a distributed table "natlogrecords". The database uses a COMPO partitioning scheme VALUE-HASH-HASH.
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

DolphinDB does not allow concurrent writes to the same partition. Therefore, when multiple threads on the client side write data in parallel, we need to ensure that each thread writes to a different partition.

For distributed tables with a HASH domain, DolphinDB C++ API provides the `getHash` function to obtain the hash value of the data. In multi-threaded concurrent writing to a distributed table, we can group the data according to the hash value of the partitioning column and specify a write thread for each group. This can ensure that each thread writes to a different partition.

```C++
ConstantSP spIP = Util::createConstant(DT_IP);
int key = spIP->getHash(BUCKETS);
```

Function `genData` below generates simulated data, and function `writeData` writes data.

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
For more examples about concurrent writes to distributed tables, please refer to [MultiThreadDFSWriting.cpp](./example/DFSWritingWithMultiThread/MultiThreadDfsWriting.cpp).

### 7.3 Save data to a local disk table

Local disk tables can be used for data analysis on historical data sets. They do not support transactions, nor do they support concurrent read and write. They are not recommended for production use. 

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

> Note: For a local disk table, function `append!` appends data to memory only. To save the new data on disk, we must execute command `saveTable` afterwards. 

For more information about DolphinDB C++ API, please refer to C++ API header file dolphindb.h. 

## 8. C++ Streaming API

DolphinDB C++ Streaming API processes streaming data in 3 ways: ThreadedClient, ThreadPooledClient and PollingClient.

For details, please refer to [test/StreamingThreadedClientTester.cpp](./test/StreamingThreadedClientTester.cpp), [test/StreamingThreadPooledClientTester.cpp](./test/StreamingThreadPooledClientTester.cpp) and [test/StreamingPollingClientTester.cpp](./test/StreamingPollingClientTester.cpp).

### 8.1 Build

#### 8.1.1 Linux64

Install cmake:
```bash
sudo apt-get install cmake
```

Build:
```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ../path_to_api-cplusplus/
make -j`nproc`
```

3 executables will be generated after the compilation.

#### 8.1.2 Build in Windows with MinGW

Install [MinGW](http://www.mingw.org/) and [CMake](https://cmake.org/)

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release `path_to_api-cplusplus` -G "MinGW Makefiles"
mingw32-make -j `nproc`
```

3 executables will be generated after the compilation.

**Note:** 
- 1. Before compiling, copy libDolphinDBAPI.dll to the build directory.
- 2. Before running, copy libDolphinDBAPI.dll and libgcc_s_seh-1.dll to the directory of your executable file.

### 8.2 API

#### 8.2.1 ThreadedClient

ThreadedClient produces a thread that calls for a user-defined handler on each incoming message.

##### 8.2.1.1 Define the threaded client

```
ThreadedClient::ThreadClient(int listeningPort);
```
###### Parameters

- listeningPort: the subscription port number of the threaded client.

##### 8.2.1.2 Subscribe

```
ThreadSP ThreadedClient::subscribe(string host, int port, MessageHandler handler, string tableName, string actionName = DEFAULT_ACTION_NAME, int64_t offset = -1, bool resub = true, VectorSP filter = nullptr);
```
- host: the hostname of the server.
- port: the port number of the server.
- handler: the user-defined function that is called on every incoming message. The input of the function is a message and function result must be void. Each message is a row of the streaming table. 
- tableName: a string indicating the name of the shared streaming table on the server.
- actionName: a string indicating the name assigned to the subscription task. It can have letters, digits, and underscores.
- offset: the position of the first message to subscribe. If 'offset' is unspecified, or negative, or beyond the number of rows in the streaming table, the subscription starts from the first row. 'offset' is relative to the first row of the stream table when it is created. If some rows were cleared from memory due to cache size limit, they are still considered in determining where the subscription starts.
- resub: a bool indicating whether to resubscribe after the subscription is interrupted.
- filter: a vector of selected values in the filtering column. Only the messages with the specified filtering column values are subscribed. The filtering column is set with function `setStreamTableFilterColumn`.

ThreadSP points to the handler loop thread, which will quit when function `unsubscribe` on the same topic is called.

###### Example

```c++
auto t = client.subscribe(host, port, [](Message msg) {
    // user-defined routine
    }, tableName);
t->join();
```

##### 8.2.1.3 Unsubscribe
```
void ThreadClient::unsubscribe(string host, int port, string tableName, string actionName = DEFAULT_ACTION_NAME);
```
Unsubscribe from a topic.

- host: the hostname of the server.
- port: the port number of the server.
- tableName: a string indicating the name of the shared stream table on the server.
- actionName: a string indicating the name assigned to the subscription task. It can have letters, digits and underscores.


#### 8.2.2 ThreadPooledClient

ThreadPooledClient produces multiple threads that poll and call a user-defined handler simultaneously on each incoming message. When messages arrive faster than a single thread can handle, ThreadPooledClient has an advantage over ThreadedClient.

##### 8.2.2.1 Define ThreadPooledClient
```
ThreadPooledClient::ThreadPooledClient(int listeningPort, int threadCount);
```
- listeningPort: the subscription port number of the threadpooled client node.
- threadCount: the size of the thread pool.

##### 8.2.2.2 Subscribe
```
vector<ThreadSP> ThreadPooledClient::subscribe(string host, int port, MessageHandler handler, string tableName, string actionName = DEFAULT_ACTION_NAME, int64_t offset = -1, bool resub = true, VectorSP filter = nullptr);
```
For explanation of the parameters, please check section 8.2.1.2.

Return a vector of ThreadSP pointers, each of which points to a handler loop thread that will quit when `unsubscribe` on the same topic is called.

###### Example

```c++
auto vec = client.subscribe(host, port, [](Message msg) {
    // user-defined routine
    }, tableName);
for(auto& t : vec) {
    t->join();
}
```

##### 8.2.2.3 Unsubscribe
```
void ThreadPooledClient::unsubscribe(string host, int port, string tableName, string actionName = DEFAULT_ACTION_NAME);
```
Unsubscribe from a topic. 

For explanation of the parameters, please check section 8.2.1.3.

#### 8.2.3 PollingClient

PollingClient returns a message queue, from which user can retrieve and process the messages.

##### 8.2.3.1 Define PollingClient
```
PollingClient::PollingClient(int listeningPort);
```
- listeningPort: the subscription port number of the polling client node.

##### Subscribe
```
2.3.2 MessageQueueSP PollingClient::subscribe(string host, int port, string tableName, string actionName = DEFAULT_ACTION_NAME, int64_t offset = -1, bool resub = true, VectorSP filter = nullptr);
```
For explanation of the parameters, please check section 8.2.1.2.

MessageQueueSP points to a message queue, from which  user can poll messages from the server.

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

##### 8.2.3.2 Unsubscribe
```
void PollingClient::unsubscribe(string host, int port, string tableName, string actionName = DEFAULT_ACTION_NAME);
```
Unsubscribe from a topic.

For explanation of the parameters, please check section 8.2.1.3.

Note that for this subscription mode, if a null pointer is returned, it means that the subscription has been cancelled.
