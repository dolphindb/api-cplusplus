# DolphinDB C++ API

This tutorial includes the following topics about how to use DolphinDB C++ API in Linux:
* Compile a project
* Execute DolphinDB script  
* Call DolphinDB built-in functions
* Upload local objects to DolphinDB server
* Append data to DolphindB tables

### 1. Environment Setup

To run DolphinDB C++ API, we need g++ 6.2 in Linux. 

### 2. Compile a project

#### 2.1 Download bin file and header files

Download api-cplusplus from this git repo, including "bin" and "include" folders in your project.

> bin (libDolphinDBAPI.so)  
  include (DolphinDB.h  Exceptions.h  SmartPointer.h  SysIO.h  Types.h  Util.h) 
  
#### 2.2 Compile main.cpp

Create a directory "project" on the same level as "bin" and "include" folders, enter the project folder, and then create the file main.cpp:
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

#### 2.3 Compile

g++ compiling command:

> g++ main.cpp -std=c++11 -DLINUX -DLOGGING_LEVEL_2 -O2 -I../include -lDolphinDBAPI -lssl  -lpthread -luuid -L../bin  -Wl,-rpath ../bin/ -o main

#### 2.4 Run

After successfully compiling the program main, start a DolphinDB server, then you can run the program "main", which connects to a DolphinDB server with IP address 111.222.3.44 and port number 8503 as specified in the program. 

### 3. Execute DolphinDB script

#### 3.1 Connect to a DolphinDB server

The C++ API connects to a DolphinDB server via TCP/IP. The `connect` method uses parameters `ip` and `port`.
```
DBConnection conn;
bool ret = conn.connect("111.222.3.44", 8503);
```

To connect to a cluster, you need to log in with the username and password. The default administrator username and password are "admin" and "123456" respectively.
```
DBConnection conn;
bool ret = conn.connect("111.222.3.44", 8503,"admin","123456");
```

#### 3.2 Execute DolphinDB script

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

If the script contains multiple statements, only the result of the last statement is returned. If there is a syntax error in the script or there is a network problem, an exception will be thrown.

#### 3.3 Support multiple data types and data forms

DolphinDB supports multiple data types (Int, Float, String, Date, DataTime, etc) and multiple data forms (Vector, Set, Matrix, Dictionary, Table, AnyVector, etc.)

##### 3.3.1 Vector

```
VectorSP v = conn.run("1..10");
int size = v->size();
for(int i = 0; i < size; i++)
    cout<<v->getInt(i)<<endl;
```

```
VectorSP v = conn.run("2010.10.01..2010.10.30");
int size = v->size();
for(int i = 0; i < size; i++)
    cout<<v->getString(i)<<endl;
```

##### 3.3.2 Set

```
VectorSP set = conn.run("set(4 5 5 2 3 11 6)");
cout<<set->getString()<<endl;
```

##### 3.3.3 Matrix

```
ConstantSP matrix = conn.run("1..6$2:3");
cout<<matrix->getString()<<endl;
```

##### 3.3.4 Dictionary

```
ConstantSP dict = conn.run("dict(1 2 3,`IBM`MSFT`GOOG)");
cout << dict->get(Util::createInt(1))->getString()<<endl;
```

You can use `get` method to retrieve a value. Note that you need to create an Int value through function `Util::createInt()` in order to do so. 

##### 3.3.5 Table

```
string sb;
sb.append("n=20000\n");
sb.append("syms=`IBM`C`MS`MSFT`JPM`ORCL`BIDU`SOHU`GE`EBAY`GOOG`FORD`GS`PEP`USO`GLD`GDX`EEM`FXI`SLV`SINA`BAC`AAPL`PALL`YHOO`KOH`TSLA`CS`CISO`SUN\n");
sb.append("mytrades=table(09:30:00+rand(18000,n) as timestamp,rand(syms,n) as sym, 10*(1+rand(100,n)) as qty,5.0+rand(100.0,n) as price);\n");
sb.append("select qty,price from mytrades where sym==`IBM;");
ConstantSP table = conn.run(sb);
```

##### 3.3.6 AnyVector

```
ConstantSP result = conn.run("{1, 2, {1,3,5},{0.9, 0.8}}");
cout<<result->getString()<<endl;
```

Get the third element with method `get`:
```
VectorSP v =  result->get(2);
cout<<v->getString()<<endl;
```

The result is an Int Vector [1,3,5].

### 4. Call DolphinDB built-in functions

The DolphinDB C++ API provides an interface to call DolphinDB built-in functions:
```
vector<ConstantSP> args;
double array[] = {1.5, 2.5, 7};
ConstantSP vec = Util::createVector(DT_DOUBLE, 3); // build a Double Vector with size of 3.
vec->setDouble(0, 3, array); // assign values
args.push_back(vec);
ConstantSP result = conn.run("sum", args); // call built-in function "sum".
cout<<result->getString()<<endl;
 ```
 
The `run` method above returns the result of function `sum`, which accepts a Double Vector via `Util::createVector(DT_DOUBLE, 3)`.

The first parameter of the `run` method is a function name; the second parameter is the vector of ConstantSP type (the Constant class is the base class of all types in DolphinDB).

### 5. Upload local objects to DolphinDB Server

The C++ API provides a flexible interface to create local objects. With the `upload` method, you can implement the conversion between local objects and Server objects.

The local object can be uploaded to the DolphinDB Server through the C++ API. The following example first creates a local table object, then uploads it to the DolphinDB server, and then gets the object from the server. The complete code is as follows:
```
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

### 6. Append data to DolphinDB tables

Data can be appended to a DolphinDB table with C++ API. DolphinDB supports 3 types of tables: in-memory table, table on local disk, and distributed table. 

#### 6.1 In-memory table

##### 6.1.1 Create an in-memory table
```
t = table(100:0, `name`date`price, [STRING, DATE, DOUBLE]);
share t as tglobal
```
`table`: create an in-memory table and specify capacity, size, column names, and data types.
`share`: share the table across sessions, so that multiple clients can write to table "t" at the same time.

##### 6.1.2 Save data to the in-memory table
```
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

To append data to an in-memory table, in addition to using `insert into`, you can also use `append!`, which accepts a table as a parameter and creates an instance reference to the table:
```
table = createDemoTable();
script += "t.append!(table);";
```
#### 6.2 Table on local disk

##### 6.2.1 Create a local disk table with DolphinDB script
You can use any DolphinDB client (GUI, Web notebook or console) to create a table on local disk:
```
t = table(100:0, `name`date`price, [STRING, DATE, DOUBLE]); // create an in-memory table
db=database("/home/dolphindb/demoDB"); // create database "demoDB"
saveTable(db,t,`dt); // save the in-memory table to the database
share t as tDiskGlobal
```
`database`: create a local database at the specified path.
`saveTable`: save the in-memory table to the local database on disk.

##### 6.2.2 Save data to a local disk table with API
```
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
`loadTable`: load a table from the database into memory.   
`append!`: append data to the table.  

Note:  
1. For a table on local disk, `append!` only adds data to an in-memory copy of the table. To save the data to disk, you must also use function `saveTable`.
2. Instead of using function `share`, you can also use function `loadTable` to load the table with the C++ API, and then `append!`. However, this method is not recommended. The reason is that function `loadTable` needs to load from the disk, which can take a long time. If there are multiple client using `loadTable`, there will be multiple copies of the table in the memory, which may cause data inconsistency.

#### 6.3 Distributed table

A distributed table in DolphinDB is stored on multiple nodes of a cluster. The following example shows how to save data to a distributed table with the C++ API.

##### 6.3.1 Create a distributed table

You can use any DolphinDB client (GUI, Web notebook or console) to create a local disk table:
```
login(`admin,`123456)
dbPath = "dfs://SAMPLE_TRDDB";
tableName = `demoTable
db = database(dbPath, VALUE, 2010.01.01..2010.01.30)
pt=db.createPartitionedTable(table(1000000:0,`name`date`price,[STRING,DATE,DOUBLE]),tableName,`date)
```
`database`: create a partitioned database with the specified partition scheme.
`createPartitionedTable`: create a distributed table.

##### 6.3.2 Save data to a distributed table

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
`append!` saves the data to a distributed table and saves to disk.

For more on the C++ API, please refer to the interface provided in the header file.

