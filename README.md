# DolphinDB C++ API
This tutorial introduces how to use DolphinDB C++ API for application development in the Linux environment. It includes the following contents:
* Compiling a project
* Executing a DolphinDB Script  
* Calling built in functions
* Uploading local objects to DolphinDB server
* Appending data to a DolphindB table

### 1、Environment setup
* linux environment；  
* g++ 6.2；（Since libDolphinDBAPI.so is compiled by g++6.2, it is recommended to use this version of the compiler to ensure ABI compatibility.）

### 2、Compiling a project
#### 2.1 Download bin file and header files
Download api-cplusplus from this git repo，including "bin" and "include" folders as the following in your project：

> bin (libDolphinDBAPI.so)  
  include (DolphinDB.h  Exceptions.h  SmartPointer.h  SysIO.h  Types.h  Util.h) 
  
#### 2.2 Compiling main.cpp
Creating a project 

Creat a directory called "project" at the same level as the bin and include folders, enter the project folder, and then create the file main.cpp, as follows:
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

#### 3.2 Executing a DolphinDB script

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

Output an int Vector：[1,3,5].

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
 
The run method above returns the result of the sum function, which accepts a Double Vector via Util::createVector(DT_DOUBLE, 3).

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

Data can be easily added to a DolphinDB table using C++ API. DolphinDB supports three types of tables, __memory table__, __local disk table__ and __distributed table__. Here's how to use the C++ API to save simulated business data to different types of tables.

#### 6.1 Memory table

The data is only stored in the memory of a data node, and the access speed is the fastest, but data will not exist after the session is closed. It is suitable for scenarios such as temporary fast calculation and temporary storage of calculation results. Since all data is stored in memory, the memory table should not be too large.

##### 6.1.1 Building a memory table
```
t = table(100:0, `name`date`price, [STRING, DATE, DOUBLE]);
share  t as tglobal
```
__table__ : create a memory table, specifying capacity, size, column name, and type;
__share__ : set the table to be visible across sessions;

##### 6.1.2 Saving data to the memory table
```
string script;
//simulate data to be stored
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
conn.upload(allnames,allcols);//upload data to server
script += "insert into tglobal values(names,dates,prices);"; //insert data to table
script += "select * from tglobal;";
TableSP table = conn.run(script); //return the updated memory table
cout<<table->getString()<<endl;
```


To add data to a memory table, in addition to using the __insert into__ syntax, you can also use __append!__, which accepts a table as a parameter and creates an instance reference to the table, as follows:
```
table = createDemoTable();
script += "t.append!(table);";
```
Attention:

>In this example, __share__ is used to set the memory table to be visible across all sessions, so multiple clients can write to table "t" at the same time. For details about method "share", please refer to DolphindB help manual.


#### 6.2 Local disk table

The data is saved on the local disk, and even after the node is shut down, it can be easily loaded into the memory after booting. Applicable to data that is not particularly large and needs to be persisted to a local disk. The following example shows how the local disk table "tglobal" has been created and how to save data through the C++ API.

##### 6.2.1 Creating a local disk table using DolphinDB script
You can any DolphinDB clients（GUI、Web notebook、console）to create a local disk table as follows:
```
t = table(100:0, `name`date`price, [STRING, DATE, DOUBLE]); //Create a memory table
db=database("/home/dolphindb/demoDB"); //create database "demoDB"
saveTable(db,t,`dt); // save memory table to the database
share t as tDiskGlobal
```
__database__ Accept a local path and create a local database;
__saveTable__ Save the memory table to the local database and save it on disk;

##### 6.2.2 Saving data to a local disk table through API
```
TableSP table = createDemoTable();
conn.upload("mt",table);
string script;
script += "db=database(\"/home/psui/demoTable1\");";
script += "tDiskGlobal.append!(mt);";
script += "saveTable(db,tDiskGlobal,`dt);";
script += "select * from tDiskGlobal;";
TableSP result = conn.run(script); //return a memory table loading from the disk table. 
cout<<result->getString()<<endl;
```
__loadTable__  load a local disk table to a memory table.   
__append!__ append data to the table.  

Attention:  
>1.For local disk tables, __append!__ simply adds data to the in-memory table. To save the data to disk, you must also use the __saveTable__ function.
 2. In addition to using the share function, you can also use the loadTable function to load the table contents in the C++ API, and then append!. However, this method is not recommended. The reason is that the function loadTable needs to read the disk, which takes a lot of time. If there are multiple client loadTables, there will be multiple copies of the table in the memory, which may cause data inconsistency.

#### 6.3 Distributed table

Using the distributed file system DFS provided by the underlying DolphinDB, the data is stored on different nodes, a query can be performed like on a local table. It is suitable for saving enterprise-level historical data, used as a data warehouse, providing functions such as query and analysis. This example shows how to save data to a distributed table through the C++ API.

##### 6.3.1 Building a distributed table

You can any DolphinDB clients（GUI、Web notebook、console）to create a local disk table as follows:
```
login(`admin,`123456)
dbPath = "dfs://SAMPLE_TRDDB";
tableName = `demoTable
db = database(dbPath, VALUE, 2010.01.01..2010.01.30)
pt=db.createPartitionedTable(table(1000000:0,`name`date`price,[STRING,DATE,DOUBLE]),tableName,`date)
```
__database__ create a partitioned database and specify the partition type.
__createPartitionedTable__ create a distributed table and specify the table type and partition fields.

##### 6.3.2 Saving data to a distributed table

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
__append!__ Save the data to a distributed table and save to disk;


For more on the C++ API, please refer to the interface provided in the header file.

