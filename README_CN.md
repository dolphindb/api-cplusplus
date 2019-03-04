### 1.C++ API 概念

C++ API本质上实现了C++程序和DolphinDB服务器之间的消息传递和数据转换协议。

#### 1.1.Linux下编译执行

C++ API需要用g++ 6.2，若遇到`undefined reference std::__cxx11::basic_string`，需要在编译时使用`-D_GLIBCXX_USE_CXX11_ABI=0`宏定义。

依赖: libpthread, libssl, libuuid, libDolphinDBAPI

```
g++ example.cpp -DLINUX -DLOGGING_LEVEL_2 -O3 -I./include -L./bin/linux_64 -lDolphinDBAPI -lpthread -uuid -ssl -o example
```

#### 1.2.Windows Visual Studio工程

导入bin\vs2017_x64\libDolphinDBAPI.lib，并配置附加库。

注意：由于VS里默认定义了min/max两个宏，与头文件中`min`、`max`函数冲突。为了解决这个问题，在预处理宏定义中需要加入 `__NOMINMAX__` 。

将对应的libDolphinDBAPI.dll拷贝到可执行程序的输出目录，即可运行。

#### 1.3.C++对象和DolphinDB对象之间的映射

C++ API使用ConstantSP来指向DolphinDB返回的所有数据类型。根据DolphinDB的数据类型，C++ API主要提供了6种类，分别是Constant，Vector，Matrix，Set，Dictionary，Table。这些类的声明都包含在DolphinDB.h头文件中。

#### 1.4.C++ API提供的主要函数

DolphinDB C++ API提供的最核心的对象是DBConnection，它主要的功能就是让C++应用可以通过它调用DolphinDB的脚本和函数，在C++应用和DolphinDB服务器之间互通数据。
DBConnection类提供如下主要方法:

| 方法名        | 详情          |
|:------------- |:-------------|
|connect(host, port, [username, password])|将会话连接到DolphinDB服务器|
|login(username,password,enableEncryption)|登陆服务器|
|run(script)|将脚本在DolphinDB服务器运行|
|run(functionName,args)|调用DolphinDB服务器上的函数|
|upload(variableNames, variableValues)|将本地数据对象上传到DolphinDB服务器|
|close()|关闭当前会话|
|initialize()|静态方法，需要在使用其他方法前首先调用一次|

**如果脚本含有错误或者出现网络问题，抛出`IOException`（定义在Exception.h头文件中）**

**以下所有代码均在`example.cpp`中，建议在阅读过程中参考源代码**

### 2.建立DolphinDB连接

C++ API通过TCP/IP协议连接到DolphinDB服务器。 在下列例子中，我们连接正在运行的端口号为8848的本地DolphinDB服务器：

```
DBConnection conn;
DBConnection::initialize();
bool success = conn.connect("localhost", 8848);
```
使用用户名和密码建立连接：

```
bool success = conn.connect("localhost", 8848, "admin", "123456");
```

### 3.运行脚本

在C++中运行DolphinDB脚本的语法如下：
```
conn.run("script");
```
其中，脚本的最大长度为65,535字节。

如果脚本只包含一条语句，如表达式，DolphinDB会返回一个数据对象；否则返回NULL对象。如果脚本包含多条语句，将返回最后一个对象。

### 4.调用DolphinDB函数

调用的函数可以是内置函数或用户自定义函数。 下面的示例将一个`double`向量传递给服务器，并调用sum函数。
使用`Util::create`（定义在Util头文件中）来构建DolphinDB的各种数据类型，如下代码中预留的size是2,因此可以直接用`setDouble(idx, value)`进行设置值，此后也可以通过`appendDouble(data, length)`直接批量添加原生数组或STL容器内的数据。

```
void testCallFunction(DBConnection &conn) {
    VectorSP vec = Util::createVector(DT_DOUBLE, 2);
    double data[] = {3.5, 4.5};
    vec->setDouble(0, 1.5);
    vec->setDouble(1, 2.5);
    vec->appendDouble(data, 2);

    std::vector<ConstantSP> args{vec};
    ConstantSP result = conn.run("sum", args);
    std::cout << result->getDouble() << std::endl;;
}
```

### 5.将对象上传到DolphinDB服务器

我们可以将二进制数据对象上传到DolphinDB服务器，并将其分配给一个变量以备将来使用。变量名称可以使用三种类型的字符：字母，数字或下划线；第一个字符必须是字母。

```
void testUpload(DBConnection &conn) {
    ConstantSP vec = Util::createVector(DT_DOUBLE, 0);
    double data[] = {1.5, 2.5, 3.5, 4.5};
    ((Vector *)vec.get())->appendDouble(data, 4);

    std::vector<std::string> vars{"a"};
    std::vector<ConstantSP> args{vec};
    conn.upload(vars, args);
    ConstantSP result = conn.run("accumulate(+,a)");
    std::cout << result->getString() << std::endl;
}
```

### 6.DolphinDB读取数据结构实例

下面介绍建立DolphinDB连接后，在C++环境中，对不同DolphinDB数据类型进行操作。

首先包含DolphinDB的头文件：

```
#include "DolphinDB.h"
#include "Util.h"
```

- 向量Vector

在下面的示例中，DolphinDB语句:
```
rand(`IBM`MSFT`GOOG`BIDU,10)
```
返回C++对象`ConstantSP`，`size()`方法能够获取向量的大小。我们可以使用`getString(i)`方法按照索引访问向量元素。

```
void testStringVector(DBConnection &conn) {
    ConstantSP vector = conn.run("rand(`IBM`MSFT`GOOG`BIDU, 10)");

    int size = vector->size();
    for(int i = 0; i < size; ++i) {
        std::cout << vector->getString(i) << " ";
    }
    std::cout << std::endl;
}
```

类似的，也可以处理双精度浮点类型的向量或者其他类型的向量。

```
void testDoubleVector(DBConnection &conn) {
    ConstantSP vector = conn.run("rand(10.0, 10)");

    int size = vector->size();
    for(int i = 0; i < size; ++i) {
        std::cout << vector->getDouble(i) << " ";
    }
    std::cout << std::endl;
}
```

- 集合Set

```
void testIntSet(DBConnection &conn) {
    ConstantSP set = conn.run("set(1+3*1..10)");
    std::cout << set->getString() << std::endl;
}
```

- 矩阵Matrix

`Matrix`不继承自`Constant`，用`get(idx)`来获取存储的元素。

```
void testIntMatrix(DBConnection &conn) {
    ConstantSP matrix = conn.run("1..6$3:2");
    std::cout << matrix->getString() << std::endl;
}
```

- 字典Dictionary

用函数`keys()`和`values()`可以从字典取得所有的键和值；要从一个键里取得它的值，可以调用`get(key)`。

```
void testDictionary(DBConnection &conn) {
    ConstantSP dict = conn.run("dict(1 2 3,`IBM`MSFT`GOOG)");
    std::cout << dict->getString() << std::endl;
}
```


- 表Table

要获取表的列，我们可以调用`getColumn(index)`；同样，我们可以调用`getColumnName(index)`获取列名。 对于列和行的数量，我们可以分别调用`columns()`和`rows()`。

```
void testTable(DBConnection &conn) {
    string script;
    script += "n=2000\n";
    script += "syms=`IBM`C`MS`MSFT`JPM`ORCL\n";
    script += "mytrades=table(09:30:00+rand(18000,n) as timestamp,rand(syms, n) as sym, 10*(1+rand(100,n)) as qty,5.0+rand(100.0,n) as price)\n";
    script += "select qty,price,sym from mytrades";
    ConstantSP table = conn.run(script);
    std::cout << table->getString() << std::endl;
}
```

当用户在C++程序中取到的值已经组成`std::vector`时，也可以很方便的构造出`Table`用于追加数据，比如现在已有`cbool, cint, cdouble, cdate, cstring`5个向量，可以通过以下语句构造`Table`对象。

```
void testCreateTable(DBConnection &conn) {
    std::vector<char> cbool{1,0,1};
    std::vector<int> cint{29,37,INT32_MIN};
    std::vector<double> cdouble{4.9,1.2,7.7};

    std::vector<std::string> colNames = {"cbool", "cint", "cdouble"};
    std::vector<ConstantSP> cols;
    cols.push_back(Util::createVector(DT_BOOL, 3));
    cols[0]->setBool(0, 3, cbool.data());
    cols.push_back(Util::createVector(DT_INT, 3));
    cols[1]->setInt(0, 3, cint.data());
    cols.push_back(Util::createVector(DT_DOUBLE, 3));
    cols[2]->setDouble(0, 3, cdouble.data());
    ConstantSP table = Util::createTable(colNames, cols);
}
```

- Void和null

对具体类型可以用`setNull()`将ConstantSP所指对象置为null。

```
void testVoid(DBConnection &conn) {
    ConstantSP null = conn.run("NULL");
    std::cout << null->getType() << std::endl;
}

void testIntNull(DBConnection &conn) {
    ConstantSP null = conn.run("int(1)");
    null->setNull();
    std::cout << null->getInt() << std::endl;
}
```

### 7.如何将C++数据表对象保存到DolphinDB的数据库中

使用C++ API的一个重要场景是，用户从其他数据库系统或是第三方API中取到数据，将数据进行清洗后存入DolphinDB数据库中，本节将介绍通过C++ API将取到的数据上传并保存到DolphinDB的数据表中。

DolphinDB数据表按存储方式分为三种:

- 内存表: 数据仅保存在本节点内存，存取速度最快，但是节点关闭数据就不存在了。
- 本地磁盘表：数据保存在本地磁盘上，即使节点关闭，通过脚本就可以方便的从磁盘加载到内存。
- 分布式表：数据在物理上分布在不同的节点，通过DolphinDB的分布式计算引擎，逻辑上仍然可以像本地表一样做统一查询。

下面分三部分介绍内存表数据追加及本地磁盘和分布式表的数据追加。

#### 7.1.将数据保存到DolphinDB内存表

DolphinDB提供多种方式来保存数据，分别对应以下几个场景：
- 保存单点数据： 通过`insert into`方式保存单点数据；
- 保存批量数据： 通过`tableInsert`函数保存多个数组对象；
- 保存批量数据： 通过`append!`函数保存表对象。

这三种方式的区别是接收的参数类型不同，具体业务场景中，可能从数据源取到的是单点数据，也可能是多个数组或者表的方式组成的数据集。

下面分别介绍三种方式保存数据的实例，在例子中使用到的数据表有4个列，分别是`string,int,timestamp,double`类型，列名分别为`cstring,cint,ctimestamp,cdouble`，在DolphinDB上首先需要运行如下构建脚本：
```
login("admin","123456")
t = table(10000:0,`cstring`cint`ctimestamp`cdouble,[STRING,INT,TIMESTAMP,DOUBLE])
share t as sharedTable
```
由于内存表是会话隔离的，所以GUI中创建的内存表只有GUI会话可见，如果需要在C++或者其他终端查看数据，需要通过share关键字在会话间共享内存表。

##### 7.1.1.保存单点数据

若C++程序是每次获取单条数据记录保存到DolphinDB，那么可以通过类似SQL语句的`insert into`的方式保存数据，缺点是需要自行拼接字符串。
```
std::string s = "'test1'";
int i = 1;
long l = 10;
double d = 11.0;
std::string script;
script += "insert into sharedTable values(";
script += s + "," + std::to_string(i) + "," + std::to_string(l) + "," + std::to_string(d) +")";
conn.run(script);
```

##### 7.1.2.使用多个数组方式批量保存

若C++程序获取的数据易于组织成向量，使用`tableInsert`函数是一个比较适合的保存方式，这个函数可以接受多个数组作为参数，将数组追加到数据表中。

```
ConstantSP col0 = Util::createVector(DT_STRING, 1);       
col0->setString(0, "test2");
ConstantSP col1 = Util::createVector(DT_INT, 1);
col1->setInt(0, 2);
ConstantSP col2 = Util::createVector(DT_TIMESTAMP, 1);
col2->setLong(0, 20);
ConstantSP col3 = Util::createVector(DT_DOUBLE, 1);
col3->setDouble(0, 22.0);

vector<ConstantSP> cols{col0, col1, col2, col3};            //预先已组织好的向量直接构成参数
ConstantSP result1 = conn.run("tableInsert{sharedTable}", cols);
```

实际运用的场景中，通常是C++程序往服务端已经存在的表中写入数据，在服务端可以用 `tableInsert(sharedTable,cols)` 这样的脚本，但是在C++里用 `conn.run("tableInsert",args)` 方式调用时，tableInsert的第一个参数是服务端表的对象引用，它无法在C++程序端获取到，所以常规的做法是在预先在服务端定义一个函数，把sharedTable固化的函数体内，比如：

```
def saveData(v1,v2,v3,v4){tableInsert(sharedTable,v1,v2,v3,v4)}
```

然后再通过`conn.run("saveData",args)`运行函数，虽然这样也能实现目标，但是对C++程序来说要多一次服务端的调用，多消耗了网络资源。
在本例中，使用了DolphinDB中的`部分应用`这一特性，将服务端表名以`tableInsert{sharedTable}`这样的方式固化到tableInsert中，作为一个独立函数来使用，这样就不需要再使用自定义函数的方式来实现。
具体的文档请参考[部分应用](https://www.dolphindb.com/cn/help/PartialApplication.html)。

##### 7.1.3.使用表方式批量保存

若C++程序利用`Util::createTable`构建表再保存，那么使用`append!`函数会更加方便，`append!`函数接受一个表对象作为参数，将数据追加到数据表中。

```
std::vector<std::string> cstring{"rec1", "rec2", "rec3"};
std::vector<int> cint{1,0,1};
std::vector<long long> ctimestamp{59603423,59604235,INT64_MIN};
std::vector<double> cdouble{4.9,1.2,7.7};

std::vector<std::string> colNames = {"cstring", "cint", "ctimestamp", "cdouble"};
std::vector<ConstantSP> cols;
cols.push_back(Util::createVector(DT_STRING, 3));
cols[0]->setBool(0, 3, cstring.data());
cols.push_back(Util::createVector(DT_INT, 3));
cols[1]->setInt(0, 3, cint.data());
cols.push_back(Util::createVector(DT_TIMESTAMP, 3));
cols[2]->setLong(0, 3, ctimestamp.data());
cols.push_back(Util::createVector(DT_DOUBLE, 3));
cols[3]->setDouble(0, 3, cdouble.data());
ConstantSP myTable = Util::createTable(colNames, cols);

std::vector<ConstantSP> arg{myTable};
ConstantSP result2 = conn.run("append!{sharedTable}", arg); //调用自定义函数来append!
```

#### 7.2.将数据保存到分布式表

分布式表是DolphinDB推荐在生产环境下使用的数据存储方式，它支持快照级别的事务隔离，保证数据一致性; 分布式表支持多副本机制，既提供了数据容错能力，又能作为数据访问的负载均衡。

本例中涉及到的数据表可以通过如下脚本构建 ：

*请注意只有启用 `enableDFS=1` 的集群环境才能使用分布式表。*


```
login("admin","123456")
//使用分布式表
dbPath = 'dfs://testDatabase'
tbName = 'tb1'

t = table(10000:0,`cstring`cint`ctimestamp`cdouble,[STRING,INT,TIMESTAMP,DOUBLE])
share t as sharedTable

if(existsDatabase(dbPath)) { dropDatabase(dbPath) }
db = database(dbPath,RANGE,2018.01.01..2018.12.31)
db.createPartitionedTable(t,tbName,'ctimestamp')
```

DolphinDB提供`loadTable`方法可以加载分布式表，通过`append!`方式追加数据，具体的脚本示例如下：

```
std::vector<std::string> cstring{"rec4", "rec5", "rec6"};
std::vector<int> cint{123,750,41};
std::vector<long long> ctimestamp{59603423,59604235,98604937};
std::vector<double> cdouble{4。0,0.2,73.7};

std::vector<std::string> colNames = {"cstring", "cint", "ctimestamp", "cdouble"};
std::vector<ConstantSP> cols;
cols.push_back(Util::createVector(DT_STRING, 3));
cols[0]->setBool(0, 3, cstring.data());
cols.push_back(Util::createVector(DT_INT, 3));
cols[1]->setInt(0, 3, cint.data());
cols.push_back(Util::createVector(DT_TIMESTAMP, 3));
cols[2]->setLong(0, 3, ctimestamp.data());
cols.push_back(Util::createVector(DT_DOUBLE, 3));
cols[3]->setDouble(0, 3, cdouble.data());
ConstantSP myTable = Util::createTable(colNames, cols);

vector<ConstantSP> args{myTable};
conn.run("append!{loadTable('dfs://testDatabase','tb1')}", args);
```

#### 7.3.将数据保存到本地磁盘表

通常本地磁盘表用于学习环境或者单机静态数据集测试，它不支持事务，不保证运行中的数据一致性，所以不建议在生产环境中使用。

```
//使用本地磁盘表
dbPath = "C:/data/testDatabase"
tbName = 'tb1'

t = table(10000:0,`cstring`cint`ctimestamp`cdouble,[STRING,INT,TIMESTAMP,DOUBLE])
share t as sharedTable

if(existsDatabase(dbPath)){dropDatabase(dbPath)}
db = database(dbPath,RANGE,2018.01.01..2018.12.31)
db.createPartitionedTable(t,tbName,'ctimestamp')
```

DolphinDB提供`loadTable`方法可以加载本地磁盘表和分布式表，对于本地磁盘表而言，追加数据都是通过`append!`方式进行。

```
std::vector<std::string> cstring{"rec4", "rec5", "rec6"};
std::vector<int> cint{123,750,41};
std::vector<long long> ctimestamp{59603423,59604235,98604937};
std::vector<double> cdouble{4。0,0.2,73.7};

std::vector<std::string> colNames = {"cstring", "cint", "ctimestamp", "cdouble"};
std::vector<ConstantSP> cols;
cols.push_back(Util::createVector(DT_STRING, 3));
cols[0]->setBool(0, 3, cstring.data());
cols.push_back(Util::createVector(DT_INT, 3));
cols[1]->setInt(0, 3, cint.data());
cols.push_back(Util::createVector(DT_TIMESTAMP, 3));
cols[2]->setLong(0, 3, ctimestamp.data());
cols.push_back(Util::createVector(DT_DOUBLE, 3));
cols[3]->setDouble(0, 3, cdouble.data());
ConstantSP myTable = Util::createTable(colNames, cols);

vector<ConstantSP> args{myTable};
conn.run("append!{loadTable('C:/data/testDatabase','tb1')}", args);
```

#### 7.4.读取和使用数据

在C++ API中，表数据保存为`Table`对象，由于`Table`是列式存储，因此需要通过先取出列，再循环取行的方式。

例子中的表`sharedTable`的有4个列，分别是`string,int,timestamp,double`类型，列名分别为`cstring,cint,ctimestamp,cdouble`。

```
void testUseTable(DBConnection &conn) {
    ConstantSP table = conn.run("select * from sharedTable");
    vector<ConstantSP> cols;
    for(int i = 0; i < table->columns(); ++i) {
        cols.emplace_back(table->getColumn(i));
    }
    for(int row = 0; row < table->rows(); ++row) {
        std::cout << "row " << row << ":";
        for(int col = 0; col < cols.size(); ++col) {
            std::cout << " " << cols[col]->get(row)->getString();
        }
        std::cout << std::endl;
    }
}
```

### 8.DolphinDB和C++ API之间的数据类型转换

一些C++的基础类型，可以直接在DolphinDB的数据类型中使用，比如`setInt(5)`，`setString("abc")`，部分C++ STL容器对应C++ API中提供的Vector,Set,Dictionary等容器，可以通过Util::create+`<DataType>`这种方式来创建，比如`Util::createVector(DT_INT,0)`就创建了存储int的向量，初始元素个数为0；但是也有一些类型需要做一些转换，下面列出需要做简单转换的类型：

- 时间类型：DolphinDB的时间类型内部是以`int`或者`long long`来描述的，DolphinDB提供`date, month, time, minute, second, datetime, timestamp, nanotime, nanotimestamp`九种类型的时间类型，最高精度可以到纳秒级。具体的描述可以参考[DolphinDB时序类型和转换](https://www.dolphindb.com/cn/help/TemporalTypeandConversion.html)。C++ API在Util类里里提供了创建所有DolphinDB时间类型的静态方法，对于具体时间类型的`ConstantSP`，直接用`getInt()`或`getLong()`就可以获得对应的`int`或`long long`值。

以下展示C++ API中DolphinDB时间类型的构建以及与`int`和`long`的关系：

```
void testTimeTypes(DBConnection &conn) {
    ConstantSP date = Util::createDate(1970, 1, 2);                             //1970.01.02
    TEST("testDate", date->getInt(), 1);
    
    ConstantSP month = Util::createMonth(2018, 1);                              //2018.01M
    TEST("testMonth", month->getInt(), 2018*12);
    
    ConstantSP time = Util::createTime(1, 1, 1, 1);                             //01:01:01:001
    TEST("testTime", time->getInt(), 3600000+60000+1000+1);
    
    ConstantSP minute = Util::createMinute(2, 30);                              //02:30m
    TEST("testMinute", minute->getInt(), 2*60+30);
    
    ConstantSP second = Util::createSecond(5,6,7);                              //05:06:07
    TEST("testSecond", second->getInt(), 5*3600+6*60+7);
    
    ConstantSP datetime = Util::createDateTime(1970, 1, 1, 1, 1, 1);            //1970.01.01T01:01:01
    TEST("testDatetime", datetime->getInt(),3600+60+1);
    
    ConstantSP timestamp = Util::createTimestamp(1970, 1, 1, 1, 1, 1, 123);     //1970.01.01T01:01:01.123
    TEST("testTimestamp", timestamp->getLong(), (long long)3600000+60000+1000+123);
    
    ConstantSP nanotime = Util::createNanoTime(0, 0, 1, 123);                   //00:00:01.000000123
    TEST("testNanotime", nanotime->getLong(), (long long)1000000123);
    
    ConstantSP nanotimestamp = Util::createNanoTimestamp(1970,1,1,0,0,0,567);   //1970.01.01T00:00:00.000000567
    TEST("testNanotimestamp", nanotimestamp->getLong(), (long long)567);
}
```