# DolphinDB C++ API例子

### 1. 概述

目前已有3个C++ API的例子，如下表所示，3个例子的代码分别位于3个子目录下：

| 例子主题        | 子目录          |
|:-------------- |:-------------|
|数据库写入|[DFSWriting](./DFSWriting)|
|多线程并行写入数据库|[DFSWritingWithMultiThread](./DFSWritingWithMultiThread)|
|流数据写入和订阅|[streamingData](./streamingData/)|

这些例子的开发环境详见[DolphinDB C++ API](https://github.com/dolphindb/api-cplusplus/blob/master/README_CN.md)。下面对每个例子分别进行简单的说明。

### 2. 数据库写入例子

本例实现了用单线程往分布式数据库写入数据的功能。例子中的目标数据库是一个按日期分区的分布式数据库

#### 2.1 代码说明

本例子目录下包含3个文件，各文件说明如下：

* [DFSWriting.cpp](./DFSWriting/DFSWriting.cpp)：源代码文件
* [CreateDB.dos](./DFSWriting/CreateDB.dos)：脚本文件，用于创建例子用到的分布式数据库
* [Makefile](./DFSWriting/Makefile)：makefile文件，用于编译和链接例子程序


数据每写入一次都有网络传输、磁盘IO等固定的开销，批量写入并在延迟允许的情况下尽量增加每次写入的数据记录数，能提升写入性能，提高系统吞吐量。比如以写入10万条记录为例，若一条条写入，需要网络来回10万次，若10万条一批写入，一个来回就结束了。所以例子中使用了[tableInsert](https://www.dolphindb.cn/cn/help/tableInsert.html)往表中批量插入数据。在源代码中，主要有3个函数，各函数说明如下：

* `createDemoTable1`函数，用于产生模拟数据，该函数创建了一个本地的表对象,函数返回值是TableSP。
* `createDemoTable2`函数，也用于产生模拟数据，该函数与`createDemoTable1`不同的是，该函数先创建一个本地的表对象，并对每列定义一个相应类型的数组，然后通过诸如“setInt(INDEX start, int len, const int* buf)”的方式一次或者多次地将数据批量传给列向量。具体地说就是先采用诸如`getIntBuffer`等方法直接获取一个可读写的缓冲区，写完后再使用`setInt`等方法将缓冲区写回数组。`setInt`这类函数会检查给定的缓冲区地址和变量底层储存的地址是否一致，如果一致就不会发生数据拷贝。在多数情况下，用`getIntBuffer`函数获得的缓冲区就是变量实际的存储区域，因此能减少数据拷贝，提高性能。当表对象的数据量较小时，可以采用`createDemoTable1`的方式生成TableSP对象的数据，但是当数据量较多时，建议采用`createDemoTable2`来生成数据，以提高数据生成的效率。
* `main`函数，其中先与DolphinDB server建立连接，然后用`run`函数运行tableInsert脚本把`createDemoTable1`或`createDemoTable2`产生的模拟数据写入数据库。这里要注意的是，[append!](https://www.dolphindb.cn/cn/help/append1.html)函数也能向分布式表追加数据，但是性能与 [tableInsert](https://www.dolphindb.cn/cn/help/tableInsert.html)相比要差，所以建议使用`tableInsert`。

这些函数参照[DolphinDB C++ API](https://github.com/dolphindb/api-cplusplus/blob/master/README_CN.md)的说明实现，包括如何连接DolphinDB服务器，如何创建本地表对象并产生模拟数据，如何使用[tableInsert](https://www.dolphindb.cn/cn/help/tableInsert.html)函数批量写入分布式数据库等等。若想了解具体实现原理请参阅此[教程](https://github.com/dolphindb/api-cplusplus/blob/master/README_CN.md)。

#### 2.2 编译

在子目录下运行make命令即可生成可执行程序。

#### 2.3 运行

在运行前，需要先在DolphinDB上创建数据库，创建数据库的脚本详见[CreateDB.dos](./DFSWriting/CreateDB.dos)。然后在子目录下运行DFSWriting，示例如下：

```
./DFSWriting   
```

其中DolphinDB的数据节点IP，数据节点的端口号，每批次插入数据的记录数，插入批数需要在源代码main函数中赋值，目前默认值如下：
```
  string host = "127.0.0.1";
  int port = 8848;
  long rows = 100000;
  long batches = 1000;
```

### 3. 数据库多线程并行写入例子

#### 3.1 代码说明

本例实现了多线程往分布式数据库写入数据的功能。本例的子目录下包含3个文件，各文件说明如下：

* [MultiThreadDfsWriting.cpp](./DFSWritingWithMultiThread/MultiThreadDfsWriting.cpp)：源代码文件
* [CreateDatabase.dos](./DFSWritingWithMultiThread/CreateDatabase.dos)：脚本文件，用于创建例子用到的分布式数据库
* [Makefile](./DFSWritingWithMultiThread/Makefile)：makefile文件，用于编译和链接例子程序

DolphinDB采用数据分区技术，按照一定的规则将大规模数据集水平分区，每个分区内的数据采用列式存储。写入由事务机制保证数据原子性，通常，每次写入会涉及到多个分区，这样多个分区同时参与某一个事务。影响写入吞吐量的主要因素包括：

* 批量写入，每次写入数据量不宜过小，一般每次数据量写入在几十兆字节比较合适。
* 每次写入数据涉及的分区数据量不宜过多。DolphinDB按照分区来列式存储，比如每次写入1000分区10列，那么每次需要写10000(1000*10)个文件，会降低写入性能。
* 开启写入缓存（Cache Engine）可以提升写入效率。
* 并行写入可以提升吞吐量，但不同的线程要保证写不同的分区，否则会写入失败。

实际生产环境中，通过API单线程写入到分布式数据，如果上述提到的影响写入吞吐量的几个主要因素都考虑到了，但写入吞吐量还是不能满足业务需求，就可以通过多线程并行写入来提升性能。但要保证，同一个分区不能同时参与多个事务，也就是不同的线程要负责写不同的分区。

本例为每个写入线程创建一个生产数据的线程，两者之间通过阻塞队列BoundedBlockingQueue传递数据，保证数据消费一个后再生产一个，有序进行，不过多占用系统资源。在写入时要注意的是，多个wirter不能同时往DolphinDB分布式数据库的同一个分区写数据，所以产生数据时，保证每个线程产生的数据是不同分区的。在本例中分布式数据库第一层按时间分区，第二层是按IP地址分50个HASH分区。通过为每个写入线程平均分配分区的方法（比如10个线程，50个分区，则线程1写入1-5，线程2写入6-10，其他线程依次类推），保证多个写入线程写到不同的分区。其中每个IP地址的hash值是通过API内置的`getHash`函数计算的：得到相同的hash值，说明数据属于相同分区，反之属于不同分区。在源代码中，主要有4个函数，各函数说明如下：

* `createDemoTable`函数，用于产生模拟数据，返回TableSP。
* `genData`函数，生产数据线程的主函数。
* `writeData`函数，写数据线程的主函数，与DolphinDB server建立连接，用`run`函数运行tableInsert脚本把模拟数据写入数据库。这里要注意的是，[append!](https://www.dolphindb.cn/cn/help/append1.html)函数也能向分布式表追加数据，但是性能与[tableInsert](https://www.dolphindb.cn/cn/help/tableInsert.html)相比要差，所以建议使用`tableInsert`。
* `main`函数，接收输入参数，创建线程，显示写入的结果。

注意：若分布式数据库不是HASH分区，可以通过如下方式确保不同的线程写不同的分区：

* 若采用了范围（RANGE）分区，可以先在server端执行函数schema(database(dbName)).partitionSchema[1]获取到分区字段的分区边界（partitionSchema取第一个元素的前提是一般数据库采用两层分区，第一层是日期，第二层是设备或股票进行范围分区）。然后对比数据的分区字段的取值和分区的边界值，控制不同的线程负责不同的1个或多个分区。
* 对于分区类型为值（VALUE）分区、列表（LIST）分区，用值比较的方法可以判定数据所属的分区。然后不同的线程负责写1个或多个不同分区。

#### 3.2 编译

在子目录下运行make命令即可生成可执行程序。

#### 3.3 运行

在运行前，需要先在DolphinDB上创建数据库，创建数据库的脚本详见[CreateDatabase.dos](./DFSWritingWithMultiThread/CreateDatabase.dos)。然后在子目录下运行MultiThreadDfsWriting，示例如下：

```
./MultiThreadDfsWriting --h=192.168.1.12,192.168.1.13,192.168.1.12,192.168.1.13 --p=19162,19162,19163,19163 --c=100000 --n=100  --t=8
```

其中参数h是DolphinDB的数据节点IP，p是数据节点的端口号，c是每批次插入数据的记录数，n是插入批数，t是线程数。程序会依次把线程分配到配置的数据节点上。

### 4. 流数据写入和订阅例子

#### 4.1 代码说明

本例实现了多线程写入流数据表和流数据订阅的功能。本例的子目录下包含4个文件，文件说明如下：

* [StreamingDataWriting.cpp](./streamingData/StreamingDataWriting.cpp)：流数据写入的源代码文件
* [StreamingThreadClientSubscriber.cpp](./streamingData/StreamingThreadClientSubscriber.cpp)：流数据订阅的源代码文件
* [CreateStreamingTable.dos](./streamingData/CreateStreamingTable.dos)：脚本文件，用于创建例子用到的流数据表等
* [Makefile](./streamingData/Makefile)：makefile文件，用于编译和链接例子程序

本例多线程写入流表的实现与分布式数据库多线程写入的例子非常相似，不同的地方主要有两点，一是流表与分布式数据库不同，流表的数据类型需要用户保证一致，不会自动转换；二是流表与分布式数据库不同，流表没有分区概念，所以多线程可以无限制地同时写入。如前文所述，多线程和批量写入能显著提高吞吐量和写入性能，建议实际环境中采用多线程并批量写入流数据。在流数据写入的源代码中，主要有4个函数，各函数说明如下：

* `createDemoTable`函数，用于产生模拟数据，返回TableSP。
* `genData`函数，生产数据线程的主函数。
* `writeData`函数，写数据线程的主函数，与DolphinDB server建立连接，用`run`函数运行tableInsert脚本把模拟数据写入流表。
* `main`函数，接收输入参数，创建线程，显示写入的结果。

API提供了ThreadedClient、ThreadPooledClient和PollingClient三种订阅模式订阅流表的数据。三种模式的主要区别在于收取数据的方式。ThreadedClient单线程执行，并且对收到的消息直接执行用户定义的handler函数进行处理；ThreadPooledClient多线程执行，对收到的消息进行多线程并行调用用户定义的handler函数进行处理；PollingClient返回一个消息队列，用户可以通过轮寻的方式获取和处理数据。在本例流数据订阅的源代码中，用了ThreadedClient类，handler处理函数中是把订阅的消息写入到另一个表中。

#### 4.2 编译

在子目录下运行make命令即可生成可执行程序。

#### 4.3 运行

使用流数据需要先配置发布节点和订阅节点，详见
[DolphinDB 流数据教程](https://github.com/dolphindb/Tutorials_CN/blob/master/streaming_tutorial.md)。然后在DolphinDB上创建流数据表和数据库，相关脚本详见[CreateStreamingTable.dos](./streamingData/CreateStreamingTable.dos)。写入流表在子目录下运行StreamingDataWriting，例子如下：

```
./StreamingDataWriting --h=192.168.1.12,192.168.1.13,192.168.1.12,192.168.1.13 --p=19162,19162,19163,19163 --c=100000 --n=100  --t=4
```

其中参数h是DolphinDB的数据节点IP，p是数据节点的端口号，c是每批次插入数据的记录数，n是插入批数，t是线程数。程序会依次把线程分配到配置的数据节点上。

流数据订阅在子目录下运行StreamingThreadClentSubsciber。目前订阅的发布节点的IP和port以及其他参数都在源代码中指定。
