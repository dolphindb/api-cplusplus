#include "config.h"

class StreamingDeserilizerTester : public ::testing::Test
{
public:
    // Suite
    static void SetUpTestCase()
    {
        // DBConnection conn;
        conn.initialize();
        bool ret = conn.connect(hostName, port, "admin", "123456");
        if (!ret)
        {
            cout << "Failed to connect to the server" << endl;
        }
        else
        {
            cout << "connect to " + hostName + ":" + std::to_string(port) << endl;
        }
    }
    static void TearDownTestCase()
    {
        usedPorts.clear();
        conn.close();
    }

    // Case
    virtual void SetUp()
    {
        cout << "check connect...";
        try
        {
            ConstantSP res = conn.run("1+1");
        }
        catch (const std::exception &e)
        {
            conn.connect(hostName, port, "admin", "123456");
        }

        cout << "ok" << endl;
        CLEAR_ENV(conn);
    }
    virtual void TearDown()
    {
        CLEAR_ENV(conn);
    }
};

class StreamingDeserilizerTester_basic : public StreamingDeserilizerTester, public ::testing::WithParamInterface<int>
{
};

StreamDeserializerSP createStreamDeserializer(const string &st)
{
    string script = "login(\"admin\",\"123456\")\n\
            st2 = streamTable(1:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n\
            enableTableShareAndPersistence(table=st2, tableName=`"+st+", asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\n\
            go\n\
            setStreamTableFilterColumn("+st+", `sym)";
    conn.run(script);

    string replayScript = "n = 1000;\
            table1_SDT = table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);\
            share table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]) as res1_SDT;\
            table2_SDT = table(1:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);\
            share table(1:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]) as res2_SDT;\
            go;\
            tableInsert(table1_SDT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));\
            tableInsert(table2_SDT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));\
            share table1_SDT as ex1_SDT;\
            share table2_SDT as ex2_SDT;\
            go;\
            d = dict(['msg1','msg2'], [table1_SDT, table2_SDT]);\
            replay(inputTables=d, outputTables=`"+st+", dateColumn=`timestampv, timeColumn=`timestampv)";
    conn.run(replayScript);

    DictionarySP t1s = conn.run("schema(table1_SDT)");
    DictionarySP t2s = conn.run("schema(table2_SDT)");
    unordered_map<string, DictionarySP> sym2schema;
    sym2schema["msg1"] = t1s;
    sym2schema["msg2"] = t2s;
    StreamDeserializerSP sdsp = new StreamDeserializer(sym2schema);
    return sdsp;
}

StreamDeserializerSP createStreamDeserializer_2(const string &st)
{
    string script = "login(\"admin\",\"123456\")\n\
            st2 = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n\
            enableTableShareAndPersistence(table=st2, tableName=`"+st+", asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\n\
            go\n\
            setStreamTableFilterColumn("+st+", `sym)";
    conn.run(script);

    string replayScript = "n = 1000;\
            table1_SDT = table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);\
            share table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]) as res1_SDT;\
            table2_SDT = table(1:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);\
            share table(1:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]) as res2_SDT;\
            go;\
            tableInsert(table1_SDT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));\
            tableInsert(table2_SDT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));\
            share table1_SDT as ex1_SDT;\
            share table2_SDT as ex2_SDT;\
            go;\
            d = dict(['msg1','msg2'], [table1_SDT, table2_SDT]);\
            replay(inputTables=d, outputTables=`"+st+", dateColumn=`timestampv, timeColumn=`timestampv)";
    conn.run(replayScript);

    TableSP t1 = conn.run("table1_SDT");
    TableSP t2 = conn.run("table2_SDT");
    vector<DATA_TYPE> t1s;
    vector<DATA_TYPE> t2s;
    for (auto i = 0; i < t1->columns(); i++)
        t1s.emplace_back(t1->getColumnType(i));
    for (auto i = 0; i < t2->columns(); i++)
        t2s.emplace_back(t2->getColumnType(i));
    unordered_map<string, vector<DATA_TYPE>> sym2schema;
    sym2schema["msg1"] = t1s;
    sym2schema["msg2"] = t2s;
    StreamDeserializerSP sdsp = new StreamDeserializer(sym2schema);
    return sdsp;
}

StreamDeserializerSP createStreamDeserializer_3(const string &st)
{
    string script = "login(\"admin\",\"123456\")\n\
            st2 = streamTable(1:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n\
            enableTableShareAndPersistence(table=st2, tableName=`"+st+", asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\n\
            go\n\
            setStreamTableFilterColumn("+st+", `sym)";
    conn.run(script);

    string replayScript = "n = 1000;\
            t1 = table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);\
            share t1 as table1_SDT; share t1 as res1_SDT;\
            t2 = table(1:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);\
            share t2 as table2_SDT; share t2 as res2_SDT;\
            go;\
            tableInsert(table1_SDT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));\
            tableInsert(table2_SDT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));\
            share table1_SDT as ex1_SDT;\
            share table2_SDT as ex2_SDT;\
            go;\
            dbpath=\"dfs://test_"+st+"\";\
            if(existsDatabase(dbpath)){dropDatabase(dbpath)};\
            db=database(dbpath, VALUE, `a`b`c);\
            db.createPartitionedTable(t2,`table2_SDT,`sym).append!(table2_SDT);\
            d = dict(['msg1','msg2'], [table1_SDT, table2_SDT]);\
            replay(inputTables=d, outputTables=`"+st+", dateColumn=`timestampv, timeColumn=`timestampv)";
    conn.run(replayScript);

    unordered_map<string, pair<string, string>> sym2schema;
    sym2schema["msg1"] = {"", "table1_SDT"};
    sym2schema["msg2"] = {"dfs://test_"+st, "table2_SDT"};
    StreamDeserializerSP sdsp = new StreamDeserializer(sym2schema, &conn);
    return sdsp;
}

StreamDeserializerSP createStreamDeserializer_withallTypes(const string &st, const DATA_TYPE &dataType, const string &vecVal)
{
    string typeString = Util::getDataTypeString(dataType);
    if (typeString.compare(0, 9, "DECIMAL32") == 0)
        typeString = typeString.substr(0, 9) + "(8)";
    else if (typeString.compare(0, 9, "DECIMAL64") == 0)
        typeString = typeString.substr(0, 9) + "(15)";
    else if (typeString.compare(0, 10, "DECIMAL128") == 0)
        typeString = typeString.substr(0, 10) + "(25)";
    cout << "test type: " << typeString << endl;
    string script = "login(\"admin\",\"123456\")\n\
            st2 = streamTable(1:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB," + typeString + "])\n\
            enableTableShareAndPersistence(table=st2, tableName=`"+st+", asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\n\
            go\n\
            setStreamTableFilterColumn("+st+", `sym)";
    conn.run(script);

    string replayScript = "n = 1000;\
            table1_SDT = table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, " + typeString + ", DOUBLE]);\
            share table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, " + typeString + ", DOUBLE]) as res1_SDT;\
            table2_SDT = table(1:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, " + typeString + "]);\
            share table(1:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, " + typeString + "]) as res2_SDT;\
            go;\
            tableInsert(table1_SDT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), take(array(" + typeString + ").append!(" + vecVal + "),n), rand(100.00,n));\
            tableInsert(table2_SDT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), take(array(" + typeString + ").append!(" + vecVal + "),n));\
            share table1_SDT as ex1_SDT;\
            share table2_SDT as ex2_SDT;\
            go;\
            d = dict(['msg1','msg2'], [table1_SDT, table2_SDT]);\
            replay(inputTables=d, outputTables=`"+st+", dateColumn=`timestampv, timeColumn=`timestampv)";
    conn.run(replayScript);

    DictionarySP t1s = conn.run("schema(table1_SDT)");
    DictionarySP t2s = conn.run("schema(table2_SDT)");
    unordered_map<string, DictionarySP> sym2schema;
    sym2schema["msg1"] = t1s;
    sym2schema["msg2"] = t2s;
    StreamDeserializerSP sdsp = new StreamDeserializer(sym2schema);
    return sdsp;
}

StreamDeserializerSP createStreamDeserializer_witharrayVector(const string &st, const DATA_TYPE &dataType, const string &vecVal)
{
    string typeString = Util::getDataTypeString(dataType);
    if (typeString.compare(0, 9, "DECIMAL32") == 0)
        typeString = typeString.substr(0, 9) + "(8)[]";
    else if (typeString.compare(0, 9, "DECIMAL64") == 0)
        typeString = typeString.substr(0, 9) + "(15)[]";
    else if (typeString.compare(0, 10, "DECIMAL128") == 0)
        typeString = typeString.substr(0, 10) + "(25)[]";
    cout << "test type: " << typeString << endl;
    string script = "login(\"admin\",\"123456\")\n\
            st2 = streamTable(1:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB," + typeString + "])\n\
            enableTableShareAndPersistence(table=st2, tableName=`"+st+", asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\n\
            go\n\
            setStreamTableFilterColumn("+st+", `sym)";
    conn.run(script);

    string replayScript = "n = 1000;\
            table1_SDT = table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, " + typeString + ", DOUBLE]);\
            share table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, " + typeString + ", DOUBLE]) as res1_SDT;\
            table2_SDT = table(1:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, " + typeString + "]);\
            share table(1:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, " + typeString + "]) as res2_SDT;\
            go;\
            tableInsert(table1_SDT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), take(array(" + typeString + ").append!([" + vecVal + "]),n), rand(100.00,n));\
            tableInsert(table2_SDT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), take(array(" + typeString + ").append!([" + vecVal + "]),n));\
            share table1_SDT as ex1_SDT;\
            share table2_SDT as ex2_SDT;\
            go;\
            d = dict(['msg1','msg2'], [table1_SDT, table2_SDT]);\
            replay(inputTables=d, outputTables=`"+st+", dateColumn=`timestampv, timeColumn=`timestampv)";
    conn.run(replayScript);

    DictionarySP t1s = conn.run("schema(table1_SDT)");
    DictionarySP t2s = conn.run("schema(table2_SDT)");
    unordered_map<string, DictionarySP> sym2schema;
    sym2schema["msg1"] = t1s;
    sym2schema["msg2"] = t2s;
    StreamDeserializerSP sdsp = new StreamDeserializer(sym2schema);
    return sdsp;
}

INSTANTIATE_TEST_SUITE_P(, StreamingDeserilizerTester_basic, testing::Values(-1, rand() % 1000 + 13000));
TEST_P(StreamingDeserilizerTester_basic, test_Threadclient_onehandler_subscribeWithstreamDeserilizer)
{
    int msg1_total = 0, msg2_total = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    Signal notify;
    Mutex mutex;
    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer(st);

    AutoFitTableAppender appender1("", "res1_SDT", conn);
    AutoFitTableAppender appender2("", "res2_SDT", conn);

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        LockGuard<Mutex> lock(&mutex);
        bool succeeded = false; 
        TableSP tmp = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                if (symbol == "msg1")
                {
                    appender1.append(tmp);
                    msg1_total += 1;
                }
                else if (symbol == "msg2")
                {
                    appender2.append(tmp);
                    msg2_total += 1;
                }
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                Util::sleep(100);
            }
        }

        if (msg.getOffset() == 1999)
        {
            notify.set();
        }
    };

    ThreadedClient threadedClient = listenport == -1? ThreadedClient() : ThreadedClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(auto thread1 = threadedClient.subscribe(hostName, port, onehandler, st, "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp););
    }
    else
    {
        auto thread1 = threadedClient.subscribe(hostName, port, onehandler, st, "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp);
        thread1->setAffinity(0);
        notify.wait();

        threadedClient.unsubscribe(hostName, port, st, "test_SD");
        EXPECT_TRUE(conn.run("re = select * from res1_SDT order by datetimev;\
                            ex = select * from ex1_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn.run("re = select * from res2_SDT order by datetimev;\
                            ex = select * from ex2_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());                   
        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.insert(listenport);
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadclient_batchhandler_subscribeWithstreamDeserilizer)
{
    int msg1_total = 0, msg2_total = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    Signal notify;
    Mutex mutex;
    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer(st);

    AutoFitTableAppender appender1("", "res1_SDT", conn);
    AutoFitTableAppender appender2("", "res2_SDT", conn);

    auto batchhandler = [&](vector<Message> msgs)
    {
        LockGuard<Mutex> lock(&mutex);
        for (auto &msg : msgs)
        {
            const string &symbol = msg.getSymbol();
            bool succeeded = false; 
            TableSP tmp = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    if (symbol == "msg1")
                    {
                        appender1.append(tmp);
                        msg1_total += 1;
                    }
                    else if (symbol == "msg2")
                    {
                        appender2.append(tmp);
                        msg2_total += 1;
                    }
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    Util::sleep(100);
                }
            }
            if (msg.getOffset() == 1999)
            {
                notify.set();
            }
        }
    };

    ThreadedClient threadedClient = listenport == -1? ThreadedClient() : ThreadedClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, st, "mutiSchemaBatch", 0, true, nullptr, true, 1, 1.0, false, "admin", "123456", sdsp));
    }
    else
    {
        auto thread2 = threadedClient.subscribe(hostName, port, batchhandler, st, "mutiSchemaBatch", 0, true, nullptr, true, 1, 1.0, false, "admin", "123456", sdsp);
        thread2->setAffinity(0);
        notify.wait();

        threadedClient.unsubscribe(hostName, port, st, "mutiSchemaBatch");
        EXPECT_TRUE(conn.run("re = select * from res1_SDT order by datetimev;\
                            ex = select * from ex1_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn.run("re = select * from res2_SDT order by datetimev;\
                            ex = select * from ex2_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());         
        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.insert(listenport);
}

TEST_P(StreamingDeserilizerTester_basic, test_Pollingclient_subscribeWithstreamDeserilizer)
{
    int msg1_total = 0, msg2_total = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer(st);

    AutoFitTableAppender appender1("", "res1_SDT", conn);
    AutoFitTableAppender appender2("", "res2_SDT", conn);

    PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(hostName, port, st, "actionTest", 0, true, nullptr, false, false, "admin", "123456", sdsp));
    }
    else
    {
        auto queue = client.subscribe(hostName, port, st, "actionTest", 0, true, nullptr, false, false, "admin", "123456", sdsp);

        Message msg;
        Signal notify;
        Mutex mutex;

        thread th1 = thread([&]{
        LockGuard<Mutex> lock(&mutex);
        while (true)
        {
            queue->pop(msg);
            // cout<<msg->getString()<<endl;
            if(msg.isNull()) {break;}
            else{
                const string &symbol = msg.getSymbol();
                bool succeeded = false; 
                TableSP tmp = AnyVectorToTable(msg);
                while(!succeeded){
                    try
                    {
                        if (symbol == "msg1")
                        {
                            appender1.append(tmp);
                            msg1_total += 1;
                        }
                        else if (symbol == "msg2")
                        {
                            appender2.append(tmp);
                            msg2_total += 1;
                        }
                        succeeded = true;
                    }
                    catch(const std::exception& e)
                    {
                        Util::sleep(100);
                    }
                }
                if (msg.getOffset() == 1999)
                {
                    notify.set();
                }
            }
        } });
        
        notify.wait();
        client.unsubscribe(hostName, port, st, "actionTest");
        th1.join();
        EXPECT_TRUE(conn.run("re = select * from res1_SDT order by datetimev;\
                            ex = select * from ex1_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn.run("re = select * from res2_SDT order by datetimev;\
                            ex = select * from ex2_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }
    usedPorts.insert(listenport);
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadpooledclient_threadCount_1_subscribeWithstreamDeserilizer)
{
    int msg1_total = 0, msg2_total = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;
    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer(st);

    AutoFitTableAppender appender1("", "res1_SDT", conn);
    AutoFitTableAppender appender2("", "res2_SDT", conn);

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        LockGuard<Mutex> lock(&mutex);
        bool succeeded = false; 
        TableSP tmp = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                if (symbol == "msg1")
                {
                    appender1.append(tmp);
                    msg1_total += 1;
                }
                else if (symbol == "msg2")
                {
                    appender2.append(tmp);
                    msg2_total += 1;
                }
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                Util::sleep(100);
            }
        }

        if (msg.getOffset() == 1999)
        {
            notify.set();
        }
    };


    ThreadPooledClient client = listenport == -1? ThreadPooledClient(0, 1) : ThreadPooledClient(listenport, 1);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, st, "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp));
    }
    else
    {
        auto threadVec = client.subscribe(hostName, port, onehandler, st, "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp);
        EXPECT_EQ(threadVec.size(), 1);
        notify.wait();

        client.unsubscribe(hostName, port, st, "test_SD");
        EXPECT_TRUE(conn.run("re = select * from res1_SDT order by datetimev;\
                            ex = select * from ex1_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn.run("re = select * from res2_SDT order by datetimev;\
                            ex = select * from ex2_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.insert(listenport);
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadpooledclient_threadCount_2_subscribeWithstreamDeserilizer)
{
    int msg1_total = 0;
    int msg2_total = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer(st);

    AutoFitTableAppender appender1("", "res1_SDT", conn);
    AutoFitTableAppender appender2("", "res2_SDT", conn);

    Signal notify;
    Mutex mutex;
    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        LockGuard<Mutex> lock(&mutex);
        bool succeeded = false; 
        TableSP tmp = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                if (symbol == "msg1")
                {
                    appender1.append(tmp);
                    msg1_total += 1;
                }
                else if (symbol == "msg2")
                {
                    appender2.append(tmp);
                    msg2_total += 1;
                }
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                Util::sleep(100);
            }
        }

        if (msg.getOffset() == 1999)
        {
            notify.set();
        }
    };

    ThreadPooledClient client = listenport == -1? ThreadPooledClient(0, 2) : ThreadPooledClient(listenport, 2);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, st, "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp));
    }
    else
    {
        auto threadVec = client.subscribe(hostName, port, onehandler, st, "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp);
        EXPECT_EQ(threadVec.size(), 2);
        notify.wait();

        client.unsubscribe(hostName, port, st, "test_SD");
        EXPECT_TRUE(conn.run("re = select * from res1_SDT order by datetimev;\
                            ex = select * from ex1_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn.run("re = select * from res2_SDT order by datetimev;\
                            ex = select * from ex2_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }
    usedPorts.insert(listenport);
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadclient_with_msgAsTable_True)
{
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer(st);

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        // cout << symbol << endl;
    };

    ThreadedClient threadedClient = listenport == -1? ThreadedClient() : ThreadedClient(listenport);
    EXPECT_ANY_THROW(auto thread1 = threadedClient.subscribe(hostName, port, onehandler, st, "test_SD", 0, true, nullptr, true, false, "admin", "123456", sdsp));

    usedPorts.insert(listenport);
}

TEST_P(StreamingDeserilizerTester_basic, test_Pollingclient_with_msgAsTable_True)
{
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer(st);

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        // cout << symbol << endl;
    };

    PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
    EXPECT_ANY_THROW(client.subscribe(hostName, port, st, "actionTest", 0, true, nullptr, true, false, "admin", "123456", sdsp));

    usedPorts.insert(listenport);
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadpooledclient_with_msgAsTable_True)
{
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer(st);

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        // cout << symbol << endl;
    };

    ThreadPooledClient client = listenport == -1? ThreadPooledClient() : ThreadPooledClient(listenport, 1);

    EXPECT_ANY_THROW(auto threadVec = client.subscribe(hostName, port, onehandler, st, "test_SD", 0, true, nullptr, true, false, "admin", "123456", sdsp));

    usedPorts.insert(listenport);
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadclient_onehandler_subscribeWithstreamDeserilizer_2)
{
    int msg1_total = 0, msg2_total = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;
    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer_2(st);


    AutoFitTableAppender appender1("", "res1_SDT", conn);
    AutoFitTableAppender appender2("", "res2_SDT", conn);

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        LockGuard<Mutex> lock(&mutex);
        bool succeeded = false; 
        TableSP tmp = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                if (symbol == "msg1")
                {
                    appender1.append(tmp);
                    msg1_total += 1;
                }
                else if (symbol == "msg2")
                {
                    appender2.append(tmp);
                    msg2_total += 1;
                }
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                Util::sleep(100);
            }
        }

        if (msg.getOffset() == 1999)
        {
            notify.set();
        }
    };


    ThreadedClient threadedClient = listenport == -1? ThreadedClient() : ThreadedClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, st, "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp));
    }
    else
    {
        auto thread1 = threadedClient.subscribe(hostName, port, onehandler, st, "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp);
        thread1->setAffinity(0);
        notify.wait();

        threadedClient.unsubscribe(hostName, port, st, "test_SD");
        EXPECT_TRUE(conn.run("re = select * from res1_SDT order by datetimev;\
                            ex = select * from ex1_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn.run("re = select * from res2_SDT order by datetimev;\
                            ex = select * from ex2_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.insert(listenport);
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadclient_batchhandler_subscribeWithstreamDeserilizer_2)
{
    int msg1_total = 0, msg2_total = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    Signal notify;
    Mutex mutex;
    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer_2(st);

    AutoFitTableAppender appender1("", "res1_SDT", conn);
    AutoFitTableAppender appender2("", "res2_SDT", conn);

    auto batchhandler = [&](vector<Message> msgs)
    {
        LockGuard<Mutex> lock(&mutex);
        for (auto &msg : msgs)
        {
            const string &symbol = msg.getSymbol();
            bool succeeded = false; 
            TableSP tmp = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    if (symbol == "msg1")
                    {
                        appender1.append(tmp);
                        msg1_total += 1;
                    }
                    else if (symbol == "msg2")
                    {
                        appender2.append(tmp);
                        msg2_total += 1;
                    }
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    Util::sleep(100);
                }
            }

            if (msg.getOffset() == 1999)
            {
                notify.set();
            }
        }
    };

    ThreadedClient threadedClient = listenport == -1? ThreadedClient() : ThreadedClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, st, "mutiSchemaBatch", 0, true, nullptr, true, 1, 1.0, false, "admin", "123456", sdsp));
    }
    else
    {
        auto thread2 = threadedClient.subscribe(hostName, port, batchhandler, st, "mutiSchemaBatch", 0, true, nullptr, true, 1, 1.0, false, "admin", "123456", sdsp);
        thread2->setAffinity(0);
        notify.wait();

        threadedClient.unsubscribe(hostName, port, st, "mutiSchemaBatch");
        EXPECT_TRUE(conn.run("re = select * from res1_SDT order by datetimev;\
                            ex = select * from ex1_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn.run("re = select * from res2_SDT order by datetimev;\
                            ex = select * from ex2_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.insert(listenport);
}

TEST_P(StreamingDeserilizerTester_basic, test_Pollingclient_subscribeWithstreamDeserilizer_2)
{
    int msg1_total = 0, msg2_total = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer_2(st);

    AutoFitTableAppender appender1("", "res1_SDT", conn);
    AutoFitTableAppender appender2("", "res2_SDT", conn);

    PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(hostName, port, st, "actionTest", 0, true, nullptr, false, false, "admin", "123456", sdsp));
    }
    else
    {
        auto queue = client.subscribe(hostName, port, st, "actionTest", 0, true, nullptr, false, false, "admin", "123456", sdsp);

        Message msg;
        Signal notify;
        Mutex mutex;
        thread th1 = thread([&]{
        LockGuard<Mutex> lock(&mutex);
        while (true)
        {
            queue->pop(msg);
            // cout<<msg->getString()<<endl;
            if(msg.isNull()) {break;}
            else{
                const string &symbol = msg.getSymbol();
                bool succeeded = false; 
                TableSP tmp = AnyVectorToTable(msg);
                while(!succeeded){
                    try
                    {
                        if (symbol == "msg1")
                        {
                            appender1.append(tmp);
                            msg1_total += 1;
                        }
                        else if (symbol == "msg2")
                        {
                            appender2.append(tmp);
                            msg2_total += 1;
                        }
                        succeeded = true;
                    }
                    catch(const std::exception& e)
                    {
                        Util::sleep(100);
                    }
                }
                if (msg.getOffset() == 1999)
                    notify.set();
            }
        } });

        notify.wait();
        client.unsubscribe(hostName, port, st, "actionTest");
        th1.join();
        EXPECT_TRUE(conn.run("re = select * from res1_SDT order by datetimev;\
                            ex = select * from ex1_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn.run("re = select * from res2_SDT order by datetimev;\
                            ex = select * from ex2_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.insert(listenport);
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadpooledclient_threadCount_1_subscribeWithstreamDeserilizer_2)
{
    int msg1_total = 0, msg2_total = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;
    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer_2(st);

    AutoFitTableAppender appender1("", "res1_SDT", conn);
    AutoFitTableAppender appender2("", "res2_SDT", conn);

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        LockGuard<Mutex> lock(&mutex);
        bool succeeded = false; 
        TableSP tmp = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                if (symbol == "msg1")
                {
                    appender1.append(tmp);
                    msg1_total += 1;
                }
                else if (symbol == "msg2")
                {
                    appender2.append(tmp);
                    msg2_total += 1;
                }
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                Util::sleep(100);
            }
        }

        if (msg.getOffset() == 1999)
        {
            notify.set();
        }
    };

    ThreadPooledClient client = listenport == -1? ThreadPooledClient(0, 1) : ThreadPooledClient(listenport, 1);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, st, "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp));
    }
    else
    {
        auto threadVec = client.subscribe(hostName, port, onehandler, st, "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp);
        EXPECT_EQ(threadVec.size(), 1);
        notify.wait();

        client.unsubscribe(hostName, port, st, "test_SD");
        EXPECT_TRUE(conn.run("re = select * from res1_SDT order by datetimev;\
                            ex = select * from ex1_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn.run("re = select * from res2_SDT order by datetimev;\
                            ex = select * from ex2_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.insert(listenport);
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadpooledclient_threadCount_2_subscribeWithstreamDeserilizer_2)
{
    int msg1_total = 0;
    int msg2_total = 0;

    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer_2(st);

    Signal notify;
    Mutex mutex;
    AutoFitTableAppender appender1("", "res1_SDT", conn);
    AutoFitTableAppender appender2("", "res2_SDT", conn);

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        LockGuard<Mutex> lock(&mutex);
        bool succeeded = false; 
        TableSP tmp = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                if (symbol == "msg1")
                {
                    appender1.append(tmp);
                    msg1_total += 1;
                }
                else if (symbol == "msg2")
                {
                    appender2.append(tmp);
                    msg2_total += 1;
                }
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                Util::sleep(100);
            }
        }

        if (msg.getOffset() == 1999)
        {
            notify.set();
        }
    };

    ThreadPooledClient client = listenport == -1? ThreadPooledClient(0, 2) : ThreadPooledClient(listenport, 2);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, st, "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp));
    }
    else
    {
        auto threadVec = client.subscribe(hostName, port, onehandler, st, "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp);
        EXPECT_EQ(threadVec.size(), 2);
        notify.wait();

        client.unsubscribe(hostName, port, st, "test_SD");
        EXPECT_TRUE(conn.run("re = select * from res1_SDT order by datetimev;\
                            ex = select * from ex1_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn.run("re = select * from res2_SDT order by datetimev;\
                            ex = select * from ex2_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }
    usedPorts.insert(listenport);
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadclient_with_msgAsTable_True_2)
{
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer_2(st);

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        cout << symbol << endl;
    };

    ThreadedClient threadedClient = listenport == -1? ThreadedClient() : ThreadedClient(listenport);
    EXPECT_ANY_THROW(auto thread1 = threadedClient.subscribe(hostName, port, onehandler, st, "test_SD", 0, true, nullptr, true, false, "admin", "123456", sdsp));

    usedPorts.insert(listenport);
}

TEST_P(StreamingDeserilizerTester_basic, test_Pollingclient_with_msgAsTable_True_2)
{
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer_2(st);

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        cout << symbol << endl;
    };

    PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
    EXPECT_ANY_THROW(client.subscribe(hostName, port, st, "actionTest", 0, true, nullptr, true, false, "admin", "123456", sdsp));

    usedPorts.insert(listenport);
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadpooledclient_with_msgAsTable_True_2)
{
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer_2(st);

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        cout << symbol << endl;
    };

    ThreadPooledClient client = listenport == -1? ThreadPooledClient() : ThreadPooledClient(listenport, 1);

    EXPECT_ANY_THROW(auto threadVec = client.subscribe(hostName, port, onehandler, st, "test_SD", 0, true, nullptr, true, false, "admin", "123456", sdsp));

    usedPorts.insert(listenport);
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadclient_onehandler_subscribeWithstreamDeserilizer_3)
{
    int msg1_total = 0, msg2_total = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;
    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer_3(st);

    AutoFitTableAppender appender1("", "res1_SDT", conn);
    AutoFitTableAppender appender2("", "res2_SDT", conn);

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        LockGuard<Mutex> lock(&mutex);
        bool succeeded = false; 
        TableSP tmp = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                if (symbol == "msg1")
                {
                    appender1.append(tmp);
                    msg1_total += 1;
                }
                else if (symbol == "msg2")
                {
                    appender2.append(tmp);
                    msg2_total += 1;
                }
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                Util::sleep(100);
            }
        }

        if (msg.getOffset() == 1999)
        {
            notify.set();
        }
    };

    ThreadedClient threadedClient = listenport == -1? ThreadedClient() : ThreadedClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, st, "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp));
    }
    else
    {
        auto thread1 = threadedClient.subscribe(hostName, port, onehandler, st, "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp);
        thread1->setAffinity(0);
        notify.wait();

        threadedClient.unsubscribe(hostName, port, st, "test_SD");
        EXPECT_TRUE(conn.run("re = select * from res1_SDT order by datetimev;\
                            ex = select * from ex1_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn.run("re = select * from res2_SDT order by datetimev;\
                            ex = select * from ex2_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.insert(listenport);
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadclient_batchhandler_subscribeWithstreamDeserilizer_3)
{
    int msg1_total = 0, msg2_total = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    Signal notify;
    Mutex mutex;
    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer_3(st);

    AutoFitTableAppender appender1("", "res1_SDT", conn);
    AutoFitTableAppender appender2("", "res2_SDT", conn);

    auto batchhandler = [&](vector<Message> msgs)
    {
        LockGuard<Mutex> lock(&mutex);
        for (auto &msg : msgs)
        {
            const string &symbol = msg.getSymbol();
            bool succeeded = false; 
            TableSP tmp = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    if (symbol == "msg1")
                    {
                        appender1.append(tmp);
                        msg1_total += 1;
                    }
                    else if (symbol == "msg2")
                    {
                        appender2.append(tmp);
                        msg2_total += 1;
                    }
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    Util::sleep(100);
                }
            }

            if (msg.getOffset() == 1999)
            {
                notify.set();
            }
        }
    };

    ThreadedClient threadedClient = listenport == -1? ThreadedClient() : ThreadedClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, st, "mutiSchemaBatch", 0, true, nullptr, true, 1, 1.0, false, "admin", "123456", sdsp));
    }
    else
    {
        auto thread2 = threadedClient.subscribe(hostName, port, batchhandler, st, "mutiSchemaBatch", 0, true, nullptr, true, 1, 1.0, false, "admin", "123456", sdsp);
        thread2->setAffinity(0);
        notify.wait();

        threadedClient.unsubscribe(hostName, port, st, "mutiSchemaBatch");
        EXPECT_TRUE(conn.run("re = select * from res1_SDT order by datetimev;\
                            ex = select * from ex1_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn.run("re = select * from res2_SDT order by datetimev;\
                            ex = select * from ex2_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.insert(listenport);
}

TEST_P(StreamingDeserilizerTester_basic, test_Pollingclient_subscribeWithstreamDeserilizer_3)
{
    int msg1_total = 0, msg2_total = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer_3(st);

    AutoFitTableAppender appender1("", "res1_SDT", conn);
    AutoFitTableAppender appender2("", "res2_SDT", conn);

    PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(hostName, port, st, "actionTest", 0, true, nullptr, false, false, "admin", "123456", sdsp));
    }
    else
    {
        auto queue = client.subscribe(hostName, port, st, "actionTest", 0, true, nullptr, false, false, "admin", "123456", sdsp);

        Message msg;
        Signal notify;
        Mutex mutex;
        thread th1 = thread([&]{
        LockGuard<Mutex> lock(&mutex);
        while (true)
        {
            queue->pop(msg);
            // cout<<msg->getString()<<endl;
            if(msg.isNull()) {break;}
            else{
                const string &symbol = msg.getSymbol();
                bool succeeded = false; 
                TableSP tmp = AnyVectorToTable(msg);
                while(!succeeded){
                    try
                    {
                        if (symbol == "msg1")
                        {
                            appender1.append(tmp);
                            msg1_total += 1;
                        }
                        else if (symbol == "msg2")
                        {
                            appender2.append(tmp);
                            msg2_total += 1;
                        }
                        succeeded = true;
                    }
                    catch(const std::exception& e)
                    {
                        Util::sleep(100);
                    }
                }
                if (msg.getOffset() == 1999)
                    notify.set();
            }
        } });

        notify.wait();
        client.unsubscribe(hostName, port, st, "actionTest");
        th1.join();
        EXPECT_TRUE(conn.run("re = select * from res1_SDT order by datetimev;\
                            ex = select * from ex1_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn.run("re = select * from res2_SDT order by datetimev;\
                            ex = select * from ex2_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.insert(listenport);
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadpooledclient_threadCount_1_subscribeWithstreamDeserilizer_3)
{
    int msg1_total = 0, msg2_total = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;
    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer_3(st);

    AutoFitTableAppender appender1("", "res1_SDT", conn);
    AutoFitTableAppender appender2("", "res2_SDT", conn);

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        LockGuard<Mutex> lock(&mutex);
        bool succeeded = false; 
        TableSP tmp = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                if (symbol == "msg1")
                {
                    appender1.append(tmp);
                    msg1_total += 1;
                }
                else if (symbol == "msg2")
                {
                    appender2.append(tmp);
                    msg2_total += 1;
                }
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                Util::sleep(100);
            }
        }

        if (msg.getOffset() == 1999)
        {
            notify.set();
        }
    };

    ThreadPooledClient client = listenport == -1? ThreadPooledClient(0, 1) : ThreadPooledClient(listenport, 1);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, st, "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp));
    }
    else
    {
        auto threadVec = client.subscribe(hostName, port, onehandler, st, "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp);
        EXPECT_EQ(threadVec.size(), 1);
        notify.wait();

        client.unsubscribe(hostName, port, st, "test_SD");
        EXPECT_TRUE(conn.run("re = select * from res1_SDT order by datetimev;\
                            ex = select * from ex1_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn.run("re = select * from res2_SDT order by datetimev;\
                            ex = select * from ex2_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.insert(listenport);
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadpooledclient_threadCount_3_subscribeWithstreamDeserilizer_3)
{
    int msg1_total = 0;
    int msg2_total = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer_3(st);

    Signal notify;
    Mutex mutex;
    AutoFitTableAppender appender1("", "res1_SDT", conn);
    AutoFitTableAppender appender2("", "res2_SDT", conn);

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        LockGuard<Mutex> lock(&mutex);
        bool succeeded = false; 
        TableSP tmp = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                if (symbol == "msg1")
                {
                    appender1.append(tmp);
                    msg1_total += 1;
                }
                else if (symbol == "msg2")
                {
                    appender2.append(tmp);
                    msg2_total += 1;
                }
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                Util::sleep(100);
            }
        }

        if (msg.getOffset() == 1999)
        {
            notify.set();
        }
    };

    ThreadPooledClient client = listenport == -1? ThreadPooledClient() : ThreadPooledClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, st, "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp));
    }
    else
    {
        auto threadVec = client.subscribe(hostName, port, onehandler, st, "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp);
        EXPECT_EQ(threadVec.size(), 3);
        notify.wait();

        client.unsubscribe(hostName, port, st, "test_SD");
        EXPECT_TRUE(conn.run("re = select * from res1_SDT order by datetimev;\
                            ex = select * from ex1_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn.run("re = select * from res2_SDT order by datetimev;\
                            ex = select * from ex2_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }
    usedPorts.insert(listenport);
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadclient_with_msgAsTable_True_3)
{
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer_3(st);

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        cout << symbol << endl;
    };

    ThreadedClient threadedClient = listenport == -1? ThreadedClient() : ThreadedClient(listenport);
    EXPECT_ANY_THROW(auto thread1 = threadedClient.subscribe(hostName, port, onehandler, st, "test_SD", 0, true, nullptr, true, false, "admin", "123456", sdsp));

    usedPorts.insert(listenport);
}

TEST_P(StreamingDeserilizerTester_basic, test_Pollingclient_with_msgAsTable_True_3)
{
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer_3(st);

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        cout << symbol << endl;
    };

    PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
    EXPECT_ANY_THROW(client.subscribe(hostName, port, st, "actionTest", 0, true, nullptr, true, false, "admin", "123456", sdsp));

    usedPorts.insert(listenport);
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadpooledclient_with_msgAsTable_True_3)
{
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer_3(st);

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        cout << symbol << endl;
    };

    ThreadPooledClient client = listenport == -1? ThreadPooledClient() : ThreadPooledClient(listenport, 1);

    EXPECT_ANY_THROW(auto threadVec = client.subscribe(hostName, port, onehandler, st, "test_SD", 0, true, nullptr, true, false, "admin", "123456", sdsp));

    usedPorts.insert(listenport);
}

class StreamingDeserilizerTester_allTypes : public StreamingDeserilizerTester, public ::testing::WithParamInterface<tuple<int, pair<DATA_TYPE, string>>>
{
public:
    static vector<pair<DATA_TYPE, string>> getData()
    {
        return {{DT_BOOL, "rand(0 1, 1)[0]"},{DT_BOOL, "bool()"},
                {DT_CHAR, "rand(127c, 1)[0]"},{DT_CHAR, "char()"},
                {DT_SHORT, "rand(32767h, 1)[0]"},{DT_SHORT, "short()"},
                {DT_INT, "rand(2147483647, 1)[0]"},{DT_INT, "int()"},
                {DT_LONG, "rand(1000l, 1)[0]"},{DT_LONG, "long()"},
                {DT_DATE, "rand(2019.01.01, 1)[0]"},{DT_DATE, "date()"},
                {DT_MONTH, "rand(2019.01M, 1)[0]"},{DT_MONTH, "month()"},
                {DT_TIME, "rand(12:00:00.123, 1)[0]"},{DT_TIME, "time()"},
                {DT_MINUTE, "rand(12:00m, 1)[0]"},{DT_MINUTE, "minute()"},
                {DT_SECOND, "rand(12:00:00, 1)[0]"},{DT_SECOND, "second()"},
                {DT_DATETIME, "rand(2019.01.01 12:00:00, 1)[0]"},{DT_DATETIME, "datetime()"},
                {DT_TIMESTAMP, "rand(2019.01.01 12:00:00.123, 1)[0]"},{DT_TIMESTAMP, "timestamp()"},
                {DT_NANOTIME, "rand(12:00:00.123456789, 1)[0]"},{DT_NANOTIME, "nanotime()"},
                {DT_NANOTIMESTAMP, "rand(2019.01.01 12:00:00.123456789, 1)[0]"},{DT_NANOTIMESTAMP, "nanotimestamp()"},
                {DT_DATEHOUR, "rand(datehour(100), 1)[0]"},{DT_DATEHOUR, "datehour()"},
                {DT_FLOAT, "rand(10.00f, 1)[0]"},{DT_FLOAT, "float()"},
                {DT_DOUBLE, "rand(10.00, 1)[0]"},{DT_DOUBLE, "double()"},
                {DT_IP, "take(ipaddr('192.168.1.1'), 1)[0]"},{DT_IP, "ipaddr()"},
                {DT_UUID, "take(uuid('12345678-1234-1234-1234-123456789012'), 1)[0]"},{DT_UUID, "uuid()"},
                {DT_INT128, "take(int128(`e1671797c52e15f763380b45e841ec32), 1)[0]"},{DT_INT128, "int128()"},
                {DT_DECIMAL32, "decimal32(rand('-1.123''''2.23468965412', 1)[0], 8)"},{DT_DECIMAL32, "decimal32(,2)"},
                {DT_DECIMAL64, "decimal64(rand('-1.123''''2.123123123123123123', 1)[0], 15)"},{DT_DECIMAL64, "decimal64(,16)"},
                {DT_DECIMAL128, "decimal128(rand('-1.123''''2.123123123123123123123123123', 1)[0], 25)"},{DT_DECIMAL128, "decimal128(,26)"},
                {DT_BLOB, "blob('abc')"},{DT_BLOB, "blob(`)"}};
    };
};
INSTANTIATE_TEST_SUITE_P(, StreamingDeserilizerTester_allTypes, testing::Combine(
    testing::Values(0, rand() % 1000 + 13000),
    testing::ValuesIn(StreamingDeserilizerTester_allTypes::getData())
));


TEST_P(StreamingDeserilizerTester_allTypes, test_Threadclient_onehandler_subscribeWithstreamDeserilizer_allTypes)
{
    int msg1_total = 0, msg2_total = 0;
    int listenport = std::get<0>(GetParam());
    DATA_TYPE ttp = std::get<1>(GetParam()).first;
    string scr = std::get<1>(GetParam()).second;
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;
    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer_withallTypes(st, ttp, scr);

    AutoFitTableAppender appender1("", "res1_SDT", conn);
    AutoFitTableAppender appender2("", "res2_SDT", conn);

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        LockGuard<Mutex> lock(&mutex);
        bool succeeded = false; 
        TableSP tmp = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                if (symbol == "msg1")
                {
                    appender1.append(tmp);
                    msg1_total += 1;
                }
                else if (symbol == "msg2")
                {
                    appender2.append(tmp);
                    msg2_total += 1;
                }
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                Util::sleep(100);
            }
        }

        if (msg.getOffset() == 1999)
        {
            notify.set();
        }
    };

    ThreadedClient threadedClient = listenport == -1? ThreadedClient() : ThreadedClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, st, "test_SD", 0, false, nullptr, false, false, "admin", "123456", sdsp));
    }
    else
    {
        auto thread1 = threadedClient.subscribe(hostName, port, onehandler, st, "test_SD", 0, false, nullptr, false, false, "admin", "123456", sdsp);
        thread1->setAffinity(0);
        notify.wait();

        threadedClient.unsubscribe(hostName, port, st, "test_SD");
        EXPECT_TRUE(conn.run("re = select * from res1_SDT order by datetimev;\
                            ex = select * from ex1_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn.run("re = select * from res2_SDT order by datetimev;\
                            ex = select * from ex2_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.insert(listenport);
}

TEST_P(StreamingDeserilizerTester_allTypes, test_Threadclient_batchhandler_subscribeWithstreamDeserilizer_allTypes)
{
    int msg1_total = 0, msg2_total = 0;
    int listenport = std::get<0>(GetParam());
    DATA_TYPE ttp = std::get<1>(GetParam()).first;
    string scr = std::get<1>(GetParam()).second;
    cout << "current listenport is " << listenport << endl;

    Signal notify;
    Mutex mutex;
    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer_withallTypes(st, ttp, scr);

    AutoFitTableAppender appender1("", "res1_SDT", conn);
    AutoFitTableAppender appender2("", "res2_SDT", conn);

    auto batchhandler = [&](vector<Message> msgs)
    {
        LockGuard<Mutex> lock(&mutex);
        for (auto &msg : msgs)
        {
            const string &symbol = msg.getSymbol();
            bool succeeded = false; 
            TableSP tmp = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    if (symbol == "msg1")
                    {
                        appender1.append(tmp);
                        msg1_total += 1;
                    }
                    else if (symbol == "msg2")
                    {
                        appender2.append(tmp);
                        msg2_total += 1;
                    }
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    Util::sleep(100);
                }
            }
            if (msg.getOffset() == 1999)
            {
                notify.set();
            }
        }
    };

    ThreadedClient threadedClient = listenport == -1? ThreadedClient() : ThreadedClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, st, "mutiSchemaBatch", 0, false, nullptr, true, 1, 1.0, false, "admin", "123456", sdsp));
    }
    else
    {
        auto thread2 = threadedClient.subscribe(hostName, port, batchhandler, st, "mutiSchemaBatch", 0, false, nullptr, true, 1, 1.0, false, "admin", "123456", sdsp);
        thread2->setAffinity(0);
        notify.wait();

        threadedClient.unsubscribe(hostName, port, st, "mutiSchemaBatch");
        EXPECT_TRUE(conn.run("re = select * from res1_SDT order by datetimev;\
                            ex = select * from ex1_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn.run("re = select * from res2_SDT order by datetimev;\
                            ex = select * from ex2_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.insert(listenport);
}

TEST_P(StreamingDeserilizerTester_allTypes, test_Pollingclient_subscribeWithstreamDeserilizer_allTypes)
{
    int msg1_total = 0, msg2_total = 0;
    int listenport = std::get<0>(GetParam());
    DATA_TYPE ttp = std::get<1>(GetParam()).first;
    string scr = std::get<1>(GetParam()).second;
    cout << "current listenport is " << listenport << endl;

    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer_withallTypes(st, ttp, scr);

    AutoFitTableAppender appender1("", "res1_SDT", conn);
    AutoFitTableAppender appender2("", "res2_SDT", conn);

    PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(hostName, port, st, "actionTest", 0, true, nullptr, false, false, "admin", "123456", sdsp));
    }
    else
    {
        auto queue = client.subscribe(hostName, port, st, "actionTest", 0, true, nullptr, false, false, "admin", "123456", sdsp);

        Message msg;
        Signal notify;
        Mutex mutex;
        thread th1 = thread([&]{
        LockGuard<Mutex> lock(&mutex);
        while (true)
        {
            queue->pop(msg);
            // cout<<msg->getString()<<endl;
            if(msg.isNull()) {break;}
            else{
                const string &symbol = msg.getSymbol();
                bool succeeded = false; 
                TableSP tmp = AnyVectorToTable(msg);
                while(!succeeded){
                    try
                    {
                        if (symbol == "msg1")
                        {
                            appender1.append(tmp);
                            msg1_total += 1;
                        }
                        else if (symbol == "msg2")
                        {
                            appender2.append(tmp);
                            msg2_total += 1;
                        }
                        succeeded = true;
                    }
                    catch(const std::exception& e)
                    {
                        Util::sleep(100);
                    }
                }
                if (msg.getOffset() == 1999)
                    notify.set();
            }
        } });

        notify.wait();
        client.unsubscribe(hostName, port, st, "actionTest");
        th1.join();
        EXPECT_TRUE(conn.run("re = select * from res1_SDT order by datetimev;\
                            ex = select * from ex1_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn.run("re = select * from res2_SDT order by datetimev;\
                            ex = select * from ex2_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.insert(listenport);
}

TEST_P(StreamingDeserilizerTester_allTypes, test_Threadpooledclient_subscribeWithstreamDeserilizer_allTypes)
{
    int msg1_total = 0, msg2_total = 0;
    int listenport = std::get<0>(GetParam());
    DATA_TYPE ttp = std::get<1>(GetParam()).first;
    string scr = std::get<1>(GetParam()).second;
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;
    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer_withallTypes(st, ttp, scr);

    AutoFitTableAppender appender1("", "res1_SDT", conn);
    AutoFitTableAppender appender2("", "res2_SDT", conn);

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        LockGuard<Mutex> lock(&mutex);
        bool succeeded = false; 
        TableSP tmp = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                if (symbol == "msg1")
                {
                    appender1.append(tmp);
                    msg1_total += 1;
                }
                else if (symbol == "msg2")
                {
                    appender2.append(tmp);
                    msg2_total += 1;
                }
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                Util::sleep(100);
            }
        }

        if (msg.getOffset() == 1999)
        {
            notify.set();
        }
    };

    int threadCount = rand() % 10 + 1;
    ThreadPooledClient client = listenport == -1? ThreadPooledClient() : ThreadPooledClient(listenport, threadCount);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, st, "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp));
    }
    else
    {
        auto threadVec = client.subscribe(hostName, port, onehandler, st, "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp);
        EXPECT_EQ(threadVec.size(), threadCount);
        notify.wait();

        client.unsubscribe(hostName, port, st, "test_SD");
        EXPECT_TRUE(conn.run("re = select * from res1_SDT order by datetimev;\
                            ex = select * from ex1_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn.run("re = select * from res2_SDT order by datetimev;\
                            ex = select * from ex2_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.insert(listenport);
}

class StreamingDeserilizerTester_arrayVector : public StreamingDeserilizerTester, public ::testing::WithParamInterface<tuple<int, pair<DATA_TYPE, string>>>
{
public:
    static vector<pair<DATA_TYPE, string>> getData()
    {
        return {{DT_BOOL_ARRAY, "rand(0 1, 2)"},{DT_BOOL_ARRAY, "array(BOOL, 2,2,NULL)"},
                {DT_CHAR_ARRAY, "rand(127c, 2)"},{DT_CHAR_ARRAY, "array(CHAR, 2,2,NULL)"},
                {DT_SHORT_ARRAY, "rand(32767h, 2)"}, {DT_SHORT_ARRAY, "array(SHORT, 2,2,NULL)"},
                {DT_INT_ARRAY, "rand(2147483647, 2)"}, {DT_INT_ARRAY, "array(INT, 2,2,NULL)"},
                {DT_LONG_ARRAY, "rand(1000l, 2)"}, {DT_LONG_ARRAY, "array(LONG, 2,2,NULL)"},
                {DT_DATE_ARRAY, "rand(2019.01.01, 2)"}, {DT_DATE_ARRAY, "array(DATE, 2,2,NULL)"},
                {DT_MONTH_ARRAY, "rand(2019.01M, 2)"}, {DT_MONTH_ARRAY, "array(MONTH, 2,2,NULL)"},
                {DT_TIME_ARRAY, "rand(12:00:00.123, 2)"}, {DT_TIME_ARRAY, "array(TIME, 2,2,NULL)"},
                {DT_MINUTE_ARRAY, "rand(12:00m, 2)"}, {DT_MINUTE_ARRAY, "array(MINUTE, 2,2,NULL)"},
                {DT_SECOND_ARRAY, "rand(12:00:00, 2)"}, {DT_SECOND_ARRAY, "array(SECOND, 2,2,NULL)"},
                {DT_DATETIME_ARRAY, "rand(2019.01.01 12:00:00, 2)"}, {DT_DATETIME_ARRAY, "array(DATETIME, 2,2,NULL)"},
                {DT_TIMESTAMP_ARRAY, "rand(2019.01.01 12:00:00.123, 2)"}, {DT_TIMESTAMP_ARRAY, "array(TIMESTAMP, 2,2,NULL)"},
                {DT_NANOTIME_ARRAY, "rand(12:00:00.123456789, 2)"}, {DT_NANOTIME_ARRAY, "array(NANOTIME, 2,2,NULL)"},
                {DT_NANOTIMESTAMP_ARRAY, "rand(2019.01.01 12:00:00.123456789, 2)"}, {DT_NANOTIMESTAMP_ARRAY, "array(NANOTIMESTAMP, 2,2,NULL)"},
                {DT_DATEHOUR_ARRAY, "rand(datehour(100), 2)"}, {DT_DATEHOUR_ARRAY, "array(DATEHOUR, 2,2,NULL)"},
                {DT_FLOAT_ARRAY, "rand(10.00f, 2)"}, {DT_FLOAT_ARRAY, "array(FLOAT, 2,2,NULL)"},
                {DT_DOUBLE_ARRAY, "rand(10.00, 2)"}, {DT_DOUBLE_ARRAY, "array(DOUBLE, 2,2,NULL)"},
                {DT_IP_ARRAY, "take(ipaddr('192.168.1.1'), 2)"}, {DT_IP_ARRAY, "array(IPADDR, 2,2,NULL)"},
                {DT_UUID_ARRAY, "take(uuid('12345678-1234-1234-1234-123456789012'), 2)"}, {DT_UUID_ARRAY, "array(UUID, 2,2,NULL)"},
                {DT_INT128_ARRAY, "take(int128(`e1671797c52e15f763380b45e841ec32), 2)"}, {DT_INT128_ARRAY, "array(INT128, 2,2,NULL)"},
                {DT_DECIMAL32_ARRAY, "decimal32(rand('-1.123''''2.23468965412', 2), 8)"}, {DT_DECIMAL32_ARRAY, "array(DECIMAL32(2), 2,2,NULL)"},
                {DT_DECIMAL64_ARRAY, "decimal64(rand('-1.123''''2.123123123123123123', 2), 15)"}, {DT_DECIMAL64_ARRAY, "array(DECIMAL64(15), 2,2,NULL)"},
                {DT_DECIMAL128_ARRAY, "decimal128(rand('-1.123''''2.123123123123123123123123123', 2), 25)"}, {DT_DECIMAL128_ARRAY, "array(DECIMAL128(25), 2,2,NULL)"},};
    };
};
INSTANTIATE_TEST_SUITE_P(, StreamingDeserilizerTester_arrayVector, testing::Combine(
    testing::Values(0, rand() % 1000 + 13000),
    testing::ValuesIn(StreamingDeserilizerTester_arrayVector::getData())
));

TEST_P(StreamingDeserilizerTester_arrayVector, test_Threadclient_onehandler_subscribeWithstreamDeserilizer_arrayVector)
{
    int msg1_total = 0, msg2_total = 0;
    int listenport = std::get<0>(GetParam());
    DATA_TYPE ttp = std::get<1>(GetParam()).first;
    string scr = std::get<1>(GetParam()).second;
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;
    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer_witharrayVector(st, ttp, scr);

    AutoFitTableAppender appender1("", "res1_SDT", conn);
    AutoFitTableAppender appender2("", "res2_SDT", conn);

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        LockGuard<Mutex> lock(&mutex);
        bool succeeded = false; 
        TableSP tmp = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                if (symbol == "msg1")
                {
                    appender1.append(tmp);
                    msg1_total += 1;
                }
                else if (symbol == "msg2")
                {
                    appender2.append(tmp);
                    msg2_total += 1;
                }
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                Util::sleep(100);
            }
        }

        if (msg.getOffset() == 1999)
        {
            notify.set();
        }
    };

    ThreadedClient threadedClient = listenport == -1? ThreadedClient() : ThreadedClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, st, "test_SD", 0, false, nullptr, false, false, "admin", "123456", sdsp));
    }
    else
    {
        auto thread1 = threadedClient.subscribe(hostName, port, onehandler, st, "test_SD", 0, false, nullptr, false, false, "admin", "123456", sdsp);
        thread1->setAffinity(0);
        notify.wait();

        threadedClient.unsubscribe(hostName, port, st, "test_SD");
        EXPECT_TRUE(conn.run("re = select * from res1_SDT order by datetimev;\
                            ex = select * from ex1_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn.run("re = select * from res2_SDT order by datetimev;\
                            ex = select * from ex2_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.insert(listenport);
}

TEST_P(StreamingDeserilizerTester_arrayVector, test_Threadclient_batchhandler_subscribeWithstreamDeserilizer_arrayVector)
{
    int msg1_total = 0, msg2_total = 0;
    int listenport = std::get<0>(GetParam());
    DATA_TYPE ttp = std::get<1>(GetParam()).first;
    string scr = std::get<1>(GetParam()).second;
    cout << "current listenport is " << listenport << endl;

    Signal notify;
    Mutex mutex;
    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer_witharrayVector(st, ttp, scr);

    AutoFitTableAppender appender1("", "res1_SDT", conn);
    AutoFitTableAppender appender2("", "res2_SDT", conn);

    auto batchhandler = [&](vector<Message> msgs)
    {
        LockGuard<Mutex> lock(&mutex);
        for (auto &msg : msgs)
        {
            const string &symbol = msg.getSymbol();
            bool succeeded = false; 
            TableSP tmp = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    if (symbol == "msg1")
                    {
                        appender1.append(tmp);
                        msg1_total += 1;
                    }
                    else if (symbol == "msg2")
                    {
                        appender2.append(tmp);
                        msg2_total += 1;
                    }
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    Util::sleep(100);
                }
            }
            if (msg.getOffset() == 1999)
            {
                notify.set();
            }
        }
    };

    ThreadedClient threadedClient = listenport == -1? ThreadedClient() : ThreadedClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, st, "mutiSchemaBatch", 0, false, nullptr, true, 1, 1.0, false, "admin", "123456", sdsp));
    }
    else
    {
        auto thread2 = threadedClient.subscribe(hostName, port, batchhandler, st, "mutiSchemaBatch", 0, false, nullptr, true, 1, 1.0, false, "admin", "123456", sdsp);
        thread2->setAffinity(0);
        notify.wait();

        threadedClient.unsubscribe(hostName, port, st, "mutiSchemaBatch");
        EXPECT_TRUE(conn.run("re = select * from res1_SDT order by datetimev;\
                            ex = select * from ex1_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn.run("re = select * from res2_SDT order by datetimev;\
                            ex = select * from ex2_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.insert(listenport);
}

TEST_P(StreamingDeserilizerTester_arrayVector, test_Pollingclient_subscribeWithstreamDeserilizer_arrayVector)
{
    int msg1_total = 0, msg2_total = 0;
    int listenport = std::get<0>(GetParam());
    DATA_TYPE ttp = std::get<1>(GetParam()).first;
    string scr = std::get<1>(GetParam()).second;
    cout << "current listenport is " << listenport << endl;

    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer_witharrayVector(st, ttp, scr);

    AutoFitTableAppender appender1("", "res1_SDT", conn);
    AutoFitTableAppender appender2("", "res2_SDT", conn);

    PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(hostName, port, st, "actionTest", 0, true, nullptr, false, false, "admin", "123456", sdsp));
    }
    else
    {
        auto queue = client.subscribe(hostName, port, st, "actionTest", 0, true, nullptr, false, false, "admin", "123456", sdsp);

        Message msg;
        Signal notify;
        Mutex mutex;
        thread th1 = thread([&]{
        LockGuard<Mutex> lock(&mutex);
        while (true)
        {
            queue->pop(msg);
            // cout<<msg->getString()<<endl;
            if(msg.isNull()) {break;}
            else{
                const string &symbol = msg.getSymbol();
                bool succeeded = false; 
                TableSP tmp = AnyVectorToTable(msg);
                while(!succeeded){
                    try
                    {
                        if (symbol == "msg1")
                        {
                            appender1.append(tmp);
                            msg1_total += 1;
                        }
                        else if (symbol == "msg2")
                        {
                            appender2.append(tmp);
                            msg2_total += 1;
                        }
                        succeeded = true;
                    }
                    catch(const std::exception& e)
                    {
                        Util::sleep(100);
                    }
                }
                if (msg.getOffset() == 1999)
                    notify.set();
            }
        } });

        notify.wait();
        client.unsubscribe(hostName, port, st, "actionTest");
        th1.join();
        EXPECT_TRUE(conn.run("re = select * from res1_SDT order by datetimev;\
                            ex = select * from ex1_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn.run("re = select * from res2_SDT order by datetimev;\
                            ex = select * from ex2_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.insert(listenport);
}

TEST_P(StreamingDeserilizerTester_arrayVector, test_Threadpooledclient_subscribeWithstreamDeserilizer_arrayVector)
{
    int msg1_total = 0, msg2_total = 0;
    int listenport = std::get<0>(GetParam());
    DATA_TYPE ttp = std::get<1>(GetParam()).first;
    string scr = std::get<1>(GetParam()).second;
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;
    const string st = "test_SD_" + getRandString(10);
    StreamDeserializerSP sdsp = createStreamDeserializer_witharrayVector(st, ttp, scr);

    AutoFitTableAppender appender1("", "res1_SDT", conn);
    AutoFitTableAppender appender2("", "res2_SDT", conn);

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        LockGuard<Mutex> lock(&mutex);
        bool succeeded = false; 
        TableSP tmp = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                if (symbol == "msg1")
                {
                    appender1.append(tmp);
                    msg1_total += 1;
                }
                else if (symbol == "msg2")
                {
                    appender2.append(tmp);
                    msg2_total += 1;
                }
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                Util::sleep(100);
            }
        }

        if (msg.getOffset() == 1999)
        {
            notify.set();
        }
    };

    int threadCount = rand() % 10 + 1;
    ThreadPooledClient client = listenport == -1? ThreadPooledClient() : ThreadPooledClient(listenport, threadCount);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, st, "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp));
    }
    else
    {
        auto threadVec = client.subscribe(hostName, port, onehandler, st, "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp);
        EXPECT_EQ(threadVec.size(), threadCount);
        notify.wait();

        client.unsubscribe(hostName, port, st, "test_SD");
        EXPECT_TRUE(conn.run("re = select * from res1_SDT order by datetimev;\
                            ex = select * from ex1_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn.run("re = select * from res2_SDT order by datetimev;\
                            ex = select * from ex2_SDT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.insert(listenport);
}