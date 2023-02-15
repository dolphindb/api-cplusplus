class StreamingDeserilizerTester : public ::testing::Test, public ::testing::WithParamInterface<int>
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
        isNewVersion = conn.run("flag = 1;v = split(version(), ' ')[0];\
                                tmp=int(v.split('.'));\
                                if((tmp[0]==2 && tmp[1]==00 && tmp[2]>=9 )||(tmp[0]==2 && tmp[1]==10)){flag=1;}else{flag=0};\
                                flag")
                           ->getBool();
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
        ConstantSP res = conn.run("1+1");

        cout << "ok" << endl;
        string del_streamtable = "login(\"admin\",\"123456\");"
                                 "st2 = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE]);"
                                 "try{ enableTableShareAndPersistence(table=st2, tableName=`SDoutTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 10000);}catch(ex){};"
                                 "try{ dropStreamTable(`SDoutTables);}catch(ex){};"
                                 "try{ dropStreamTable(`st2);}catch(ex){};"
                                 "try{ dropStreamTable(`table1_SDPT);}catch(ex){};"
                                 "try{ dropStreamTable(`table2_SDPT);}catch(ex){};";
        conn.run(del_streamtable);
    }
    virtual void TearDown()
    {
        conn.run("undef all;");
    }
};

StreamDeserializerSP createStreamDeserializer()
{
    string script = "login(\"admin\",\"123456\")\n\
            st2 = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n\
            enableTableShareAndPersistence(table=st2, tableName=`SDoutTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\n\
            go\n\
            setStreamTableFilterColumn(SDoutTables, `sym)";
    conn.run(script);

    string replayScript = "n = 1000;table1_SDPT = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);\
            table2_SDPT = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);\
            tableInsert(table1_SDPT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));\
            tableInsert(table2_SDPT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));\
            d = dict(['msg1','msg2'], [table1_SDPT, table2_SDPT]);\
            replay(inputTables=d, outputTables=`SDoutTables, dateColumn=`timestampv, timeColumn=`timestampv)";
    conn.run(replayScript);

    DictionarySP t1s = conn.run("schema(table1_SDPT)");
    DictionarySP t2s = conn.run("schema(table2_SDPT)");
    unordered_map<string, DictionarySP> sym2schema;
    sym2schema["msg1"] = t1s;
    sym2schema["msg2"] = t2s;
    StreamDeserializerSP sdsp = new StreamDeserializer(sym2schema);
    return sdsp;
}

StreamDeserializerSP createStreamDeserializer_2()
{
    string script = "login(\"admin\",\"123456\")\n\
            st2 = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n\
            enableTableShareAndPersistence(table=st2, tableName=`SDoutTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\n\
            go\n\
            setStreamTableFilterColumn(SDoutTables, `sym)";
    conn.run(script);

    string replayScript = "n = 1000;table1_SDPT = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);\
            table2_SDPT = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);\
            tableInsert(table1_SDPT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));\
            tableInsert(table2_SDPT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));\
            d = dict(['msg1','msg2'], [table1_SDPT, table2_SDPT]);\
            replay(inputTables=d, outputTables=`SDoutTables, dateColumn=`timestampv, timeColumn=`timestampv)";
    conn.run(replayScript);

    TableSP t1 = conn.run("table1_SDPT");
    TableSP t2 = conn.run("table2_SDPT");
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

StreamDeserializerSP createStreamDeserializer_3()
{
    string script = "login(\"admin\",\"123456\")\n\
            st2 = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n\
            enableTableShareAndPersistence(table=st2, tableName=`SDoutTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\n\
            go\n\
            setStreamTableFilterColumn(SDoutTables, `sym)";
    conn.run(script);

    string replayScript = "n = 1000;t0 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);share t0 as table1_SDPT;\
            t = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);\
            tableInsert(table1_SDPT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));\
            tableInsert(t, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));\
            dbpath=\"dfs://test_dfs\";if(existsDatabase(dbpath)){dropDatabase(dbpath)};db=database(dbpath, VALUE, `a`b`c);\
            db.createPartitionedTable(t,`table2_SDPT,`sym).append!(t);\
            t2 = select * from loadTable(dbpath,`table2_SDPT);share t2 as table2_SDPT;\
            d = dict(['msg1','msg2'], [table1_SDPT, table2_SDPT]);\
            replay(inputTables=d, outputTables=`SDoutTables, dateColumn=`timestampv, timeColumn=`timestampv)";
    conn.run(replayScript);

    unordered_map<string, pair<string, string>> sym2schema;
    sym2schema["msg1"] = {"", "table1_SDPT"};
    sym2schema["msg2"] = {"dfs://test_dfs", "table2_SDPT"};
    StreamDeserializerSP sdsp = new StreamDeserializer(sym2schema, &conn);
    return sdsp;
}

StreamDeserializerSP createStreamDeserializer_witharrayVector()
{
    string script = "login(\"admin\",\"123456\")\n\
            st2 = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n\
            enableTableShareAndPersistence(table=st2, tableName=`SDoutTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\n\
            go\n\
            setStreamTableFilterColumn(SDoutTables, `sym)";
    conn.run(script);

    string replayScript = "n = 1000;table1_SDPT = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE[]]);\
            table2_SDPT = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);\
            index = 1..n;value = rand(100,n)+rand(1.0, n);av1 = arrayVector(index,value);\
            tableInsert(table1_SDPT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), av1);\
            tableInsert(table2_SDPT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));\
            d = dict(['msg1','msg2'], [table1_SDPT, table2_SDPT]);\
            replay(inputTables=d, outputTables=`SDoutTables, dateColumn=`timestampv, timeColumn=`timestampv)";
    conn.run(replayScript);

    DictionarySP t1s = conn.run("schema(table1_SDPT)");
    DictionarySP t2s = conn.run("schema(table2_SDPT)");
    unordered_map<string, DictionarySP> sym2schema;
    sym2schema["msg1"] = t1s;
    sym2schema["msg2"] = t2s;
    StreamDeserializerSP sdsp = new StreamDeserializer(sym2schema);
    return sdsp;
}

INSTANTIATE_TEST_CASE_P(listenPortIs, StreamingDeserilizerTester, testing::Values(0, rand() % 1000 + 13000));
TEST_P(StreamingDeserilizerTester, test_Threadclient_onehandler_subscribeWithstreamDeserilizer)
{
    int msg1_total = 0, msg2_total = 0;
    int index1 = 0, index2 = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    Signal notify;
    Mutex mutex;
    StreamDeserializerSP sdsp = createStreamDeserializer();

    TableSP table1_SDPT = conn.run("table1_SDPT");
    TableSP table2_SDPT = conn.run("table2_SDPT");

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        LockGuard<Mutex> lock(&mutex);
        if (symbol == "msg1")
        {
            msg1_total += 1;
            EXPECT_EQ(msg->get(0)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("datetimev"))->getString());
            EXPECT_EQ(msg->get(1)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("timestampv"))->getString());
            EXPECT_EQ(msg->get(2)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("sym"))->getString());
            EXPECT_EQ(msg->get(3)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("price1"))->getString());
            EXPECT_EQ(msg->get(4)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("price2"))->getString());
            index1++;
        }
        else if (symbol == "msg2")
        {
            msg2_total += 1;
            EXPECT_EQ(msg->get(0)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("datetimev"))->getString());
            EXPECT_EQ(msg->get(1)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("timestampv"))->getString());
            EXPECT_EQ(msg->get(2)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("sym"))->getString());
            EXPECT_EQ(msg->get(3)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("price1"))->getString());
            index2++;
        }
        if (msg1_total + msg2_total == 2000)
        {
            notify.set();
        }
    };

    ThreadedClient threadedClient(listenport);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(auto thread1 = threadedClient.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp););
    }
    else
    {
        auto thread1 = threadedClient.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp);
        thread1->setAffinity(0);
        notify.wait();

        threadedClient.unsubscribe(hostName, port, "SDoutTables", "test_SD");

        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.push_back(listenport);
}

TEST_P(StreamingDeserilizerTester, test_Threadclient_batchhandler_subscribeWithstreamDeserilizer)
{
    int msg1_total = 0, msg2_total = 0;
    int index1 = 0, index2 = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    Signal notify;
    Mutex mutex;
    StreamDeserializerSP sdsp = createStreamDeserializer();

    TableSP table1_SDPT = conn.run("table1_SDPT");
    TableSP table2_SDPT = conn.run("table2_SDPT");

    auto batchhandler = [&](vector<Message> msgs)
    {
        LockGuard<Mutex> lock(&mutex);
        for (auto &msg : msgs)
        {
            const string &symbol = msg.getSymbol();
            if (symbol == "msg1")
            {
                msg1_total += 1;
                // cout<<"index1= "<<index1<<endl;
                // cout<<msg->get(0)->getString()<<",";
                // cout<<msg->get(1)->getString()<<",";
                // cout<<msg->get(2)->getString()<<",";
                // cout<<msg->get(3)->getString()<<",";
                // cout<<msg->get(4)->getString()<<endl;
                // cout<<table1_SDPT->getRow(index1)->get(Util::createString("datetimev"))->getString()<<",";
                // cout<<table1_SDPT->getRow(index1)->get(Util::createString("timestampv"))->getString()<<",";
                // cout<<table1_SDPT->getRow(index1)->get(Util::createString("sym"))->getString()<<",";
                // cout<<table1_SDPT->getRow(index1)->get(Util::createString("price1"))->getString()<<",";
                // cout<<table1_SDPT->getRow(index1)->get(Util::createString("price2"))->getString()<<endl<<endl;

                // EXPECT_EQ(msg->get(0)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("datetimev"))->getString());
                // EXPECT_EQ(msg->get(1)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("timestampv"))->getString());
                // EXPECT_EQ(msg->get(2)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("sym"))->getString());
                // EXPECT_EQ(msg->get(3)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("price1"))->getString());
                // EXPECT_EQ(msg->get(4)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("price2"))->getString());
                index1++;
            }
            else if (symbol == "msg2")
            {
                msg2_total += 1;
                // cout<<"index2= "<<index2<<endl;
                // cout<<msg->get(0)->getString()<<",";
                // cout<<msg->get(1)->getString()<<",";
                // cout<<msg->get(2)->getString()<<",";
                // cout<<msg->get(3)->getString()<<endl;
                // cout<<table2_SDPT->getRow(index2)->get(Util::createString("datetimev"))->getString()<<",";
                // cout<<table2_SDPT->getRow(index2)->get(Util::createString("timestampv"))->getString()<<",";
                // cout<<table2_SDPT->getRow(index2)->get(Util::createString("sym"))->getString()<<",";
                // cout<<table2_SDPT->getRow(index2)->get(Util::createString("price1"))->getString()<<endl<<endl;

                // EXPECT_EQ(msg->get(0)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("datetimev"))->getString());
                // EXPECT_EQ(msg->get(1)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("timestampv"))->getString());
                // EXPECT_EQ(msg->get(2)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("sym"))->getString());
                // EXPECT_EQ(msg->get(3)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("price1"))->getString());
                index2++;
            }
            if (msg1_total + msg2_total == 2000)
            {
                notify.set();
            }
        }
    };

    ThreadedClient threadedClient(listenport);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, "SDoutTables", "mutiSchemaBatch", 0, true, nullptr, true, 1, 1.0, false, "admin", "123456", sdsp));
    }
    else
    {
        auto thread2 = threadedClient.subscribe(hostName, port, batchhandler, "SDoutTables", "mutiSchemaBatch", 0, true, nullptr, true, 1, 1.0, false, "admin", "123456", sdsp);
        thread2->setAffinity(0);
        notify.wait();

        threadedClient.unsubscribe(hostName, port, "SDoutTables", "mutiSchemaBatch");

        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.push_back(listenport);
}

TEST_P(StreamingDeserilizerTester, test_Pollingclient_subscribeWithstreamDeserilizer)
{
    int msg1_total = 0, msg2_total = 0;
    int index1 = 0, index2 = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    StreamDeserializerSP sdsp = createStreamDeserializer();

    TableSP table1_SDPT = conn.run("table1_SDPT");
    TableSP table2_SDPT = conn.run("table2_SDPT");

    PollingClient client(listenport);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(hostName, port, "SDoutTables", "actionTest", 0, true, nullptr, false, false, "admin", "123456", sdsp));
    }
    else
    {
        auto queue = client.subscribe(hostName, port, "SDoutTables", "actionTest", 0, true, nullptr, false, false, "admin", "123456", sdsp);

        Message msg;
        thread th1 = thread([&]
                            {
        while (true)
        {
            queue->pop(msg);
            // cout<<msg->getString()<<endl;
            if(msg.isNull()) {break;}
            else{
                const string &symbol = msg.getSymbol();
                if (symbol == "msg1")
                {
                    msg1_total += 1;
                    // cout<<"index1= "<<index1<<endl;
                    // cout<<msg->get(0)->getString()<<",";
                    // cout<<msg->get(1)->getString()<<",";
                    // cout<<msg->get(2)->getString()<<",";
                    // cout<<msg->get(3)->getString()<<",";
                    // cout<<msg->get(4)->getString()<<endl;
                    // cout<<table1_SDPT->getRow(index1)->get(Util::createString("datetimev"))->getString()<<",";
                    // cout<<table1_SDPT->getRow(index1)->get(Util::createString("timestampv"))->getString()<<",";
                    // cout<<table1_SDPT->getRow(index1)->get(Util::createString("sym"))->getString()<<",";
                    // cout<<table1_SDPT->getRow(index1)->get(Util::createString("price1"))->getString()<<",";
                    // cout<<table1_SDPT->getRow(index1)->get(Util::createString("price2"))->getString()<<endl<<endl;

                    EXPECT_EQ(msg->get(0)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("datetimev"))->getString());
                    EXPECT_EQ(msg->get(1)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("timestampv"))->getString());
                    EXPECT_EQ(msg->get(2)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("sym"))->getString());
                    EXPECT_EQ(msg->get(3)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("price1"))->getString());
                    EXPECT_EQ(msg->get(4)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("price2"))->getString());
                    index1++;
                }
                else if (symbol == "msg2")
                {
                    msg2_total += 1;
                    // cout<<"index2= "<<index2<<endl;
                    // cout<<msg->get(0)->getString()<<",";
                    // cout<<msg->get(1)->getString()<<",";
                    // cout<<msg->get(2)->getString()<<",";
                    // cout<<msg->get(3)->getString()<<endl;
                    // cout<<table2_SDPT->getRow(index2)->get(Util::createString("datetimev"))->getString()<<",";
                    // cout<<table2_SDPT->getRow(index2)->get(Util::createString("timestampv"))->getString()<<",";
                    // cout<<table2_SDPT->getRow(index2)->get(Util::createString("sym"))->getString()<<",";
                    // cout<<table2_SDPT->getRow(index2)->get(Util::createString("price1"))->getString()<<endl<<endl;

                    EXPECT_EQ(msg->get(0)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("datetimev"))->getString());
                    EXPECT_EQ(msg->get(1)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("timestampv"))->getString());
                    EXPECT_EQ(msg->get(2)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("sym"))->getString());
                    EXPECT_EQ(msg->get(3)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("price1"))->getString());
                    index2++;
                }
            }
        } });
        Util::sleep(1000);
        client.unsubscribe(hostName, port, "SDoutTables", "actionTest");
        th1.join();

        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }
    usedPorts.push_back(listenport);
}

TEST_P(StreamingDeserilizerTester, test_Threadpooledclient_threadCount_1_subscribeWithstreamDeserilizer)
{
    int msg1_total = 0, msg2_total = 0;
    int index1 = 0, index2 = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;
    StreamDeserializerSP sdsp = createStreamDeserializer();

    TableSP table1_SDPT = conn.run("table1_SDPT");
    TableSP table2_SDPT = conn.run("table2_SDPT");

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        LockGuard<Mutex> lock(&mutex);
        if (symbol == "msg1")
        {
            msg1_total += 1;
            EXPECT_EQ(msg->get(0)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("datetimev"))->getString());
            EXPECT_EQ(msg->get(1)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("timestampv"))->getString());
            EXPECT_EQ(msg->get(2)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("sym"))->getString());
            EXPECT_EQ(msg->get(3)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("price1"))->getString());
            EXPECT_EQ(msg->get(4)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("price2"))->getString());
            index1++;
        }
        else if (symbol == "msg2")
        {
            msg2_total += 1;
            EXPECT_EQ(msg->get(0)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("datetimev"))->getString());
            EXPECT_EQ(msg->get(1)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("timestampv"))->getString());
            EXPECT_EQ(msg->get(2)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("sym"))->getString());
            EXPECT_EQ(msg->get(3)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("price1"))->getString());
            index2++;
        }
        if (msg1_total + msg2_total == 2000)
        {
            notify.set();
        }
    };

    ThreadPooledClient client(listenport, 1);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp));
    }
    else
    {
        auto threadVec = client.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp);
        EXPECT_EQ(threadVec.size(), 1);
        notify.wait();

        client.unsubscribe(hostName, port, "SDoutTables", "test_SD");

        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.push_back(listenport);
}

TEST_P(StreamingDeserilizerTester, test_Threadpooledclient_threadCount_2_subscribeWithstreamDeserilizer)
{
    int msg1_total = 0;
    int msg2_total = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    StreamDeserializerSP sdsp = createStreamDeserializer();

    TableSP table1_SDPT = conn.run("table1_SDPT");
    TableSP table2_SDPT = conn.run("table2_SDPT");

    Signal notify;
    Mutex mutex;
    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        LockGuard<Mutex> lock(&mutex);
        if (symbol == "msg1")
        {
            msg1_total += 1;
        }
        else if (symbol == "msg2")
        {
            msg2_total += 1;
        }
        if (msg1_total + msg2_total == 2000)
        {
            notify.set();
        }
    };

    ThreadPooledClient client(listenport, 2);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp));
    }
    else
    {
        auto threadVec = client.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp);
        EXPECT_EQ(threadVec.size(), 2);
        notify.wait();

        client.unsubscribe(hostName, port, "SDoutTables", "test_SD");

        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }
    usedPorts.push_back(listenport);
}

TEST_P(StreamingDeserilizerTester, test_Threadclient_with_msgAsTable_True)
{
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    StreamDeserializerSP sdsp = createStreamDeserializer();

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        cout << symbol << endl;
    };

    ThreadedClient threadedClient(listenport);
    EXPECT_ANY_THROW(auto thread1 = threadedClient.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true, nullptr, true, false, "admin", "123456", sdsp));

    usedPorts.push_back(listenport);
}

TEST_P(StreamingDeserilizerTester, test_Pollingclient_with_msgAsTable_True)
{
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    StreamDeserializerSP sdsp = createStreamDeserializer();

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        cout << symbol << endl;
    };

    PollingClient client(listenport);
    EXPECT_ANY_THROW(client.subscribe(hostName, port, "SDoutTables", "actionTest", 0, true, nullptr, true, false, "admin", "123456", sdsp));

    usedPorts.push_back(listenport);
}

TEST_P(StreamingDeserilizerTester, test_Threadpooledclient_with_msgAsTable_True)
{
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    StreamDeserializerSP sdsp = createStreamDeserializer();

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        cout << symbol << endl;
    };

    ThreadPooledClient client(listenport, 1);

    EXPECT_ANY_THROW(auto threadVec = client.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true, nullptr, true, false, "admin", "123456", sdsp));

    usedPorts.push_back(listenport);
}

TEST_P(StreamingDeserilizerTester, test_Threadclient_onehandler_subscribeWithstreamDeserilizer_2)
{
    int msg1_total = 0, msg2_total = 0;
    int index1 = 0, index2 = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;
    StreamDeserializerSP sdsp = createStreamDeserializer_2();

    TableSP table1_SDPT = conn.run("table1_SDPT");
    TableSP table2_SDPT = conn.run("table2_SDPT");

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        LockGuard<Mutex> lock(&mutex);
        if (symbol == "msg1")
        {
            msg1_total += 1;
            EXPECT_EQ(msg->get(0)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("datetimev"))->getString());
            EXPECT_EQ(msg->get(1)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("timestampv"))->getString());
            EXPECT_EQ(msg->get(2)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("sym"))->getString());
            EXPECT_EQ(msg->get(3)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("price1"))->getString());
            EXPECT_EQ(msg->get(4)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("price2"))->getString());
            index1++;
        }
        else if (symbol == "msg2")
        {
            msg2_total += 1;
            EXPECT_EQ(msg->get(0)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("datetimev"))->getString());
            EXPECT_EQ(msg->get(1)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("timestampv"))->getString());
            EXPECT_EQ(msg->get(2)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("sym"))->getString());
            EXPECT_EQ(msg->get(3)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("price1"))->getString());
            index2++;
        }
        if (msg1_total + msg2_total == 2000)
        {
            notify.set();
        }
    };

    ThreadedClient threadedClient(listenport);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp));
    }
    else
    {
        auto thread1 = threadedClient.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp);
        thread1->setAffinity(0);
        notify.wait();

        threadedClient.unsubscribe(hostName, port, "SDoutTables", "test_SD");

        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.push_back(listenport);
}

TEST_P(StreamingDeserilizerTester, test_Threadclient_batchhandler_subscribeWithstreamDeserilizer_2)
{
    int msg1_total = 0, msg2_total = 0;
    int index1 = 0, index2 = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    Signal notify;
    Mutex mutex;
    StreamDeserializerSP sdsp = createStreamDeserializer_2();

    TableSP table1_SDPT = conn.run("table1_SDPT");
    TableSP table2_SDPT = conn.run("table2_SDPT");

    auto batchhandler = [&](vector<Message> msgs)
    {
        LockGuard<Mutex> lock(&mutex);
        for (auto &msg : msgs)
        {
            const string &symbol = msg.getSymbol();
            if (symbol == "msg1")
            {
                msg1_total += 1;
                // cout<<"index1= "<<index1<<endl;
                // cout<<msg->get(0)->getString()<<",";
                // cout<<msg->get(1)->getString()<<",";
                // cout<<msg->get(2)->getString()<<",";
                // cout<<msg->get(3)->getString()<<",";
                // cout<<msg->get(4)->getString()<<endl;
                // cout<<table1_SDPT->getRow(index1)->get(Util::createString("datetimev"))->getString()<<",";
                // cout<<table1_SDPT->getRow(index1)->get(Util::createString("timestampv"))->getString()<<",";
                // cout<<table1_SDPT->getRow(index1)->get(Util::createString("sym"))->getString()<<",";
                // cout<<table1_SDPT->getRow(index1)->get(Util::createString("price1"))->getString()<<",";
                // cout<<table1_SDPT->getRow(index1)->get(Util::createString("price2"))->getString()<<endl<<endl;

                // EXPECT_EQ(msg->get(0)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("datetimev"))->getString());
                // EXPECT_EQ(msg->get(1)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("timestampv"))->getString());
                // EXPECT_EQ(msg->get(2)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("sym"))->getString());
                // EXPECT_EQ(msg->get(3)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("price1"))->getString());
                // EXPECT_EQ(msg->get(4)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("price2"))->getString());
                index1++;
            }
            else if (symbol == "msg2")
            {
                msg2_total += 1;
                // cout<<"index2= "<<index2<<endl;
                // cout<<msg->get(0)->getString()<<",";
                // cout<<msg->get(1)->getString()<<",";
                // cout<<msg->get(2)->getString()<<",";
                // cout<<msg->get(3)->getString()<<endl;
                // cout<<table2_SDPT->getRow(index2)->get(Util::createString("datetimev"))->getString()<<",";
                // cout<<table2_SDPT->getRow(index2)->get(Util::createString("timestampv"))->getString()<<",";
                // cout<<table2_SDPT->getRow(index2)->get(Util::createString("sym"))->getString()<<",";
                // cout<<table2_SDPT->getRow(index2)->get(Util::createString("price1"))->getString()<<endl<<endl;

                // EXPECT_EQ(msg->get(0)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("datetimev"))->getString());
                // EXPECT_EQ(msg->get(1)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("timestampv"))->getString());
                // EXPECT_EQ(msg->get(2)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("sym"))->getString());
                // EXPECT_EQ(msg->get(3)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("price1"))->getString());
                index2++;
            }
            if (msg1_total + msg2_total == 2000)
            {
                notify.set();
            }
        }
    };

    ThreadedClient threadedClient(listenport);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, "SDoutTables", "mutiSchemaBatch", 0, true, nullptr, true, 1, 1.0, false, "admin", "123456", sdsp));
    }
    else
    {
        auto thread2 = threadedClient.subscribe(hostName, port, batchhandler, "SDoutTables", "mutiSchemaBatch", 0, true, nullptr, true, 1, 1.0, false, "admin", "123456", sdsp);
        thread2->setAffinity(0);
        notify.wait();

        threadedClient.unsubscribe(hostName, port, "SDoutTables", "mutiSchemaBatch");

        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.push_back(listenport);
}

TEST_P(StreamingDeserilizerTester, test_Pollingclient_subscribeWithstreamDeserilizer_2)
{
    int msg1_total = 0, msg2_total = 0;
    int index1 = 0, index2 = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    StreamDeserializerSP sdsp = createStreamDeserializer_2();

    TableSP table1_SDPT = conn.run("table1_SDPT");
    TableSP table2_SDPT = conn.run("table2_SDPT");

    PollingClient client(listenport);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(hostName, port, "SDoutTables", "actionTest", 0, true, nullptr, false, false, "admin", "123456", sdsp));
    }
    else
    {
        auto queue = client.subscribe(hostName, port, "SDoutTables", "actionTest", 0, true, nullptr, false, false, "admin", "123456", sdsp);

        Message msg;
        thread th1 = thread([&]
                            {
        while (true)
        {
            queue->pop(msg);
            // cout<<msg->getString()<<endl;
            if(msg.isNull()) {break;}
            else{
                const string &symbol = msg.getSymbol();
                if (symbol == "msg1")
                {
                    msg1_total += 1;
                    // cout<<"index1= "<<index1<<endl;
                    // cout<<msg->get(0)->getString()<<",";
                    // cout<<msg->get(1)->getString()<<",";
                    // cout<<msg->get(2)->getString()<<",";
                    // cout<<msg->get(3)->getString()<<",";
                    // cout<<msg->get(4)->getString()<<endl;
                    // cout<<table1_SDPT->getRow(index1)->get(Util::createString("datetimev"))->getString()<<",";
                    // cout<<table1_SDPT->getRow(index1)->get(Util::createString("timestampv"))->getString()<<",";
                    // cout<<table1_SDPT->getRow(index1)->get(Util::createString("sym"))->getString()<<",";
                    // cout<<table1_SDPT->getRow(index1)->get(Util::createString("price1"))->getString()<<",";
                    // cout<<table1_SDPT->getRow(index1)->get(Util::createString("price2"))->getString()<<endl<<endl;

                    EXPECT_EQ(msg->get(0)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("datetimev"))->getString());
                    EXPECT_EQ(msg->get(1)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("timestampv"))->getString());
                    EXPECT_EQ(msg->get(2)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("sym"))->getString());
                    EXPECT_EQ(msg->get(3)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("price1"))->getString());
                    EXPECT_EQ(msg->get(4)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("price2"))->getString());
                    index1++;
                }
                else if (symbol == "msg2")
                {
                    msg2_total += 1;
                    // cout<<"index2= "<<index2<<endl;
                    // cout<<msg->get(0)->getString()<<",";
                    // cout<<msg->get(1)->getString()<<",";
                    // cout<<msg->get(2)->getString()<<",";
                    // cout<<msg->get(3)->getString()<<endl;
                    // cout<<table2_SDPT->getRow(index2)->get(Util::createString("datetimev"))->getString()<<",";
                    // cout<<table2_SDPT->getRow(index2)->get(Util::createString("timestampv"))->getString()<<",";
                    // cout<<table2_SDPT->getRow(index2)->get(Util::createString("sym"))->getString()<<",";
                    // cout<<table2_SDPT->getRow(index2)->get(Util::createString("price1"))->getString()<<endl<<endl;

                    EXPECT_EQ(msg->get(0)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("datetimev"))->getString());
                    EXPECT_EQ(msg->get(1)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("timestampv"))->getString());
                    EXPECT_EQ(msg->get(2)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("sym"))->getString());
                    EXPECT_EQ(msg->get(3)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("price1"))->getString());
                    index2++;
                }
            }
        } });
        Util::sleep(1000);
        client.unsubscribe(hostName, port, "SDoutTables", "actionTest");
        th1.join();

        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.push_back(listenport);
}

TEST_P(StreamingDeserilizerTester, test_Threadpooledclient_threadCount_1_subscribeWithstreamDeserilizer_2)
{
    int msg1_total = 0, msg2_total = 0;
    int index1 = 0, index2 = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;
    StreamDeserializerSP sdsp = createStreamDeserializer_2();

    TableSP table1_SDPT = conn.run("table1_SDPT");
    TableSP table2_SDPT = conn.run("table2_SDPT");

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        LockGuard<Mutex> lock(&mutex);
        if (symbol == "msg1")
        {
            msg1_total += 1;
            EXPECT_EQ(msg->get(0)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("datetimev"))->getString());
            EXPECT_EQ(msg->get(1)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("timestampv"))->getString());
            EXPECT_EQ(msg->get(2)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("sym"))->getString());
            EXPECT_EQ(msg->get(3)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("price1"))->getString());
            EXPECT_EQ(msg->get(4)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("price2"))->getString());
            index1++;
        }
        else if (symbol == "msg2")
        {
            msg2_total += 1;
            EXPECT_EQ(msg->get(0)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("datetimev"))->getString());
            EXPECT_EQ(msg->get(1)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("timestampv"))->getString());
            EXPECT_EQ(msg->get(2)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("sym"))->getString());
            EXPECT_EQ(msg->get(3)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("price1"))->getString());
            index2++;
        }
        if (msg1_total + msg2_total == 2000)
        {
            notify.set();
        }
    };

    ThreadPooledClient client(listenport, 1);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp));
    }
    else
    {
        auto threadVec = client.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp);
        EXPECT_EQ(threadVec.size(), 1);
        notify.wait();

        client.unsubscribe(hostName, port, "SDoutTables", "test_SD");

        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.push_back(listenport);
}

TEST_P(StreamingDeserilizerTester, test_Threadpooledclient_threadCount_2_subscribeWithstreamDeserilizer_2)
{
    int msg1_total = 0;
    int msg2_total = 0;

    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    StreamDeserializerSP sdsp = createStreamDeserializer_2();

    TableSP table1_SDPT = conn.run("table1_SDPT");
    TableSP table2_SDPT = conn.run("table2_SDPT");

    Signal notify;
    Mutex mutex;
    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        LockGuard<Mutex> lock(&mutex);
        if (symbol == "msg1")
        {
            msg1_total += 1;
        }
        else if (symbol == "msg2")
        {
            msg2_total += 1;
        }
        if (msg1_total + msg2_total == 2000)
        {
            notify.set();
        }
    };

    ThreadPooledClient client(listenport, 2);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp));
    }
    else
    {
        auto threadVec = client.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp);
        EXPECT_EQ(threadVec.size(), 2);
        notify.wait();

        client.unsubscribe(hostName, port, "SDoutTables", "test_SD");

        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }
    usedPorts.push_back(listenport);
}

TEST_P(StreamingDeserilizerTester, test_Threadclient_with_msgAsTable_True_2)
{
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    StreamDeserializerSP sdsp = createStreamDeserializer_2();

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        cout << symbol << endl;
    };

    ThreadedClient threadedClient(listenport);
    EXPECT_ANY_THROW(auto thread1 = threadedClient.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true, nullptr, true, false, "admin", "123456", sdsp));

    usedPorts.push_back(listenport);
}

TEST_P(StreamingDeserilizerTester, test_Pollingclient_with_msgAsTable_True_2)
{
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    StreamDeserializerSP sdsp = createStreamDeserializer_2();

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        cout << symbol << endl;
    };

    PollingClient client(listenport);
    EXPECT_ANY_THROW(client.subscribe(hostName, port, "SDoutTables", "actionTest", 0, true, nullptr, true, false, "admin", "123456", sdsp));

    usedPorts.push_back(listenport);
}

TEST_P(StreamingDeserilizerTester, test_Threadpooledclient_with_msgAsTable_True_2)
{
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    StreamDeserializerSP sdsp = createStreamDeserializer_2();

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        cout << symbol << endl;
    };

    ThreadPooledClient client(listenport, 1);

    EXPECT_ANY_THROW(auto threadVec = client.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true, nullptr, true, false, "admin", "123456", sdsp));

    usedPorts.push_back(listenport);
}

TEST_P(StreamingDeserilizerTester, test_Threadclient_onehandler_subscribeWithstreamDeserilizer_3)
{
    int msg1_total = 0, msg2_total = 0;
    int index1 = 0, index2 = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;
    StreamDeserializerSP sdsp = createStreamDeserializer_3();

    TableSP table1_SDPT = conn.run("table1_SDPT");
    TableSP table2_SDPT = conn.run("table2_SDPT");

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        LockGuard<Mutex> lock(&mutex);
        if (symbol == "msg1")
        {
            msg1_total += 1;
            EXPECT_EQ(msg->get(0)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("datetimev"))->getString());
            EXPECT_EQ(msg->get(1)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("timestampv"))->getString());
            EXPECT_EQ(msg->get(2)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("sym"))->getString());
            EXPECT_EQ(msg->get(3)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("price1"))->getString());
            EXPECT_EQ(msg->get(4)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("price2"))->getString());
            index1++;
        }
        else if (symbol == "msg2")
        {
            msg2_total += 1;
            // EXPECT_EQ(msg->get(0)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("datetimev"))->getString());
            // EXPECT_EQ(msg->get(1)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("timestampv"))->getString());
            // EXPECT_EQ(msg->get(2)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("sym"))->getString());
            // EXPECT_EQ(msg->get(3)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("price1"))->getString());
            index2++;
        }
        if (msg1_total + msg2_total == 2000)
        {
            notify.set();
        }
    };

    ThreadedClient threadedClient(listenport);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp));
    }
    else
    {
        auto thread1 = threadedClient.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp);
        thread1->setAffinity(0);
        notify.wait();

        threadedClient.unsubscribe(hostName, port, "SDoutTables", "test_SD");

        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.push_back(listenport);
}

TEST_P(StreamingDeserilizerTester, test_Threadclient_batchhandler_subscribeWithstreamDeserilizer_3)
{
    int msg1_total = 0, msg2_total = 0;
    int index1 = 0, index2 = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    Signal notify;
    Mutex mutex;
    StreamDeserializerSP sdsp = createStreamDeserializer_3();

    TableSP table1_SDPT = conn.run("table1_SDPT");
    TableSP table2_SDPT = conn.run("select * from loadTable(dbpath,`table2_SDPT)");

    auto batchhandler = [&](vector<Message> msgs)
    {
        LockGuard<Mutex> lock(&mutex);
        for (auto &msg : msgs)
        {
            const string &symbol = msg.getSymbol();
            if (symbol == "msg1")
            {
                msg1_total += 1;
                // cout<<"index1= "<<index1<<endl;
                // cout<<msg->get(0)->getString()<<",";
                // cout<<msg->get(1)->getString()<<",";
                // cout<<msg->get(2)->getString()<<",";
                // cout<<msg->get(3)->getString()<<",";
                // cout<<msg->get(4)->getString()<<endl;
                // cout<<table1_SDPT->getRow(index1)->get(Util::createString("datetimev"))->getString()<<",";
                // cout<<table1_SDPT->getRow(index1)->get(Util::createString("timestampv"))->getString()<<",";
                // cout<<table1_SDPT->getRow(index1)->get(Util::createString("sym"))->getString()<<",";
                // cout<<table1_SDPT->getRow(index1)->get(Util::createString("price1"))->getString()<<",";
                // cout<<table1_SDPT->getRow(index1)->get(Util::createString("price2"))->getString()<<endl<<endl;

                // EXPECT_EQ(msg->get(0)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("datetimev"))->getString());
                // EXPECT_EQ(msg->get(1)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("timestampv"))->getString());
                // EXPECT_EQ(msg->get(2)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("sym"))->getString());
                // EXPECT_EQ(msg->get(3)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("price1"))->getString());
                // EXPECT_EQ(msg->get(4)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("price2"))->getString());
                index1++;
            }
            else if (symbol == "msg2")
            {
                msg2_total += 1;
                // cout<<"index2= "<<index2<<endl;
                // cout<<msg->get(0)->getString()<<",";
                // cout<<msg->get(1)->getString()<<",";
                // cout<<msg->get(2)->getString()<<",";
                // cout<<msg->get(3)->getString()<<endl;
                // cout<<table2_SDPT->getRow(index2)->get(Util::createString("datetimev"))->getString()<<",";
                // cout<<table2_SDPT->getRow(index2)->get(Util::createString("timestampv"))->getString()<<",";
                // cout<<table2_SDPT->getRow(index2)->get(Util::createString("sym"))->getString()<<",";
                // cout<<table2_SDPT->getRow(index2)->get(Util::createString("price1"))->getString()<<endl<<endl;

                // EXPECT_EQ(msg->get(0)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("datetimev"))->getString());
                // EXPECT_EQ(msg->get(1)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("timestampv"))->getString());
                // EXPECT_EQ(msg->get(2)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("sym"))->getString());
                // EXPECT_EQ(msg->get(3)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("price1"))->getString());
                index2++;
            }
            if (msg1_total + msg2_total == 2000)
            {
                notify.set();
            }
        }
    };

    ThreadedClient threadedClient(listenport);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, "SDoutTables", "mutiSchemaBatch", 0, true, nullptr, true, 1, 1.0, false, "admin", "123456", sdsp));
    }
    else
    {
        auto thread2 = threadedClient.subscribe(hostName, port, batchhandler, "SDoutTables", "mutiSchemaBatch", 0, true, nullptr, true, 1, 1.0, false, "admin", "123456", sdsp);
        thread2->setAffinity(0);
        notify.wait();

        threadedClient.unsubscribe(hostName, port, "SDoutTables", "mutiSchemaBatch");

        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.push_back(listenport);
}

TEST_P(StreamingDeserilizerTester, test_Pollingclient_subscribeWithstreamDeserilizer_3)
{
    int msg1_total = 0, msg2_total = 0;
    int index1 = 0, index2 = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    StreamDeserializerSP sdsp = createStreamDeserializer_3();

    TableSP table1_SDPT = conn.run("table1_SDPT");
    TableSP table2_SDPT = conn.run("select * from loadTable(dbpath,`table2_SDPT)");

    PollingClient client(listenport);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(hostName, port, "SDoutTables", "actionTest", 0, true, nullptr, false, false, "admin", "123456", sdsp));
    }
    else
    {
        auto queue = client.subscribe(hostName, port, "SDoutTables", "actionTest", 0, true, nullptr, false, false, "admin", "123456", sdsp);

        Message msg;
        thread th1 = thread([&]
                            {
        while (true)
        {
            queue->pop(msg);
            // cout<<msg->getString()<<endl;
            if(msg.isNull()) {break;}
            else{
                const string &symbol = msg.getSymbol();
                if (symbol == "msg1")
                {
                    msg1_total += 1;
                    // cout<<"index1= "<<index1<<endl;
                    // cout<<msg->get(0)->getString()<<",";
                    // cout<<msg->get(1)->getString()<<",";
                    // cout<<msg->get(2)->getString()<<",";
                    // cout<<msg->get(3)->getString()<<",";
                    // cout<<msg->get(4)->getString()<<endl;
                    // cout<<table1_SDPT->getRow(index1)->get(Util::createString("datetimev"))->getString()<<",";
                    // cout<<table1_SDPT->getRow(index1)->get(Util::createString("timestampv"))->getString()<<",";
                    // cout<<table1_SDPT->getRow(index1)->get(Util::createString("sym"))->getString()<<",";
                    // cout<<table1_SDPT->getRow(index1)->get(Util::createString("price1"))->getString()<<",";
                    // cout<<table1_SDPT->getRow(index1)->get(Util::createString("price2"))->getString()<<endl<<endl;

                    EXPECT_EQ(msg->get(0)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("datetimev"))->getString());
                    EXPECT_EQ(msg->get(1)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("timestampv"))->getString());
                    EXPECT_EQ(msg->get(2)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("sym"))->getString());
                    EXPECT_EQ(msg->get(3)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("price1"))->getString());
                    EXPECT_EQ(msg->get(4)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("price2"))->getString());
                    index1++;
                }
                else if (symbol == "msg2")
                {
                    msg2_total += 1;
                    // cout<<"index2= "<<index2<<endl;
                    // cout<<msg->get(0)->getString()<<",";
                    // cout<<msg->get(1)->getString()<<",";
                    // cout<<msg->get(2)->getString()<<",";
                    // cout<<msg->get(3)->getString()<<endl;
                    // cout<<table2_SDPT->getRow(index2)->get(Util::createString("datetimev"))->getString()<<",";
                    // cout<<table2_SDPT->getRow(index2)->get(Util::createString("timestampv"))->getString()<<",";
                    // cout<<table2_SDPT->getRow(index2)->get(Util::createString("sym"))->getString()<<",";
                    // cout<<table2_SDPT->getRow(index2)->get(Util::createString("price1"))->getString()<<endl<<endl;

                    // EXPECT_EQ(msg->get(0)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("datetimev"))->getString());
                    // EXPECT_EQ(msg->get(1)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("timestampv"))->getString());
                    // EXPECT_EQ(msg->get(2)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("sym"))->getString());
                    // EXPECT_EQ(msg->get(3)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("price1"))->getString());
                    index2++;
                }
            }
        } });
        Util::sleep(1000);
        client.unsubscribe(hostName, port, "SDoutTables", "actionTest");
        th1.join();

        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.push_back(listenport);
}

TEST_P(StreamingDeserilizerTester, test_Threadpooledclient_threadCount_1_subscribeWithstreamDeserilizer_3)
{
    int msg1_total = 0, msg2_total = 0;
    int index1 = 0, index2 = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;
    StreamDeserializerSP sdsp = createStreamDeserializer_3();

    TableSP table1_SDPT = conn.run("table1_SDPT");
    TableSP table2_SDPT = conn.run("select * from loadTable(dbpath,`table2_SDPT)");

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        LockGuard<Mutex> lock(&mutex);
        if (symbol == "msg1")
        {
            msg1_total += 1;
            EXPECT_EQ(msg->get(0)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("datetimev"))->getString());
            EXPECT_EQ(msg->get(1)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("timestampv"))->getString());
            EXPECT_EQ(msg->get(2)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("sym"))->getString());
            EXPECT_EQ(msg->get(3)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("price1"))->getString());
            EXPECT_EQ(msg->get(4)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("price2"))->getString());
            index1++;
        }
        else if (symbol == "msg2")
        {
            msg2_total += 1;
            // EXPECT_EQ(msg->get(0)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("datetimev"))->getString());
            // EXPECT_EQ(msg->get(1)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("timestampv"))->getString());
            // EXPECT_EQ(msg->get(2)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("sym"))->getString());
            // EXPECT_EQ(msg->get(3)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("price1"))->getString());
            index2++;
        }
        if (msg1_total + msg2_total == 2000)
        {
            notify.set();
        }
    };

    ThreadPooledClient client(listenport, 1);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp));
    }
    else
    {
        auto threadVec = client.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp);
        EXPECT_EQ(threadVec.size(), 1);
        notify.wait();

        client.unsubscribe(hostName, port, "SDoutTables", "test_SD");

        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.push_back(listenport);
}

TEST_P(StreamingDeserilizerTester, test_Threadpooledclient_threadCount_3_subscribeWithstreamDeserilizer_3)
{
    int msg1_total = 0;
    int msg2_total = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    StreamDeserializerSP sdsp = createStreamDeserializer_3();

    TableSP table1_SDPT = conn.run("table1_SDPT");
    TableSP table2_SDPT = conn.run("select * from loadTable(dbpath,`table2_SDPT)");

    Signal notify;
    Mutex mutex;
    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        LockGuard<Mutex> lock(&mutex);
        if (symbol == "msg1")
        {
            msg1_total += 1;
        }
        else if (symbol == "msg2")
        {
            msg2_total += 1;
        }
        if (msg1_total + msg2_total == 2000)
        {
            notify.set();
        }
    };

    ThreadPooledClient client(listenport, 2);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp));
    }
    else
    {
        auto threadVec = client.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp);
        EXPECT_EQ(threadVec.size(), 2);
        notify.wait();

        client.unsubscribe(hostName, port, "SDoutTables", "test_SD");

        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }
    usedPorts.push_back(listenport);
}

TEST_P(StreamingDeserilizerTester, test_Threadclient_with_msgAsTable_True_3)
{
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    StreamDeserializerSP sdsp = createStreamDeserializer_3();

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        cout << symbol << endl;
    };

    ThreadedClient threadedClient(listenport);
    EXPECT_ANY_THROW(auto thread1 = threadedClient.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true, nullptr, true, false, "admin", "123456", sdsp));

    usedPorts.push_back(listenport);
}

TEST_P(StreamingDeserilizerTester, test_Pollingclient_with_msgAsTable_True_3)
{
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    StreamDeserializerSP sdsp = createStreamDeserializer_3();

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        cout << symbol << endl;
    };

    PollingClient client(listenport);
    EXPECT_ANY_THROW(client.subscribe(hostName, port, "SDoutTables", "actionTest", 0, true, nullptr, true, false, "admin", "123456", sdsp));

    usedPorts.push_back(listenport);
}

TEST_P(StreamingDeserilizerTester, test_Threadpooledclient_with_msgAsTable_True_3)
{
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    StreamDeserializerSP sdsp = createStreamDeserializer_3();

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        cout << symbol << endl;
    };

    ThreadPooledClient client(listenport, 1);

    EXPECT_ANY_THROW(auto threadVec = client.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true, nullptr, true, false, "admin", "123456", sdsp));

    usedPorts.push_back(listenport);
}

TEST_P(StreamingDeserilizerTester, test_Threadclient_onehandler_subscribeWithstreamDeserilizer_arrayVector)
{
    int msg1_total = 0, msg2_total = 0;
    int index1 = 0, index2 = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;
    StreamDeserializerSP sdsp = createStreamDeserializer_witharrayVector();

    TableSP table1_SDPT = conn.run("table1_SDPT");
    TableSP table2_SDPT = conn.run("table2_SDPT");

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        LockGuard<Mutex> lock(&mutex);
        if (symbol == "msg1")
        {
            msg1_total += 1;
            EXPECT_EQ(msg->get(4)->getType(), DT_DOUBLE_ARRAY);
            // cout<<msg->get(0)->getString()<<",";
            // cout<<msg->get(1)->getString()<<",";
            // cout<<msg->get(2)->getString()<<",";
            // cout<<msg->get(3)->getString()<<",";
            // cout<<msg->get(4)->getString()<<endl;
            // cout<<table1_SDPT->getRow(index1)->get(Util::createString("datetimev"))->getString()<<",";
            // cout<<table1_SDPT->getRow(index1)->get(Util::createString("timestampv"))->getString()<<",";
            // cout<<table1_SDPT->getRow(index1)->get(Util::createString("sym"))->getString()<<",";
            // cout<<table1_SDPT->getRow(index1)->get(Util::createString("price1"))->getString()<<",";
            // cout<<table1_SDPT->getRow(index1)->get(Util::createString("price2"))->getString()<<endl<<endl;
            EXPECT_EQ(msg->get(0)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("datetimev"))->getString());
            EXPECT_EQ(msg->get(1)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("timestampv"))->getString());
            EXPECT_EQ(msg->get(2)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("sym"))->getString());
            EXPECT_EQ(msg->get(3)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("price1"))->getString());
            EXPECT_EQ(msg->get(4)->get(0)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("price2"))->getString());
            index1++;
        }
        else if (symbol == "msg2")
        {
            msg2_total += 1;
            // cout<<msg->get(0)->getString()<<",";
            // cout<<msg->get(1)->getString()<<",";
            // cout<<msg->get(2)->getString()<<",";
            // cout<<msg->get(3)->getString()<<endl;
            // cout<<table2_SDPT->getRow(index2)->get(Util::createString("datetimev"))->getString()<<",";
            // cout<<table2_SDPT->getRow(index2)->get(Util::createString("timestampv"))->getString()<<",";
            // cout<<table2_SDPT->getRow(index2)->get(Util::createString("sym"))->getString()<<",";
            // cout<<table2_SDPT->getRow(index2)->get(Util::createString("price1"))->getString()<<endl<<endl;
            EXPECT_EQ(msg->get(0)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("datetimev"))->getString());
            EXPECT_EQ(msg->get(1)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("timestampv"))->getString());
            EXPECT_EQ(msg->get(2)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("sym"))->getString());
            EXPECT_EQ(msg->get(3)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("price1"))->getString());
            index2++;
        }
        if (msg1_total + msg2_total == 2000)
        {
            notify.set();
        }
    };

    ThreadedClient threadedClient(listenport);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp));
    }
    else
    {
        auto thread1 = threadedClient.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp);
        thread1->setAffinity(0);
        notify.wait();

        threadedClient.unsubscribe(hostName, port, "SDoutTables", "test_SD");

        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.push_back(listenport);
}

TEST_P(StreamingDeserilizerTester, test_Threadclient_batchhandler_subscribeWithstreamDeserilizer_arrayVector)
{
    int msg1_total = 0, msg2_total = 0;
    int index1 = 0, index2 = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    Signal notify;
    Mutex mutex;
    StreamDeserializerSP sdsp = createStreamDeserializer_witharrayVector();

    TableSP table1_SDPT = conn.run("table1_SDPT");
    TableSP table2_SDPT = conn.run("table2_SDPT");

    auto batchhandler = [&](vector<Message> msgs)
    {
        LockGuard<Mutex> lock(&mutex);
        for (auto &msg : msgs)
        {
            const string &symbol = msg.getSymbol();
            if (symbol == "msg1")
            {
                msg1_total += 1;
                EXPECT_EQ(msg->get(4)->getType(), DT_DOUBLE_ARRAY);
                // cout<<"index1= "<<index1<<endl;
                // cout<<msg->get(0)->getString()<<",";
                // cout<<msg->get(1)->getString()<<",";
                // cout<<msg->get(2)->getString()<<",";
                // cout<<msg->get(3)->getString()<<",";
                // cout<<msg->get(4)->getString()<<endl;
                // cout<<table1_SDPT->getRow(index1)->get(Util::createString("datetimev"))->getString()<<",";
                // cout<<table1_SDPT->getRow(index1)->get(Util::createString("timestampv"))->getString()<<",";
                // cout<<table1_SDPT->getRow(index1)->get(Util::createString("sym"))->getString()<<",";
                // cout<<table1_SDPT->getRow(index1)->get(Util::createString("price1"))->getString()<<",";
                // cout<<table1_SDPT->getRow(index1)->get(Util::createString("price2"))->getString()<<endl<<endl;

                // EXPECT_EQ(msg->get(0)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("datetimev"))->getString());
                // EXPECT_EQ(msg->get(1)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("timestampv"))->getString());
                // EXPECT_EQ(msg->get(2)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("sym"))->getString());
                // EXPECT_EQ(msg->get(3)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("price1"))->getString());
                // EXPECT_EQ(msg->get(4)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("price2"))->getString());
                index1++;
            }
            else if (symbol == "msg2")
            {
                msg2_total += 1;
                // cout<<"index2= "<<index2<<endl;
                // cout<<msg->get(0)->getString()<<",";
                // cout<<msg->get(1)->getString()<<",";
                // cout<<msg->get(2)->getString()<<",";
                // cout<<msg->get(3)->getString()<<endl;
                // cout<<table2_SDPT->getRow(index2)->get(Util::createString("datetimev"))->getString()<<",";
                // cout<<table2_SDPT->getRow(index2)->get(Util::createString("timestampv"))->getString()<<",";
                // cout<<table2_SDPT->getRow(index2)->get(Util::createString("sym"))->getString()<<",";
                // cout<<table2_SDPT->getRow(index2)->get(Util::createString("price1"))->getString()<<endl<<endl;

                // EXPECT_EQ(msg->get(0)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("datetimev"))->getString());
                // EXPECT_EQ(msg->get(1)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("timestampv"))->getString());
                // EXPECT_EQ(msg->get(2)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("sym"))->getString());
                // EXPECT_EQ(msg->get(3)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("price1"))->getString());
                index2++;
            }
            if (msg1_total + msg2_total == 2000)
            {
                notify.set();
            }
        }
    };

    ThreadedClient threadedClient(listenport);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, "SDoutTables", "mutiSchemaBatch", 0, true, nullptr, true, 1, 1.0, false, "admin", "123456", sdsp));
    }
    else
    {
        auto thread2 = threadedClient.subscribe(hostName, port, batchhandler, "SDoutTables", "mutiSchemaBatch", 0, true, nullptr, true, 1, 1.0, false, "admin", "123456", sdsp);
        thread2->setAffinity(0);
        notify.wait();

        threadedClient.unsubscribe(hostName, port, "SDoutTables", "mutiSchemaBatch");

        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.push_back(listenport);
}

TEST_P(StreamingDeserilizerTester, test_Pollingclient_subscribeWithstreamDeserilizer_arrayVector)
{
    int msg1_total = 0, msg2_total = 0;
    int index1 = 0, index2 = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    StreamDeserializerSP sdsp = createStreamDeserializer_witharrayVector();

    TableSP table1_SDPT = conn.run("table1_SDPT");
    TableSP table2_SDPT = conn.run("table2_SDPT");

    PollingClient client(listenport);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(hostName, port, "SDoutTables", "actionTest", 0, true, nullptr, false, false, "admin", "123456", sdsp));
    }
    else
    {
        auto queue = client.subscribe(hostName, port, "SDoutTables", "actionTest", 0, true, nullptr, false, false, "admin", "123456", sdsp);

        Message msg;
        thread th1 = thread([&]
                            {
        while (true)
        {
            queue->pop(msg);
            // cout<<msg->getString()<<endl;
            if(msg.isNull()) {break;}
            else{
                const string &symbol = msg.getSymbol();
                if (symbol == "msg1")
                {
                    msg1_total += 1;
                    EXPECT_EQ(msg->get(4)->getType(), DT_DOUBLE_ARRAY);
                    // cout<<"index1= "<<index1<<endl;
                    // cout<<msg->get(0)->getString()<<",";
                    // cout<<msg->get(1)->getString()<<",";
                    // cout<<msg->get(2)->getString()<<",";
                    // cout<<msg->get(3)->getString()<<",";
                    // cout<<msg->get(4)->getString()<<endl;
                    // cout<<table1_SDPT->getRow(index1)->get(Util::createString("datetimev"))->getString()<<",";
                    // cout<<table1_SDPT->getRow(index1)->get(Util::createString("timestampv"))->getString()<<",";
                    // cout<<table1_SDPT->getRow(index1)->get(Util::createString("sym"))->getString()<<",";
                    // cout<<table1_SDPT->getRow(index1)->get(Util::createString("price1"))->getString()<<",";
                    // cout<<table1_SDPT->getRow(index1)->get(Util::createString("price2"))->getString()<<endl<<endl;

                    EXPECT_EQ(msg->get(0)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("datetimev"))->getString());
                    EXPECT_EQ(msg->get(1)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("timestampv"))->getString());
                    EXPECT_EQ(msg->get(2)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("sym"))->getString());
                    EXPECT_EQ(msg->get(3)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("price1"))->getString());
                    EXPECT_EQ(msg->get(4)->get(0)->getString(), table1_SDPT->getRow(index1)->get(Util::createString("price2"))->getString());
                    index1++;
                }
                else if (symbol == "msg2")
                {
                    msg2_total += 1;
                    // cout<<"index2= "<<index2<<endl;
                    // cout<<msg->get(0)->getString()<<",";
                    // cout<<msg->get(1)->getString()<<",";
                    // cout<<msg->get(2)->getString()<<",";
                    // cout<<msg->get(3)->getString()<<endl;
                    // cout<<table2_SDPT->getRow(index2)->get(Util::createString("datetimev"))->getString()<<",";
                    // cout<<table2_SDPT->getRow(index2)->get(Util::createString("timestampv"))->getString()<<",";
                    // cout<<table2_SDPT->getRow(index2)->get(Util::createString("sym"))->getString()<<",";
                    // cout<<table2_SDPT->getRow(index2)->get(Util::createString("price1"))->getString()<<endl<<endl;

                    EXPECT_EQ(msg->get(0)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("datetimev"))->getString());
                    EXPECT_EQ(msg->get(1)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("timestampv"))->getString());
                    EXPECT_EQ(msg->get(2)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("sym"))->getString());
                    EXPECT_EQ(msg->get(3)->getString(), table2_SDPT->getRow(index2)->get(Util::createString("price1"))->getString());
                    index2++;
                }
            }
        } });
        Util::sleep(1000);
        client.unsubscribe(hostName, port, "SDoutTables", "actionTest");
        th1.join();

        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.push_back(listenport);
}

TEST_P(StreamingDeserilizerTester, test_Threadpooledclient_subscribeWithstreamDeserilizer_arrayVector)
{
    int msg1_total = 0, msg2_total = 0;
    int index1 = 0, index2 = 0;
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;
    StreamDeserializerSP sdsp = createStreamDeserializer_witharrayVector();

    TableSP table1_SDPT = conn.run("table1_SDPT");
    TableSP table2_SDPT = conn.run("table2_SDPT");

    auto onehandler = [&](Message msg)
    {
        const string &symbol = msg.getSymbol();
        LockGuard<Mutex> lock(&mutex);
        if (symbol == "msg1")
        {
            msg1_total += 1;
            EXPECT_EQ(msg->get(4)->getType(), DT_DOUBLE_ARRAY);
            // EXPECT_EQ(msg->get(0)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("datetimev"))->getString());
            // EXPECT_EQ(msg->get(1)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("timestampv"))->getString());
            // EXPECT_EQ(msg->get(2)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("sym"))->getString());
            // EXPECT_EQ(msg->get(3)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("price1"))->getString());
            // EXPECT_EQ(msg->get(4)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("price2"))->getString());
            index1++;
        }
        else if (symbol == "msg2")
        {
            msg2_total += 1;
            // EXPECT_EQ(msg->get(0)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("datetimev"))->getString());
            // EXPECT_EQ(msg->get(1)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("timestampv"))->getString());
            // EXPECT_EQ(msg->get(2)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("sym"))->getString());
            // EXPECT_EQ(msg->get(3)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("price1"))->getString());
            index2++;
        }
        if (msg1_total + msg2_total == 2000)
        {
            notify.set();
        }
    };
    int threadCount = rand() % 10 + 1;
    ThreadPooledClient client(listenport, threadCount);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp));
    }
    else
    {
        auto threadVec = client.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true, nullptr, false, false, "admin", "123456", sdsp);
        EXPECT_EQ(threadVec.size(), threadCount);
        notify.wait();

        client.unsubscribe(hostName, port, "SDoutTables", "test_SD");

        EXPECT_EQ(msg1_total, 1000);
        EXPECT_EQ(msg2_total, 1000);
    }

    usedPorts.push_back(listenport);
}