namespace STCT
{
    class StreamingThreadedClientTester : public testing::Test, public ::testing::WithParamInterface<int>
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
                isNewVersion = conn.run("flag = 1;v = split(version(), ' ')[0];\
                                tmp=int(v.split('.'));\
                                if((tmp[0]==2 && tmp[1]==00 && tmp[2]>=9 )||(tmp[0]==2 && tmp[1]==10)){flag=1;}else{flag=0};\
                                flag")
                                   ->getBool();
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
            ConstantSP res = conn.run("1+1");

            cout << "ok" << endl;
            string del_streamtable = "login(\"admin\",\"123456\");"
                                     "try{ dropStreamTable(`outTables);}catch(ex){};"
                                     "try{ dropStreamTable(`st1);}catch(ex){};"
                                     "try{ dropStreamTable(`arrayVectorTable);}catch(ex){};";
            conn.run(del_streamtable);
        }
        virtual void TearDown()
        {
            conn.run("undef all;");
        }
    };

    static void createSharedTableAndReplay(int rows)
    {
        string script = "login(\"admin\",\"123456\")\n\
                st1 = streamTable(100:0, `datetimev`timestampv`sym`price1`price2,[DATETIME,TIMESTAMP,SYMBOL,DOUBLE,DOUBLE])\n\
                enableTableShareAndPersistence(table=st1, tableName=`outTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\n\
                go\n\
                setStreamTableFilterColumn(outTables, `sym)";
        conn.run(script);

        string replayScript = "n = " + to_string(rows) + ";table1_STCT = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);\
                tableInsert(table1_STCT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));\
                replay(inputTables=table1_STCT, outputTables=`outTables, dateColumn=`timestampv, timeColumn=`timestampv)";
        conn.run(replayScript);
    }

    static void createSharedTableAndReplay_withAllDataType()
    {
        srand(time(NULL));
        int scale32 = rand() % 9;
        int scale64 = rand() % 18;
        string replayScript = "colName =  `cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring`cblob`cipaddr`cuuid`cint128`cdecimal32`cdecimal64;"
                              "colType = [BOOL, CHAR, SHORT, INT,LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING, BLOB, IPADDR, UUID, INT128, DECIMAL32(" +
                              to_string(scale32) + "), DECIMAL64(" + to_string(scale64) + ")];"
                                                                                          "st1 = streamTable(100:0,colName, colType);"
                                                                                          "enableTableShareAndPersistence(table=st1, tableName=`outTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0);go;"
                                                                                          "setStreamTableFilterColumn(outTables, `csymbol);go;"
                                                                                          "row_num = 1000;"
                                                                                          "table1_SPCT = table(100:0, colName, colType);"
                                                                                          "col1 = rand(2 ,row_num);col2 = rand(256 ,row_num);col3 = rand(-row_num..row_num ,row_num);col4 = rand(-row_num..row_num ,row_num);col5 = rand(-row_num..row_num ,row_num);col6 = rand(0..row_num ,row_num);col7 = rand(0..row_num ,row_num);"
                                                                                          "col8 = rand(0..row_num ,row_num);col9 = rand(0..row_num ,row_num);col10 = rand(0..row_num ,row_num);col11 = rand(0..row_num ,row_num);col12 = rand(0..row_num ,row_num);col13 = rand(0..row_num ,row_num);col14= rand(0..row_num ,row_num);"
                                                                                          "col15 = rand(round(row_num,2) ,row_num);col16 = rand(round(row_num,2) ,row_num);col17 =rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);col18 =rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);col19 =rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);col20 =  take(ipaddr(\"192.168.1.13\"),row_num);"
                                                                                          "col21 = take(uuid(\"5d212a78-cc48-e3b1-4235-b4d91473ee87\"),row_num);col22 = take(int128(\"e1671797c52e15f763380b45e841ec32\"),row_num);col23=rand((-1000..1000)/1000.0000$DECIMAL32(" +
                              to_string(scale32) + "),row_num);col24=rand((-1000..1000)/1000.0000$DECIMAL64(" + to_string(scale64) + "),row_num);go;"
                                                                                                                                     "for (i in 0..(row_num-1)){tableInsert(table1_SPCT,col1[i],col2[i],col3[i],col4[i],col5[i],col6[i],col7[i],col8[i],col9[i],col10[i],col11[i],col12[i],col13[i],col14[i],col15[i],col16[i],col17[i],col18[i],col19[i],col20[i],col21[i],col22[i],col23[i],col24[i])};"
                                                                                                                                     "go;"
                                                                                                                                     "replay(inputTables=table1_SPCT, outputTables=`outTables, dateColumn=`ctimestamp, timeColumn=`ctimestamp);";
        conn.run(replayScript);
    }

    static void createSharedTableAndReplay_withArrayVector()
    {
        string replayScript = "colName =  `ts`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`cipaddr`cuuid`cint128;"
                              "colType = [TIMESTAMP,BOOL[], CHAR[], SHORT[], INT[],LONG[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], FLOAT[], DOUBLE[], IPADDR[], UUID[], INT128[]];"
                              "st1 = streamTable(100:0,colName, colType);"
                              "enableTableShareAndPersistence(table=st1, tableName=`arrayVectorTable, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0);go;"
                              "row_num = 1000;"
                              "table1_SPCT = table(100:0, colName, colType);"
                              "col0=rand(0..row_num ,row_num);col1 = arrayVector(1..row_num,rand(2 ,row_num));col2 = arrayVector(1..row_num,rand(256 ,row_num));col3 = arrayVector(1..row_num,rand(-row_num..row_num ,row_num));"
                              "col4 = arrayVector(1..row_num,rand(-row_num..row_num ,row_num));col5 = arrayVector(1..row_num,rand(-row_num..row_num ,row_num));col6 = arrayVector(1..row_num,rand(0..row_num ,row_num));"
                              "col7 = arrayVector(1..row_num,rand(0..row_num ,row_num));col8 = arrayVector(1..row_num,rand(0..row_num ,row_num));col9 = arrayVector(1..row_num,rand(0..row_num ,row_num));"
                              "col10 = arrayVector(1..row_num,rand(0..row_num ,row_num));col11 = arrayVector(1..row_num,rand(0..row_num ,row_num));col12 = arrayVector(1..row_num,rand(0..row_num ,row_num));"
                              "col13 = arrayVector(1..row_num,rand(0..row_num ,row_num));col14= arrayVector(1..row_num,rand(0..row_num ,row_num));col15 = arrayVector(1..row_num,rand(round(row_num,2) ,row_num));"
                              "col16 = arrayVector(1..row_num,rand(round(row_num,2) ,row_num));col17 =  arrayVector(1..row_num,take(ipaddr(\"192.168.1.13\"),row_num));col18 = arrayVector(1..row_num,take(uuid(\"5d212a78-cc48-e3b1-4235-b4d91473ee87\"),row_num));"
                              "col19 = arrayVector(1..row_num,take(int128(\"e1671797c52e15f763380b45e841ec32\"),row_num));go;"
                              "for (i in 0..(row_num-1)){tableInsert(table1_SPCT,col0[0][i],col1[0][i],col2[0][i],col3[0][i],col4[0][i],col5[0][i],col6[0][i],col7[0][i],col8[0][i],col9[0][i],col10[0][i],col11[0][i],col12[0][i],col13[0][i],col14[0][i],col15[0][i],col16[0][i],col17[0][i],col18[0][i],col19[0][i])};go;"
                              "replay(inputTables=table1_SPCT, outputTables=`arrayVectorTable, dateColumn=`ts, timeColumn=`ts);";
        conn.run(replayScript);
    }

    INSTANTIATE_TEST_CASE_P(StreamingReverse, StreamingThreadedClientTester, testing::Values(0, rand() % 1000 + 13000));
    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler)
    {
        STCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;

        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            msg_total += 1;
            // cout << msg->getString() << endl;
            if (msg_total == 1000)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;
        ThreadedClient threadedClient(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, "outTables", "mutiSchemaOne", 0));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, onehandler, "outTables", "mutiSchemaOne", 0);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, "outTables", "mutiSchemaOne");

            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler)
    {
        STCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;

        auto batchhandler = [&](vector<Message> msgs)
        {
            LockGuard<Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
                msg_total += 1;
                // cout  << msg->getString() << endl;
            }
            if (msg_total == 1000)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, "outTables", "mutiSchemaBatch", 0));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, batchhandler, "outTables", "mutiSchemaBatch", 0);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, "outTables", "mutiSchemaBatch");

            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_tableNameNull)
    {
        STCT::createSharedTableAndReplay(1000);
        int msg_total = 0;

        auto onehandler = [&](Message msg)
        {
            msg_total += 1;
            cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);

        EXPECT_ANY_THROW(auto thread = threadedClient.subscribe(hostName, port, onehandler, "", "cppStreamingAPI", 0, false));
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_tableNameNull)
    {
        STCT::createSharedTableAndReplay(1000);
        int msg_total = 0;

        auto batchhandler = [&](vector<Message> msgs)
        {
            for (auto &msg : msgs)
            {
                msg_total += 1;
                // cout  << msg->getString() << endl;
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        EXPECT_ANY_THROW(auto thread = threadedClient.subscribe(hostName, port, batchhandler, "", "cppStreamingAPI", 0, false));
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_offsetNegative)
    {
        STCT::createSharedTableAndReplay(1000);
        int msg_total = 0;

        auto onehandler = [&](Message msg)
        {
            msg_total += 1;
            // cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, "outTables", "actionTest", -1));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, onehandler, "outTables", "actionTest", -1);

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 0);
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_offsetNegative)
    {
        STCT::createSharedTableAndReplay(1000);
        int msg_total = 0;

        auto batchhandler = [&](vector<Message> msgs)
        {
            for (auto &msg : msgs)
            {
                msg_total += 1;
                // cout  << msg->getString() << endl;
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;
        ThreadedClient threadedClient(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, "outTables", "actionTest", -1));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, batchhandler, "outTables", "actionTest", -1);

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 0);
        }
        usedPorts.push_back(listenport);
    }

    // TEST_P(StreamingThreadedClientTester,test_subscribe_onehandler_offsetMoreThanRows){
    //     STCT::createSharedTableAndReplay(1000);
    // 	int msg_total = 0;

    // 	auto onehandler = [&](Message msg) {
    // 		msg_total += 1;
    // 		// cout << msg->getString() << endl;
    // 	};

    // 	srand(time(0));

    // int listenport = rand() % 13000 + 2000;
    // if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
    //     listenport = rand() % 13000 + 2000;
    // };

    //     ThreadedClient threadedClient(listenport);
    //     auto thread = threadedClient.subscribe(hostName, port, onehandler, "outTables", "actionTest", 2001);
    //     Util::sleep(2000);
    //     cout << "total size: " << msg_total << endl;
    //     threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
    // usedPorts.push_back(listenport);
    //     EXPECT_EQ(msg_total,0);
    // }

    // TEST_P(StreamingThreadedClientTester,test_subscribe_batchhandler_offsetMoreThanRows){
    //     STCT::createSharedTableAndReplay(1000);
    // 	int msg_total = 0;

    // auto batchhandler = [&](vector<Message> msgs) {
    // 	for (auto &msg : msgs) {
    // 		msg_total += 1;
    // 		// cout  << msg->getString() << endl;
    // 	}
    // };

    // 	srand(time(0));

    // int listenport = rand() % 13000 + 2000;
    // if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
    //     listenport = rand() % 13000 + 2000;
    // };

    //     ThreadedClient threadedClient(listenport);
    //     auto thread = threadedClient.subscribe(hostName, port, batchhandler, "outTables", "actionTest", 2001);
    //     Util::sleep(2000);
    //     cout << "total size: " << msg_total << endl;
    //     threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
    // usedPorts.push_back(listenport);
    //     EXPECT_EQ(msg_total,0);
    // }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_filter)
    {
        STCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;

        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            msg_total += msg->rows();
            if (msg_total == 1000)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        VectorSP filter = Util::createVector(DT_SYMBOL, 1, 1);
        filter->setString(0, "b");
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0, true, filter));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0, true, filter);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_TRUE(msg_total > 0);
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_filter)
    {
        STCT::createSharedTableAndReplay(1000);
        int msg_total = 0;

        auto batchhandler = [&](vector<Message> msgs)
        {
            for (auto &msg : msgs)
            {
                msg_total += 1;
                // cout  << msg->getString() << endl;
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        VectorSP filter = Util::createVector(DT_SYMBOL, 1, 1);
        filter->setString(0, "b");
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, "outTables", "actionTest", 0, true, filter));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, batchhandler, "outTables", "actionTest", 0, true, filter);
            Util::sleep(2000);

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_TRUE(msg_total > 0);
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_msgAsTable)
    {
        STCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;

        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            EXPECT_EQ(msg->getForm(), 6);
            msg_total += msg->rows();

            if (msg_total == 1000)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0, false, nullptr, true));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0, false, nullptr, true);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_msgAsTable)
    {
        STCT::createSharedTableAndReplay(10000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;

        auto batchhandler = [&](vector<Message> msgs)
        {
            LockGuard<Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
                EXPECT_EQ(msg->getForm(), 6);
                cout << "msg_row: " << msg->rows() << endl;
                msg_total += msg->rows();
            }
            if (msg_total == 10000)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, "outTables", "actionTest", 0, false, nullptr, false, 2000, 0.1, true));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, batchhandler, "outTables", "actionTest", 0, false, nullptr, false, 2000, 0.1, true);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 10000);
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_allowExists)
    {
        STCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;

        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            msg_total += 1;
            // cout << msg->getString() << endl;
            if (msg_total == 1000)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0, true, nullptr, false, true));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0, true, nullptr, false, true);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_allowExists)
    {
        STCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;

        auto batchhandler = [&](vector<Message> msgs)
        {
            LockGuard<Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
                msg_total += 1;
                // cout  << msg->getString() << endl;
            }
            if (msg_total == 1000)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, "outTables", "actionTest", 0, true, nullptr, false, true));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, batchhandler, "outTables", "actionTest", 0, true, nullptr, false, true);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_resub_false)
    {
        int msg_total = 0;
        auto batchhandler = [&](vector<Message> msgs)
        {
            for (auto &msg : msgs)
            {
                msg_total += 1;
                // cout  << msg->getString() << endl;
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        EXPECT_ANY_THROW(auto thread = threadedClient.subscribe(hostName, port, batchhandler, "outTables", "actionTest", 0, false));
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_resub_true)
    {
        int msg_total = 0;
        auto batchhandler = [&](vector<Message> msgs)
        {
            for (auto &msg : msgs)
            {
                msg_total += 1;
                // cout  << msg->getString() << endl;
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, "outTables", "actionTest", 0, true));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, batchhandler, "outTables", "actionTest", 0, true);
            Util::sleep(2000);
            threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
        }
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_batchSize)
    {
        STCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;

        auto batchhandler = [&](vector<Message> msgs)
        {
            LockGuard<Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
                msg_total += 1;
                // cout  << msg->getString() << endl;
            }
            if (msg_total == 1000)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, "outTables", "actionTest", 0, true, nullptr, false, 1000));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, batchhandler, "outTables", "actionTest", 0, true, nullptr, false, 1000);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_throttle)
    {
        STCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;

        auto batchhandler = [&](vector<Message> msgs)
        {
            LockGuard<Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
                msg_total += 1;
                // cout  << msg->getString() << endl;
            }
            if (msg_total == 1000)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, "outTables", "actionTest", 0, true, nullptr, false, 1000, 1.0));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, batchhandler, "outTables", "actionTest", 0, true, nullptr, false, 1000, 1.0);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_hostNull)
    {
        STCT::createSharedTableAndReplay(1000);
        int msg_total = 0;

        auto onehandler = [&](Message msg)
        {
            msg_total += 1;
            // cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        EXPECT_ANY_THROW(threadedClient.subscribe("", port, onehandler, "outTables", "actionTest", 0));

        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_hostNull)
    {
        STCT::createSharedTableAndReplay(1000);
        int msg_total = 0;

        auto batchhandler = [&](vector<Message> msgs)
        {
            for (auto &msg : msgs)
            {
                msg_total += 1;
                // cout  << msg->getString() << endl;
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        EXPECT_ANY_THROW(threadedClient.subscribe("", port, batchhandler, "outTables", "actionTest", 0));

        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_portNull)
    {
        STCT::createSharedTableAndReplay(1000);
        int msg_total = 0;

        auto onehandler = [&](Message msg)
        {
            msg_total += 1;
            // cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        EXPECT_ANY_THROW(threadedClient.subscribe(hostName, NULL, onehandler, "outTables", "actionTest", 0));

        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_actionNameNull)
    {
        STCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;

        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            msg_total += 1;
            // cout << msg->getString() << endl;
            if (msg_total == 1000)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, "outTables", "", 0));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, onehandler, "outTables", "", 0);
            notify.wait();

            TableSP stat = conn.run("getStreamingStat().pubTables");

            EXPECT_EQ(stat->getColumn(0)->getRow(0)->getString(), "outTables");
            EXPECT_EQ(stat->getColumn(2)->getRow(0)->getInt(), 1000);
            EXPECT_EQ(stat->getColumn(3)->getRow(0)->getString(), "");

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, "outTables", "");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_actionNameNull)
    {
        STCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;

        auto batchhandler = [&](vector<Message> msgs)
        {
            LockGuard<Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
                msg_total += 1;
                // cout  << msg->getString() << endl;
            }
            if (msg_total == 1000)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, "outTables", "", 0));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, batchhandler, "outTables", "", 0);
            notify.wait();

            TableSP stat = conn.run("getStreamingStat().pubTables");

            EXPECT_EQ(stat->getColumn(0)->getRow(0)->getString(), "outTables");
            EXPECT_EQ(stat->getColumn(2)->getRow(0)->getInt(), 1000);
            EXPECT_EQ(stat->getColumn(3)->getRow(0)->getString(), "");

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, "outTables", "");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_unsubscribe_onehandler_hostNull)
    {
        STCT::createSharedTableAndReplay(1000);
        int msg_total = 0;

        auto onehandler = [&](Message msg)
        {
            msg_total += 1;
            // cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0);

            cout << "total size: " << msg_total << endl;
            EXPECT_ANY_THROW(threadedClient.unsubscribe("", port, "outTables", "actionTest"));

            threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_unsubscribe_portNull)
    {
        STCT::createSharedTableAndReplay(1000);
        int msg_total = 0;

        auto onehandler = [&](Message msg)
        {
            msg_total += 1;
            // cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0);
            cout << "total size: " << msg_total << endl;

            EXPECT_ANY_THROW(threadedClient.unsubscribe(hostName, NULL, "outTables", "actionTest"));
            threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_unsubscribe_tableNameNull)
    {
        STCT::createSharedTableAndReplay(1000);
        int msg_total = 0;

        auto onehandler = [&](Message msg)
        {
            msg_total += 1;
            // cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0);

            cout << "total size: " << msg_total << endl;
            EXPECT_ANY_THROW(threadedClient.unsubscribe(hostName, port, "", "actionTest"));

            threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_unsubscribe_actionNameNull)
    {
        STCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;

        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            msg_total += 1;
            // handle msg
            if (msg_total == 1000)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, "outTables", "");
            TableSP stat = conn.run("getStreamingStat().pubTables");

            EXPECT_EQ(stat->getColumn(0)->getRow(0)->getString(), "outTables");
            EXPECT_EQ(stat->getColumn(2)->getRow(0)->getInt(), 1000);
            EXPECT_EQ(stat->getColumn(3)->getRow(0)->getString(), "actionTest");

            threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");

            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
        }

        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadedClientTester, tes_onehandler_subscribe_twice)
    {
        STCT::createSharedTableAndReplay(1000);
        int msg_total = 0;

        auto onehandler = [&](Message msg)
        {
            msg_total += 1;
            // handle msg
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;
        ThreadedClient threadedClient(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, "outTables", "mutiSchemaOne", 0, false));
        }
        else
        {
            auto thread1 = threadedClient.subscribe(hostName, port, onehandler, "outTables", "mutiSchemaOne", 0, false);
            EXPECT_ANY_THROW(auto thread2 = threadedClient.subscribe(hostName, port, onehandler, "outTables", "mutiSchemaOne", 0, false));

            threadedClient.unsubscribe(hostName, port, "outTables", "mutiSchemaOne");
            Util::sleep(1000);
            cout << "total size: " << msg_total << endl;
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread1), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_batchhandler_subscribe_twice)
    {
        STCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;

        auto batchhandler = [&](vector<Message> msgs)
        {
            for (auto &msg : msgs)
            {
                LockGuard<Mutex> lock(&mutex);
                msg_total += 1;
                // handle msg
                if (msg_total == 1000)
                {
                    notify.set();
                }
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, "outTables", "mutiSchemaBatch", 0, false));
        }
        else
        {
            auto thread1 = threadedClient.subscribe(hostName, port, batchhandler, "outTables", "mutiSchemaBatch", 0, false);
            EXPECT_ANY_THROW(auto thread2 = threadedClient.subscribe(hostName, port, batchhandler, "outTables", "mutiSchemaBatch", 0, false));
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, "outTables", "mutiSchemaBatch");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread1), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_withAllDataType)
    {
        STCT::createSharedTableAndReplay_withAllDataType();
        int msg_total = 0;
        TableSP ex_table = conn.run("select * from outTables");
        int index = 0;
        Signal notify;
        Mutex mutex;

        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            msg_total += 1;
            // handle msg
            for (auto i = 0; i < ex_table->columns(); i++)
            {
                EXPECT_EQ(ex_table->getColumn(i)->get(index)->getString(), msg->get(i)->getString());
            }
            index++;
            if (msg_total == 1000)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;
        ThreadedClient threadedClient(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, "outTables", "mutiSchemaOne", 0));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, onehandler, "outTables", "mutiSchemaOne", 0);
            notify.wait();
            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, "outTables", "mutiSchemaOne");

            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_withAllDataType)
    {
        STCT::createSharedTableAndReplay_withAllDataType();
        int msg_total = 0;
        Signal notify;
        Mutex mutex;
        auto batchhandler = [&](vector<Message> msgs)
        {
            LockGuard<Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
                msg_total += 1;
                // cout  << msg->getString() << endl;
                if (msg_total == 1000)
                {
                    notify.set();
                }
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, "outTables", "mutiSchemaBatch", 0));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, batchhandler, "outTables", "mutiSchemaBatch", 0);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, "outTables", "mutiSchemaBatch");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_arrayVector)
    {
        STCT::createSharedTableAndReplay_withArrayVector();
        int msg_total = 0;
        int index = 0;
        Signal notify;
        Mutex mutex;

        TableSP ex_tab = conn.run("select * from arrayVectorTable");
        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            msg_total += 1;
            for (auto i = 0; i < ex_tab->columns(); i++)
                EXPECT_EQ(msg->get(i)->getString(), ex_tab->getColumn(i)->get(index)->getString());
            index++;
            if (msg_total == 1000)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;
        ThreadedClient threadedClient(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, "arrayVectorTable", "arrayVectorTableTest", 0));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, onehandler, "arrayVectorTable", "arrayVectorTableTest", 0);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, "arrayVectorTable", "arrayVectorTableTest");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`arrayVectorTable) ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_arrayVector)
    {
        STCT::createSharedTableAndReplay_withArrayVector();
        int msg_total = 0;
        Signal notify;
        Mutex mutex;

        TableSP ex_tab = conn.run("select * from arrayVectorTable");
        auto batchhandler = [&](vector<Message> msgs)
        {
            LockGuard<Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
                msg_total += 1;
                for (auto i = 1; i < ex_tab->columns(); i++)
                {
                    // EXPECT_EQ(ex_tab->getColumn(i)->getType(), msg->get(i)->getType());
                    EXPECT_EQ(msg->get(i)->getForm(), DF_VECTOR);
                }
                if (msg_total == 1000)
                {
                    notify.set();
                }
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, "arrayVectorTable", "arrayVectorTableTest", 0));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, batchhandler, "arrayVectorTable", "arrayVectorTableTest", 0);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, "arrayVectorTable", "arrayVectorTableTest");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`arrayVectorTable) ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_hugetable_onehandler)
    {
        STCT::createSharedTableAndReplay(1000000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;

        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            msg_total += 1;
            if (msg_total % 100000 == 0)
                cout << "now subscribed rows: " << msg_total << endl;
            // handle msg
            if (msg_total == 1000000)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;
        ThreadedClient threadedClient(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, "outTables", "mutiSchemaOne", 0));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, onehandler, "outTables", "mutiSchemaOne", 0);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, "outTables", "mutiSchemaOne");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());

            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000000);
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_hugetable_batchhandler)
    {
        STCT::createSharedTableAndReplay(1000000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;

        auto batchhandler = [&](vector<Message> msgs)
        {
            LockGuard<Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
                msg_total += 1;
                if (msg_total % 100000 == 0)
                    cout << "now subscribed rows: " << msg_total << endl;
                // handle msg
                if (msg_total == 1000000)
                {
                    notify.set();
                }
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, "outTables", "mutiSchemaBatch", 0));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, batchhandler, "outTables", "mutiSchemaBatch", 0);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, "outTables", "mutiSchemaBatch");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());

            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000000);
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_hugetable_onehandler_msgAsTable)
    {
        STCT::createSharedTableAndReplay(1000000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;

        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            msg_total += msg->rows();
            if (msg_total % 100000 == 0)
                cout << "now subscribed rows: " << msg_total << endl;
            // handle msg
            if (msg_total == 1000000)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;
        ThreadedClient threadedClient(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, "outTables", "mutiSchemaOne", 0, false, nullptr, true));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, onehandler, "outTables", "mutiSchemaOne", 0, false, nullptr, true);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, "outTables", "mutiSchemaOne");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());

            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000000);
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_hugetable_batchhandler_msgAsTable)
    {
        STCT::createSharedTableAndReplay(1000000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;

        auto batchhandler = [&](vector<Message> msgs)
        {
            LockGuard<Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
                EXPECT_EQ(msg->getForm(), DF_TABLE);
                msg_total += msg->rows();
                if (msg_total % 100000 == 0)
                    cout << "now subscribed rows: " << msg_total << endl;
                // handle msg
                if (msg_total == 1000000)
                {
                    notify.set();
                }
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, "outTables", "mutiSchemaBatch", 0, false, nullptr, false, 1, 1.0, true));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, batchhandler, "outTables", "mutiSchemaBatch", 0, false, nullptr, false, 1, 1.0, true);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, "outTables", "mutiSchemaBatch");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());

            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000000);
        }
        usedPorts.push_back(listenport);
    }

}