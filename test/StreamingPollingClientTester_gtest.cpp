namespace SPCT
{
    class StreamingPollingClientTester : public testing::Test, public ::testing::WithParamInterface<int>
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

        string replayScript = "n = " + to_string(rows) + ";table1_SPCT = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);\
                tableInsert(table1_SPCT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));\
                replay(inputTables=table1_SPCT, outputTables=`outTables, dateColumn=`timestampv, timeColumn=`timestampv)";
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

    INSTANTIATE_TEST_CASE_P(StreamingReverse, StreamingPollingClientTester, testing::Values(0, rand() % 1000 + 13000));
    TEST_P(StreamingPollingClientTester, test_subscribeUnsubscribe_normal)
    {
        SPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, "outTables", "actionTest", 0));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, "outTables", "actionTest", 0);

            Message msg;
            thread th1 = thread([&]
                                {
            while (true)
            {
                queue->pop(msg);
                // cout<<msg->getString()<<endl;
                if(msg.isNull()) {break;}else{msg_total+=1;}
            } });
            Util::sleep(1000);
            client.unsubscribe(hostName, port, "outTables", "actionTest");
            th1.join();
            cout << "total size: " << msg_total << endl;
            Util::sleep(100);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
            EXPECT_EQ(msg_total, 1000);
        }

        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_hostNull)
    {
        SPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client(listenport);
        EXPECT_ANY_THROW(client.subscribe("", port, "outTables", "actionTest", 0, false));
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_portNull)
    {
        SPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client(listenport);
        EXPECT_ANY_THROW(client.subscribe(hostName, NULL, "outTables", "actionTest", 0, false));
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_tableNameNull)
    {
        SPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client(listenport);
        EXPECT_ANY_THROW(auto queue = client.subscribe(hostName, port, "", "actionTest", 0, false));
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_actionNameNull)
    {
        SPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, "outTables", "", 0));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, "outTables", "", 0);
            Message msg;
            thread th1 = thread([&]
                                {
            while (true)
            {
                queue->pop(msg);
                // cout<<msg->getString()<<endl;
                if(msg.isNull()) {break;}else{msg_total+=1;}
            } });
            Util::sleep(1000);
            client.unsubscribe(hostName, port, "outTables", "");
            th1.join();
            cout << "total size: " << msg_total << endl;
            Util::sleep(100);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
            EXPECT_EQ(msg_total, 1000);
        }

        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_offsetNegative)
    {
        SPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, "outTables", "actionTest", -1, false));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, "outTables", "actionTest", -1, false);
            Message msg;
            thread th1 = thread([&]
                                {
            while (true)
            {
                queue->pop(msg);
                // cout<<msg->getString()<<endl;
                if(msg.isNull()) {break;}else{msg_total+=1;}
            } });
            Util::sleep(1000);
            client.unsubscribe(hostName, port, "outTables", "actionTest");
            th1.join();
            cout << "total size: " << msg_total << endl;
            Util::sleep(100);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
            EXPECT_EQ(msg_total, 0);
        }

        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_filter)
    {
        SPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        VectorSP filter = Util::createVector(DT_SYMBOL, 1, 1);
        filter->setString(0, "a");

        PollingClient client(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, "outTables", "actionTest", 0, true, filter));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, "outTables", "actionTest", 0, true, filter);
            Message msg;
            thread th1 = thread([&]
                                {
            while (true)
            {
                queue->pop(msg);
                // cout<<msg->getString()<<endl;
                if(msg.isNull()) {break;}else{msg_total+=1;}
            } });
            Util::sleep(1000);
            client.unsubscribe(hostName, port, "outTables", "actionTest");
            th1.join();
            cout << "total size: " << msg_total << endl;
            Util::sleep(100);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
            EXPECT_EQ(msg_total > 0, true);
        }

        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_msgAsTable)
    {
        SPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, "outTables", "actionTest", 0, true, nullptr, true));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, "outTables", "actionTest", 0, true, nullptr, true);
            Message msg;
            ThreadSP th1 = new Thread(new Executor([&]
                                                   {
            while (true)
            {
                queue->pop(msg);
                if(msg.isNull()) {break;}
                else{
                    EXPECT_EQ(msg->getForm(), DF_TABLE);
                    msg_total=msg->rows();
                }
            } }));

            Util::sleep(1000);
            client.unsubscribe(hostName, port, "outTables", "actionTest");
            th1->start();
            th1->join();
            cout << "total size: " << msg_total << endl;
            Util::sleep(100);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
            EXPECT_EQ(msg_total, 1000);
        }

        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_allowExists)
    {
        SPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, "outTables", "actionTest", 0, true, nullptr, false, true));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, "outTables", "actionTest", 0, true, nullptr, false, true);
            Message msg;
            thread th1 = thread([&]
                                {
            while (true)
            {
                queue->pop(msg);
                // cout<<msg->getString()<<endl;
                if(msg.isNull()) {break;}else{msg_total+=1;}
            } });
            Util::sleep(1000);
            client.unsubscribe(hostName, port, "outTables", "actionTest");
            th1.join();
            cout << "total size: " << msg_total << endl;
            Util::sleep(100);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
            EXPECT_EQ(msg_total, 1000);
        }

        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_resub_false)
    {
        // SPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client(listenport);
        EXPECT_ANY_THROW(auto queue = client.subscribe(hostName, port, "outTables", "actionTest", 0, false));

        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_resub_true)
    {
        // SPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, "outTables", "actionTest", 0, true));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, "outTables", "actionTest", 0, true);
            Util::sleep(2000);
            client.unsubscribe(hostName, port, "outTables", "actionTest");
        }

        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_unsubscribe_hostNull)
    {
        SPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, "outTables", "actionTest", 0));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, "outTables", "actionTest", 0);
            Message msg;
            thread th1 = thread([&]
                                {
            while (true)
            {
                queue->pop(msg);
                // cout<<msg->getString()<<endl;
                if(msg.isNull()) {break;}else{msg_total+=1;}
            } });
            Util::sleep(1000);
            EXPECT_ANY_THROW(client.unsubscribe("", port, "outTables", "actionTest"));

            client.unsubscribe(hostName, port, "outTables", "actionTest");
            th1.join();
            cout << "total size: " << msg_total << endl;
            Util::sleep(100);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
        }

        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_unsubscribe_portNull)
    {
        SPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, "outTables", "actionTest", 0));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, "outTables", "actionTest", 0);
            Message msg;
            thread th1 = thread([&]
                                {
            while (true)
            {
                queue->pop(msg);
                // cout<<msg->getString()<<endl;
                if(msg.isNull()) {break;}else{msg_total+=1;}
            } });
            Util::sleep(1000);
            EXPECT_ANY_THROW(client.unsubscribe(hostName, NULL, "outTables", "actionTest"));

            client.unsubscribe(hostName, port, "outTables", "actionTest");
            th1.join();
            cout << "total size: " << msg_total << endl;
            Util::sleep(100);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
        }

        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_unsubscribe_tableNameNull)
    {
        SPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, "outTables", "actionTest", 0));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, "outTables", "actionTest", 0);
            Message msg;
            thread th1 = thread([&]
                                {
            while (true)
            {
                queue->pop(msg);
                // cout<<msg->getString()<<endl;
                if(msg.isNull()) {break;}else{msg_total+=1;}
            } });
            Util::sleep(1000);
            EXPECT_ANY_THROW(client.unsubscribe(hostName, port, "", "actionTest"));

            client.unsubscribe(hostName, port, "outTables", "actionTest");
            th1.join();
            cout << "total size: " << msg_total << endl;
            Util::sleep(100);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
        }

        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_unsubscribe_actionNameNull)
    {
        SPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, "outTables", "actionTest", 0));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, "outTables", "actionTest", 0);
            Message msg;
            thread th1 = thread([&]
                                {
            while (true)
            {
                queue->pop(msg);
                // cout<<msg->getString()<<endl;
                if(msg.isNull()) {break;}else{msg_total+=1;}
            } });
            Util::sleep(1000);

            client.unsubscribe(hostName, port, "outTables", "");
            TableSP stat = conn.run("getStreamingStat().pubTables");
            EXPECT_EQ(stat->getColumn(0)->getRow(0)->getString(), "outTables");
            EXPECT_EQ(stat->getColumn(2)->getRow(0)->getInt(), 1000);
            EXPECT_EQ(stat->getColumn(3)->getRow(0)->getString(), "actionTest");
            client.unsubscribe(hostName, port, "outTables", "actionTest");
            th1.join();
            cout << "total size: " << msg_total << endl;
            Util::sleep(100);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
        }

        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_twice)
    {
        SPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, "outTables", "actionTest", 0, false));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, "outTables", "actionTest", 0, false);
            Message msg;
            thread th1 = thread([&]
                                {
            while (true)
            {
                queue->pop(msg);
                // cout<<msg->getString()<<endl;
                if(msg.isNull()) {break;}else{msg_total+=1;}
            } });
            EXPECT_ANY_THROW(auto queue1 = client.subscribe(hostName, port, "outTables", "actionTest", 0, false));

            Util::sleep(1000);
            client.unsubscribe(hostName, port, "outTables", "actionTest");
            th1.join();
            cout << "total size: " << msg_total << endl;
            Util::sleep(100);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
            EXPECT_EQ(msg_total, 1000);
        }

        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_subscribeUnsubscribe_withAllDataType)
    {
        SPCT::createSharedTableAndReplay_withAllDataType();
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, "outTables", "actionTest", 0));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, "outTables", "actionTest", 0);

            TableSP ex_table = conn.run("select * from outTables");
            int index = 0;

            Message msg;
            thread th1 = thread([&]
                                {
            while (true)
            {
                queue->pop(msg);
                // cout<<msg->getString()<<endl;
                if(msg.isNull()) {break;}
                else{
                    msg_total+=1;
                    for (auto i = 0; i < ex_table->columns(); i++)
                        // cout<<ex_table->getColumn(i)->get(index)->getString()<<endl<<msg->get(i)->getString()<<endl<<endl;
                        EXPECT_EQ(ex_table->getColumn(i)->get(index)->getString(), msg->get(i)->getString());
                    }
                    index++;
            } });
            Util::sleep(1000);
            client.unsubscribe(hostName, port, "outTables", "actionTest");
            th1.join();
            cout << "total size: " << msg_total << endl;
            Util::sleep(100);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
            EXPECT_EQ(msg_total, 1000);
        }

        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_subscribeUnsubscribe_arrayVector)
    {
        SPCT::createSharedTableAndReplay_withArrayVector();
        int msg_total = 0;
        int index = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, "arrayVectorTable", "arrayVectorTableTest", 0));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, "arrayVectorTable", "arrayVectorTableTest", 0);

            Util::sleep(1000);
            TableSP ex_tab = conn.run("select * from arrayVectorTable");
            Message msg;
            thread th1 = thread([&]
                                {
            while (true)
            {
                queue->pop(msg);
                // cout<<msg->getString()<<endl;
                if(msg.isNull()) {break;}
                else{
                    msg_total+=1;
                    for (auto i = 0; i < ex_tab->columns(); i++)
                        EXPECT_EQ(ex_tab->getColumn(i)->get(index)->getString(), msg->get(i)->getString());
                    }
                index++;
            } });
            Util::sleep(1000);
            client.unsubscribe(hostName, port, "arrayVectorTable", "arrayVectorTableTest");
            th1.join();
            cout << "total size: " << msg_total << endl;
            Util::sleep(100);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`arrayVectorTable) ==0")->getBool());
            EXPECT_EQ(msg_total, 1000);
        }

        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_subscribeUnsubscribe_hugetable)
    {
        SPCT::createSharedTableAndReplay(1000000);
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, "outTables", "actionTest", 0));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, "outTables", "actionTest", 0);

            Message msg;
            thread th1 = thread([&]
                                {
            while (true)
            {
                queue->pop(msg);
                // cout<<msg->getString()<<endl;
                if(msg.isNull()) {break;}
                else{
                        msg_total += 1;
                        if (msg_total % 100000 == 0)
                            cout << "now subscribed rows: " << msg_total << endl;
                    }
            } });

            while (msg_total != 1000000)
            {
                Util::sleep(500);
            }
            client.unsubscribe(hostName, port, "outTables", "actionTest");
            th1.join();
            cout << "total size: " << msg_total << endl;
            Util::sleep(100);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
            EXPECT_EQ(msg_total, 1000000) << "Streaming msgs may not be recieved absolutely yet";
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_subscribeUnsubscribe_hugetable_msgAsTable)
    {
        SPCT::createSharedTableAndReplay(1000000);
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client(listenport);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, "outTables", "actionTest", 0, false, nullptr, true));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, "outTables", "actionTest", 0, false, nullptr, true);

            Message msg;
            thread th1 = thread([&]
                                {
            while (true)
            {
                queue->pop(msg);
                // cout<<msg->getString()<<endl;
                if(msg.isNull()) {break;}
                else{
                        msg_total += msg->rows();
                        EXPECT_EQ(msg->getForm(), DF_TABLE);
                        if (msg_total % 100000 == 0)
                            cout << "now subscribed rows: " << msg_total << endl;
                    }
            } });
            while (msg_total != 1000000)
            {
                Util::sleep(500);
            }
            client.unsubscribe(hostName, port, "outTables", "actionTest");
            th1.join();
            cout << "total size: " << msg_total << endl;
            Util::sleep(100);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
            EXPECT_EQ(msg_total, 1000000) << "Streaming msgs may not be recieved absolutely yet";
        }
        usedPorts.push_back(listenport);
    }
}