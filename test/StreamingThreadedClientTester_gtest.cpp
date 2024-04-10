#include "config.h"

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
            catch(const std::exception& e)
            {
                conn.connect(hostName, port, "admin", "123456");
            }

            cout << "ok" << endl;
        }
        virtual void TearDown()
        {
            conn.run("undef all;");
        }
    };

    static void createSharedTableAndReplay(const string &st, int rows)
    {
        string script = "login(\"admin\",\"123456\")\n\
                st1 = streamTable(100:0, `datetimev`timestampv`sym`price1`price2,[DATETIME,TIMESTAMP,SYMBOL,DOUBLE,DOUBLE])\n\
                enableTableShareAndPersistence(table=st1, tableName=`"+st+", asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\n\
                go\n\
                setStreamTableFilterColumn("+st+", `sym)";
        conn.run(script);

        string replayScript = "n = " + to_string(rows) + ";table1_STCT = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);\
                tableInsert(table1_STCT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));\
                replay(inputTables=table1_STCT, outputTables=`"+st+", dateColumn=`timestampv, timeColumn=`timestampv)";
        conn.run(replayScript);
    }

    static void createSharedTableAndReplay_withAllDataType(const string &st)
    {
        srand(time(NULL));
        int scale32 = rand() % 9;
        int scale64 = rand() % 18;
        int scale128 = rand() % 28;
        string replayScript = "colName =  `cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring`cblob`cipaddr`cuuid`cint128`cdecimal32`cdecimal64`cdecimal128;"
                              "colType = [BOOL, CHAR, SHORT, INT,LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING, BLOB, IPADDR, UUID, INT128, DECIMAL32(" +
                              to_string(scale32) + "), DECIMAL64(" + to_string(scale64) + "), DECIMAL128(" + to_string(scale128) + ")];"
                                                                                          "st1 = streamTable(100:0,colName, colType);"
                                                                                          "enableTableShareAndPersistence(table=st1, tableName=`"+st+", asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0);go;"
                                                                                          "setStreamTableFilterColumn("+st+", `csymbol);go;"
                                                                                          "row_num = 1000;"
                                                                                          "table1_SPCT = table(100:0, colName, colType);"
                                                                                          "col1 = rand(2 ,row_num);col2 = rand(256 ,row_num);col3 = rand(-row_num..row_num ,row_num);col4 = rand(-row_num..row_num ,row_num);col5 = rand(-row_num..row_num ,row_num);col6 = rand(0..row_num ,row_num);col7 = rand(0..row_num ,row_num);"
                                                                                          "col8 = rand(0..row_num ,row_num);col9 = rand(0..row_num ,row_num);col10 = rand(0..row_num ,row_num);col11 = rand(0..row_num ,row_num);col12 = rand(0..row_num ,row_num);col13 = rand(0..row_num ,row_num);col14= rand(0..row_num ,row_num);"
                                                                                          "col15 = rand(round(row_num,2) ,row_num);col16 = rand(round(row_num,2) ,row_num);col17 =rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);col18 =rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);col19 =rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);col20 = take(ipaddr(\"192.168.1.13\"),row_num);"
                                                                                          "col21 = take(uuid(\"5d212a78-cc48-e3b1-4235-b4d91473ee87\"),row_num);col22 = take(int128(\"e1671797c52e15f763380b45e841ec32\"),row_num);col23=rand((-1000..1000)/1000.0000$DECIMAL32(" +
                              to_string(scale32) + "),row_num);col24=rand((-1000..1000)/1000.0000$DECIMAL64(" + to_string(scale64) + "),row_num);col25=rand((-1000..1000)/1000.0000$DECIMAL128(" + to_string(scale128) + "),row_num);go;"
                                                                                                                                     "for (i in 0..(row_num-1)){tableInsert(table1_SPCT,col1[i],col2[i],col3[i],col4[i],col5[i],col6[i],col7[i],col8[i],col9[i],col10[i],col11[i],col12[i],col13[i],col14[i],col15[i],col16[i],col17[i],col18[i],col19[i],col20[i],col21[i],col22[i],col23[i],col24[i],col25[i])};"
                                                                                                                                     "go;"
                                                                                                                                     "replay(inputTables=table1_SPCT, outputTables=`"+st+", dateColumn=`ctimestamp, timeColumn=`ctimestamp);";
        conn.run(replayScript);
    }

    static void createSharedTableAndReplay_withArrayVector(const string &st)
    {
        string replayScript = "colName = `ts`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cdatehour`cfloat`cdouble`cipaddr`cuuid`cint128`cdecimal32`cdecimal64`cdecimal128;"
                              "colType = [TIMESTAMP, BOOL[], CHAR[], SHORT[], INT[],LONG[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], DATEHOUR[], FLOAT[], DOUBLE[], IPADDR[], UUID[], INT128[],DECIMAL32(2)[],DECIMAL64(11)[],DECIMAL128(30)[]];"
                              "st1 = streamTable(100:0,colName, colType);"
                              "enableTableShareAndPersistence(table=st1, tableName=`"+st+");go;"
                              "row_num = 1000;"
                              "ind = [1,2,3,4,row_num];"
                              "ts=timestamp(rand(0..row_num ,row_num));"
                              "cbool= arrayVector(ind, bool(take(0 1 ,row_num)));cchar = arrayVector(ind, char(take(256 ,row_num)));cshort = arrayVector(ind, short(take(-10000..10000 ,row_num)));cint = arrayVector(ind, int(take(-10000..10000 ,row_num)));"
                              "clong = arrayVector(ind, long(take(-10000..10000 ,row_num)));cdate = arrayVector(ind, date(take(10000 ,row_num)));cmonth = arrayVector(ind, month(take(23640..25000 ,row_num)));ctime = arrayVector(ind, time(take(10000 ,row_num)));"
                              "cminute = arrayVector(ind, minute(take(100 ,row_num)));csecond = arrayVector(ind, second(take(100 ,row_num)));cdatetime = arrayVector(ind, datetime(take(10000 ,row_num)));ctimestamp = arrayVector(ind, timestamp(take(10000 ,row_num)));"
                              "cnanotime = arrayVector(ind, nanotime(take(10000 ,row_num)));cnanotimestamp = arrayVector(ind, nanotimestamp(take(10000 ,row_num)));cdatehour = arrayVector(ind, datehour(take(10000 ,row_num)));cfloat = arrayVector(ind, float(rand(10000.0000,row_num)));cdouble = arrayVector(ind, rand(10000.0000, row_num));"
                              "cipaddr = arrayVector(1..row_num, take(ipaddr(['192.168.1.13']),row_num));"
                              "cuuid = arrayVector(1..row_num, take(uuid(['5d212a78-cc48-e3b1-4235-b4d91473ee87']),row_num));"
                              "cint128 = arrayVector(1..row_num, take(int128(['e1671797c52e15f763380b45e841ec32']),row_num));"

                              "cdecimal32 = array(DECIMAL32(6)[], 0, 10).append!(decimal32([1..50, [], rand(100.000000, 10), rand(1..100, 10), take(00i, 3)], 6));"
                              "cdecimal64 = array(DECIMAL64(16)[], 0, 10).append!(decimal64([1..50, [], rand(100.000000, 10), rand(1..100, 10), take(00i, 3)], 16));"
                              "cdecimal128 = array(DECIMAL128(26)[], 0, 10).append!(decimal128([1..50, [], rand(100.000000, 10), rand(1..100, 10), take(00i, 3)], 26));"

                              "for(i in 1..(row_num-5)){"
                              "    cbool.append!([bool(take(0 1 ,50))]);"
                              "    cchar.append!([char(take(256 ,50))]);cshort.append!([short(take(-10000..10000 ,50))]);cint.append!([int(take(-10000..10000 ,50))]);"
                              "    clong.append!([long(take(-10000..10000 ,50))]);cdate.append!([date(take(10000 ,50))]);cmonth.append!([month(take(23640..25000 ,50))]);"
                              "    ctime.append!([time(take(10000 ,50))]);cminute.append!([minute(take(100 ,50))]);csecond.append!([second(take(100 ,50))]);"
                              "    cdatetime.append!([datetime(take(10000 ,50))]);ctimestamp.append!([timestamp(take(10000 ,50))]);"
                              "    cnanotime.append!([nanotime(take(10000 ,50))]);cnanotimestamp.append!([nanotimestamp(take(10000 ,50))]);"
                              "    cdatehour.append!([datehour(take(10000 ,50))]);"
                              "    cfloat.append!([float(rand(10000.0000,50))]);cdouble.append!([rand(10000.0000, 50)]);"
                              "    cdecimal32.append!([decimal32([`1.123123123123123123123123123] ,6)]);"
                              "    cdecimal64.append!([decimal64([`1.123123123123123123123123123] ,16)]);"
                              "    cdecimal128.append!([decimal128([`1.123123123123123123123123123] ,26)])};go;"
                              "table1_SPCT=table(ts,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cdatehour,cfloat,cdouble,cipaddr,cuuid,cint128,cdecimal32,cdecimal64,cdecimal128);"
                              "replay(inputTables=table1_SPCT, outputTables=`"+st+", dateColumn=`ts, timeColumn=`ts);";
        conn.run(replayScript);
    }

    INSTANTIATE_TEST_SUITE_P(, StreamingThreadedClientTester, testing::Values(0, rand() % 1000 + 13000));
    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000);
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
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, st, "mutiSchemaOne", 0));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, onehandler, st, "mutiSchemaOne", 0);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, st, "mutiSchemaOne");

            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+");}catch(ex){}");
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000);
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
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, st, "mutiSchemaBatch", 0));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, batchhandler, st, "mutiSchemaBatch", 0);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, st, "mutiSchemaBatch");

            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+");}catch(ex){}");
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_tableNameNull)
    {
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
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        auto onehandler = [&](Message msg)
        {
            msg_total += 1;
            // cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, st, "actionTest", -1));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, onehandler, st, "actionTest", -1);

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, st, "actionTest");
            threadedClient.exit();
            EXPECT_TRUE(threadedClient.isExit());
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 0);
        }
        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+");}catch(ex){}");
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_offsetNegative)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000);
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
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, st, "actionTest", -1));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, batchhandler, st, "actionTest", -1);

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, st, "actionTest");
            threadedClient.exit();
            EXPECT_TRUE(threadedClient.isExit());
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 0);
        }
        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+");}catch(ex){}");
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
    //     auto thread = threadedClient.subscribe(hostName, port, onehandler, st, "actionTest", 2001);
    //     Util::sleep(2000);
    //     cout << "total size: " << msg_total << endl;
    //     threadedClient.unsubscribe(hostName, port, st, "actionTest");
    //     threadedClient.exit();
    //     EXPECT_TRUE(threadedClient.isExit());
    // usedPorts.insert(listenport);
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
    //     auto thread = threadedClient.subscribe(hostName, port, batchhandler, st, "actionTest", 2001);
    //     Util::sleep(2000);
    //     cout << "total size: " << msg_total << endl;
    //     threadedClient.unsubscribe(hostName, port, st, "actionTest");
    //     threadedClient.exit();
    //     EXPECT_TRUE(threadedClient.isExit());
    // usedPorts.insert(listenport);
    //     EXPECT_EQ(msg_total,0);
    // }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_filter)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000);
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
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, st, "actionTest", 0, true, filter));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, onehandler, st, "actionTest", 0, true, filter);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, st, "actionTest");
            threadedClient.exit();
            EXPECT_TRUE(threadedClient.isExit());
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_TRUE(msg_total > 0);
        }
        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+");}catch(ex){}");
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_filter)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000);
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
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, st, "actionTest", 0, true, filter));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, batchhandler, st, "actionTest", 0, true, filter);
            Util::sleep(2000);

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, st, "actionTest");
            threadedClient.exit();
            EXPECT_TRUE(threadedClient.isExit());
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_TRUE(msg_total > 0);
        }
        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+");}catch(ex){}");
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_msgAsTable)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000);
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
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, st, "actionTest", 0, false, nullptr, true));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, onehandler, st, "actionTest", 0, false, nullptr, true);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, st, "actionTest");
            threadedClient.exit();
            EXPECT_TRUE(threadedClient.isExit());
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+");}catch(ex){}");
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_msgAsTable)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 10000);
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
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, st, "actionTest", 0, false, nullptr, false, 2000, 0.1, true));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, batchhandler, st, "actionTest", 0, false, nullptr, false, 2000, 0.1, true);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, st, "actionTest");
            threadedClient.exit();
            EXPECT_TRUE(threadedClient.isExit());
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 10000);
        }
        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+");}catch(ex){}");
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_allowExists)
    {
        GTEST_SKIP() << "server还没有修复allowExists的问题, jira: https://dolphindb1.atlassian.net/browse/D20-14283";
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total,msg_total2 = 0;
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
        auto onehandler2 = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            msg_total2 += 1;
            // cout << msg->getString() << endl;
            if (msg_total2 == 1000)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        ThreadedClient threadedClient2(listenport);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, st, "actionTest", 0, true, nullptr, false, true));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, onehandler, st, "actionTest", 0, false, nullptr, false, true);
            auto thread2 = threadedClient2.subscribe(hostName, port, onehandler2, st, "actionTest", 0, false, nullptr, false, true);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, st, "actionTest");
            threadedClient2.unsubscribe(hostName, port, st, "actionTest");
            threadedClient.exit();
            EXPECT_TRUE(threadedClient.isExit());
            threadedClient2.exit();
            EXPECT_TRUE(threadedClient2.isExit());

            Util::sleep(1000);
            cout << conn.run("exec * from getStreamingStat()[`pubConns]")->getString() << endl;
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(threadedClient2.getQueueDepth(thread2), 0);
            EXPECT_EQ(msg_total, 1000);
            EXPECT_EQ(msg_total2, 1000);
        }
        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+");}catch(ex){}");
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_allowExists)
    {
        GTEST_SKIP() << "server还没有修复allowExists的问题, jira: https://dolphindb1.atlassian.net/browse/D20-14283";
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total,msg_total2 = 0;
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
        auto batchhandler2 = [&](vector<Message> msgs)
        {
            LockGuard<Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
                msg_total2 += 1;
                // cout  << msg->getString() << endl;
            }
            if (msg_total2 == 1000)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        ThreadedClient threadedClient2(listenport);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, st, "actionTest", 0, true, nullptr, false, true));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, batchhandler, st, "actionTest", 0, false, nullptr, false, true);
            auto thread2 = threadedClient2.subscribe(hostName, port, batchhandler2, st, "actionTest", 0, false, nullptr, false, true);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, st, "actionTest");
            threadedClient.exit();
            EXPECT_TRUE(threadedClient.isExit());
            threadedClient2.unsubscribe(hostName, port, st, "actionTest");
            threadedClient2.exit();
            EXPECT_TRUE(threadedClient2.isExit());
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
            EXPECT_EQ(threadedClient2.getQueueDepth(thread2), 0);
            EXPECT_EQ(msg_total2, 1000);
        }
        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+");}catch(ex){}");
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
        EXPECT_ANY_THROW(auto thread = threadedClient.subscribe(hostName, port, batchhandler, "st", "actionTest", 0, false));
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_resub_true)
    {
        vector<Message> msgs;
        auto batchhandler = [&](vector<Message> ms)
        {
            for (auto &msg : ms)
            {
                msgs.push_back(msg);
            }
        };

        int listenport = GetParam();
        string st = "st_" + getRandString(10);
        conn.run("share streamTable(1:0, `sym`val, [SYMBOL, INT]) as "+st+";tableInsert("+st+", `sym1, 1)");
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, st, "resubTest", 0, true));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, batchhandler, st, "resubTest", 0, true);
            Util::sleep(2000);
            conn.run("subinfo = (exec subscriber from getStreamingStat().pubTables where tableName=`"+st+")[0].split(':');"
                     "subClient = subinfo[0];"
                     "subPort=int(subinfo[1]);go;"
                     "stopPublishTable(subClient, subPort, `"+st+", `resubTest)");
            Util::sleep(1000);
            conn.run("tableInsert("+st+", `sym2, 2)");
            Util::sleep(1000);
            threadedClient.unsubscribe(hostName, port, st, "resubTest");
            threadedClient.exit();
            EXPECT_TRUE(threadedClient.isExit());
            EXPECT_EQ(msgs.size(), 2);
        }
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_batchSize)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000);
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
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, st, "actionTest", 0, true, nullptr, false, 1000));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, batchhandler, st, "actionTest", 0, true, nullptr, false, 1000);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, st, "actionTest");
            threadedClient.exit();
            EXPECT_TRUE(threadedClient.isExit());
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+");}catch(ex){}");
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_throttle)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000);
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
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, st, "actionTest", 0, true, nullptr, false, 1000, 1.0));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, batchhandler, st, "actionTest", 0, true, nullptr, false, 1000, 1.0);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, st, "actionTest");
            threadedClient.exit();
            EXPECT_TRUE(threadedClient.isExit());
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+");}catch(ex){}");
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_hostNull)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        auto onehandler = [&](Message msg)
        {
            msg_total += 1;
            // cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        EXPECT_ANY_THROW(threadedClient.subscribe("", port, onehandler, st, "actionTest", 0));

        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+");}catch(ex){}");
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_hostNull)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000);
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
        EXPECT_ANY_THROW(threadedClient.subscribe("", port, batchhandler, st, "actionTest", 0));

        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+");}catch(ex){}");
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_portNull)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        auto onehandler = [&](Message msg)
        {
            msg_total += 1;
            // cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        EXPECT_ANY_THROW(threadedClient.subscribe(hostName, NULL, onehandler, st, "actionTest", 0));

        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+");}catch(ex){}");
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_actionNameNull)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000);
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
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, st, "", 0));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, onehandler, st, "", 0);
            notify.wait();

            TableSP stat = conn.run("getStreamingStat().pubTables");

            EXPECT_EQ(stat->getColumn(0)->getRow(0)->getString(), st);
            EXPECT_EQ(stat->getColumn(2)->getRow(0)->getInt(), 1000);
            EXPECT_EQ(stat->getColumn(3)->getRow(0)->getString(), "");

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, st, "");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+");}catch(ex){}");
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_actionNameNull)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000);
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
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, st, "", 0));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, batchhandler, st, "", 0);
            notify.wait();

            TableSP stat = conn.run("getStreamingStat().pubTables");

            EXPECT_EQ(stat->getColumn(0)->getRow(0)->getString(), st);
            EXPECT_EQ(stat->getColumn(2)->getRow(0)->getInt(), 1000);
            EXPECT_EQ(stat->getColumn(3)->getRow(0)->getString(), "");

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, st, "");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+");}catch(ex){}");
    }

    TEST_P(StreamingThreadedClientTester, test_unsubscribe_onehandler_hostNull)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        auto onehandler = [&](Message msg)
        {
            msg_total += 1;
            // cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, st, "actionTest", 0));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, onehandler, st, "actionTest", 0);

            cout << "total size: " << msg_total << endl;
            EXPECT_ANY_THROW(threadedClient.unsubscribe("", port, st, "actionTest"));

            threadedClient.unsubscribe(hostName, port, st, "actionTest");
            threadedClient.exit();
            EXPECT_TRUE(threadedClient.isExit());
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        }
        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+");}catch(ex){}");
    }

    TEST_P(StreamingThreadedClientTester, test_unsubscribe_portNull)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        auto onehandler = [&](Message msg)
        {
            msg_total += 1;
            // cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, st, "actionTest", 0));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, onehandler, st, "actionTest", 0);
            cout << "total size: " << msg_total << endl;

            EXPECT_ANY_THROW(threadedClient.unsubscribe(hostName, NULL, st, "actionTest"));
            threadedClient.unsubscribe(hostName, port, st, "actionTest");
            threadedClient.exit();
            EXPECT_TRUE(threadedClient.isExit());
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        }
        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+");}catch(ex){}");
    }

    TEST_P(StreamingThreadedClientTester, test_unsubscribe_tableNameNull)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        auto onehandler = [&](Message msg)
        {
            msg_total += 1;
            // cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, st, "actionTest", 0));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, onehandler, st, "actionTest", 0);

            cout << "total size: " << msg_total << endl;
            EXPECT_ANY_THROW(threadedClient.unsubscribe(hostName, port, "", "actionTest"));

            threadedClient.unsubscribe(hostName, port, st, "actionTest");
            threadedClient.exit();
            EXPECT_TRUE(threadedClient.isExit());
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        }
        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+");}catch(ex){}");
    }

    TEST_P(StreamingThreadedClientTester, test_unsubscribe_actionNameNull)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000);
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
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, st, "actionTest", 0));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, onehandler, st, "actionTest", 0);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, st, "");
            TableSP stat = conn.run("getStreamingStat().pubTables");

            EXPECT_EQ(stat->getColumn(0)->getRow(0)->getString(), st);
            EXPECT_EQ(stat->getColumn(2)->getRow(0)->getInt(), 1000);
            EXPECT_EQ(stat->getColumn(3)->getRow(0)->getString(), "actionTest");

            threadedClient.unsubscribe(hostName, port, st, "actionTest");
            threadedClient.exit();
            EXPECT_TRUE(threadedClient.isExit());

            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        }

        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+");}catch(ex){}");
    }

    TEST_P(StreamingThreadedClientTester, tes_onehandler_subscribe_twice)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        auto onehandler = [&](Message msg)
        {
            msg_total += 1;
            // handle msg
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;
        ThreadedClient threadedClient(listenport);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, st, "mutiSchemaOne", 0, false));
        }
        else
        {
            auto thread1 = threadedClient.subscribe(hostName, port, onehandler, st, "mutiSchemaOne", 0, false);
            EXPECT_ANY_THROW(auto thread2 = threadedClient.subscribe(hostName, port, onehandler, st, "mutiSchemaOne", 0, false));

            threadedClient.unsubscribe(hostName, port, st, "mutiSchemaOne");
            Util::sleep(1000);
            cout << "total size: " << msg_total << endl;
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread1), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+");}catch(ex){}");
    }

    TEST_P(StreamingThreadedClientTester, test_batchhandler_subscribe_twice)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000);
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
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, st, "mutiSchemaBatch", 0, false));
        }
        else
        {
            auto thread1 = threadedClient.subscribe(hostName, port, batchhandler, st, "mutiSchemaBatch", 0, false);
            EXPECT_ANY_THROW(auto thread2 = threadedClient.subscribe(hostName, port, batchhandler, st, "mutiSchemaBatch", 0, false));
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, st, "mutiSchemaBatch");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread1), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+");}catch(ex){}");
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_withAllDataType)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay_withAllDataType(st);
        int msg_total = 0;
        TableSP ex_table = conn.run("select * from "+st+"");
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
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, st, "mutiSchemaOne", 0));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, onehandler, st, "mutiSchemaOne", 0);
            notify.wait();
            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, st, "mutiSchemaOne");

            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+");}catch(ex){}");
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_withAllDataType)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay_withAllDataType(st);
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
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, st, "mutiSchemaBatch", 0));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, batchhandler, st, "mutiSchemaBatch", 0);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, st, "mutiSchemaBatch");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+");}catch(ex){}");
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_arrayVector)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay_withArrayVector(st);
        int msg_total = 0;
        int index = 0;
        Signal notify;
        Mutex mutex;

        TableSP ex_tab = conn.run("select * from "+st+"");
        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            msg_total += 1;
            for (auto i = 0; i < ex_tab->columns(); i++)
                ASSERT_EQ(msg->get(i)->getString(), ex_tab->getColumn(i)->get(index)->getString());
            index++;
            if (msg_total == 1000)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;
        ThreadedClient threadedClient(listenport);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, st, "arrayVectorTableTest", 0));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, onehandler, st, "arrayVectorTableTest", 0);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, st, "arrayVectorTableTest");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+");}catch(ex){}");
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_arrayVector)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay_withArrayVector(st);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;

        TableSP ex_tab = conn.run("select * from "+st+"");
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
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, st, "arrayVectorTableTest", 0));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, batchhandler, st, "arrayVectorTableTest", 0);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, st, "arrayVectorTableTest");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+");}catch(ex){}");
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_hugetable_onehandler)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000000);
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
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, st, "mutiSchemaOne", 0));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, onehandler, st, "mutiSchemaOne", 0);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, st, "mutiSchemaOne");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000000);
        }
        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+");}catch(ex){}");
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_hugetable_batchhandler)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000000);
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
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, st, "mutiSchemaBatch", 0));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, batchhandler, st, "mutiSchemaBatch", 0);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, st, "mutiSchemaBatch");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000000);
        }
        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+");}catch(ex){}");
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_hugetable_onehandler_msgAsTable)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000000);
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
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, st, "mutiSchemaOne", 0, false, nullptr, true));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, onehandler, st, "mutiSchemaOne", 0, false, nullptr, true);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, st, "mutiSchemaOne");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000000);
        }
        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+");}catch(ex){}");
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_hugetable_batchhandler_msgAsTable)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000000);
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
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, st, "mutiSchemaBatch", 0, false, nullptr, false, 1, 1.0, true));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, batchhandler, st, "mutiSchemaBatch", 0, false, nullptr, false, 1, 1.0, true);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, st, "mutiSchemaBatch");
            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000000);
        }
        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+");}catch(ex){}");
    }

}