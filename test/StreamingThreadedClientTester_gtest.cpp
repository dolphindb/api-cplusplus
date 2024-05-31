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
            CLEAR_ENV(conn);
        }
        virtual void TearDown()
        {
            CLEAR_ENV(conn);
        }
    };

    static void createSharedTableAndReplay(const string &st, int rows)
    {
        string script = "login(\"admin\",\"123456\")\n\
                st1 = streamTable(100:0, `datetimev`timestampv`sym`price1`price2,[DATETIME,TIMESTAMP,SYMBOL,DOUBLE,DOUBLE])\n\
                enableTableShareAndPersistence(table=st1, tableName=`"+st+")\n\
                go\n\
                setStreamTableFilterColumn("+st+", `sym)";
        conn.run(script);

        string replayScript = "n = " + to_string(rows) + ";table1_STCT = table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);\
                share table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]) as res_STCT;assert res_STCT.rows() == 0;go;\
                tableInsert(table1_STCT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));share table1_STCT as ex_STCT;\
                replay(inputTables=table1_STCT, outputTables=`"+st+", dateColumn=`timestampv, timeColumn=`timestampv)";
        conn.run(replayScript);
    }

    static void createSharedTableAndReplay_withAllDataType(const string &st)
    {
        srand(time(NULL));
        int scale32 = rand() % 9;
        int scale64 = rand() % 18;
        int scale128 = rand() % 38;
        printf("#[PARAM] scale32: %d, scale64: %d, scale128: %d\n", scale32, scale64, scale128);
        string replayScript = 
            "colName =  `ind`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring`cblob`cipaddr`cuuid`cint128`cdecimal32`cdecimal64`cdecimal128;"
            "colType = [INT, BOOL, CHAR, SHORT, INT,LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING, BLOB, IPADDR, UUID, INT128, DECIMAL32(" +
            to_string(scale32) + "), DECIMAL64(" + to_string(scale64) + "), DECIMAL128(" + to_string(scale128) + ")];"
            "st1 = streamTable(100:0,colName, colType);"
            "enableTableShareAndPersistence(table=st1, tableName=`"+st+", asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0);go;"
            "setStreamTableFilterColumn("+st+", `csymbol);go;"
            "table1_STCT = table(1:0, colName, colType);"
            "row_num = 1000;"
            "share table(1:0, colName, colType) as res_STCT;assert res_STCT.rows() == 0;go;"
            "col1 = rand(2 ,row_num);col2 = rand(256 ,row_num);col3 = rand(-row_num..row_num ,row_num);col4 = rand(-row_num..row_num ,row_num);col5 = rand(-row_num..row_num ,row_num);col6 = rand(0..row_num ,row_num);col7 = rand(0..row_num ,row_num);"
            "col8 = rand(0..row_num ,row_num);col9 = rand(0..row_num ,row_num);col10 = rand(0..row_num ,row_num);col11 = rand(0..row_num ,row_num);col12 = rand(0..row_num ,row_num);col13 = rand(0..row_num ,row_num);col14= rand(0..row_num ,row_num);"
            "col15 = rand(round(row_num,2) ,row_num);col16 = rand(round(row_num,2) ,row_num);col17 =rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);col18 =rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);col19 =rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);col20 = take(ipaddr(\"192.168.1.13\"),row_num);"
            "col21 = take(uuid(\"5d212a78-cc48-e3b1-4235-b4d91473ee87\"),row_num);col22 = take(int128(\"e1671797c52e15f763380b45e841ec32\"),row_num);col23=rand((-1000..1000)/1000.0000$DECIMAL32(" +
            to_string(scale32) + "),row_num);col24=rand((-1000..1000)/1000.0000$DECIMAL64(" + to_string(scale64) + "),row_num);col25=rand((-1000..1000)/1000.0000$DECIMAL128(" + to_string(scale128) + "),row_num);go;"
            "for (i in 0..(row_num-1)){tableInsert(table1_STCT,i,col1[i],col2[i],col3[i],col4[i],col5[i],col6[i],col7[i],col8[i],col9[i],col10[i],col11[i],col12[i],col13[i],col14[i],col15[i],col16[i],col17[i],col18[i],col19[i],col20[i],col21[i],col22[i],col23[i],col24[i],col25[i])};"
            "share table1_STCT as ex_STCT;go;"
            "replay(inputTables=table1_STCT, outputTables=`"+st+", dateColumn=`ctimestamp, timeColumn=`ctimestamp);";
        conn.run(replayScript);
    }

    static void createSharedTableAndReplay_withArrayVector(const string &st)
    {
        string replayScript = 
            "colName = `ts`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cdatehour`cfloat`cdouble`cipaddr`cuuid`cint128`cdecimal32`cdecimal64`cdecimal128;"
            "colType = [TIMESTAMP, BOOL[], CHAR[], SHORT[], INT[],LONG[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], DATEHOUR[], FLOAT[], DOUBLE[], IPADDR[], UUID[], INT128[],DECIMAL32(6)[],DECIMAL64(16)[],DECIMAL128(26)[]];"
            "st1 = streamTable(1:0,colName, colType);"
            "share table(1:0, colName, colType) as res_STCT;assert res_STCT.rows() == 0;go;"
            "enableTableShareAndPersistence(table=st1, tableName=`"+st+");go;"
            "row_num = 1000;"
            "ind = [1,2,3,4,row_num];"
            "ts=now()..(now()+row_num-1);"
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
            "table1_STCT=table(ts,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cdatehour,cfloat,cdouble,cipaddr,cuuid,cint128,cdecimal32,cdecimal64,cdecimal128);"
            "share table1_STCT as ex_STCT;go;"
            "replay(inputTables=table1_STCT, outputTables=`"+st+", dateColumn=`ts, timeColumn=`ts);";
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
        AutoFitTableAppender appender("", "res_STCT", conn);

        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            bool succeeded = false; 
            TableSP tmp = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    appender.append(tmp);
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    Util::sleep(100);
                }
            }
            msg_total+=1;
            if (msg.getOffset() == 999)
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

            EXPECT_TRUE(conn.run("re = select * from res_STCT order by datetimev;\
                                ex = select * from ex_STCT order by datetimev;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_STCT", conn);

        auto batchhandler = [&](vector<Message> msgs)
        {
            LockGuard<Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
                msg_total += 1;
                bool succeeded = false; 
                TableSP tmp = AnyVectorToTable(msg);
                while(!succeeded){
                    try
                    {
                        appender.append(tmp);
                        succeeded = true;
                    }
                    catch(const std::exception& e)
                    {
                        Util::sleep(100);
                    }
                }
                if (msg.getOffset() == 999)
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

            EXPECT_TRUE(conn.run("re = select * from res_STCT order by datetimev;\
                                ex = select * from ex_STCT order by datetimev;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribeUnsubscribe_notExistTable)
    {
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, [](Message msg){}, "st_notExist", "actionTest", 0));
        }
        else
        {
            auto queue = threadedClient.subscribe(hostName, port, [](Message msg){}, "st_notExist", "actionTest");
            Util::sleep(1000);
        }
        threadedClient.unsubscribe(hostName, port, "st_notExist", "actionTest");
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_tableNameNull)
    {
        auto onehandler = [](Message msg){};

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);

        EXPECT_ANY_THROW(auto thread = threadedClient.subscribe(hostName, port, onehandler, "", "cppStreamingAPI", 0, false));
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_tableNameNull)
    {
        auto batchhandler = [](vector<Message> msgs){};

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
    }

    TEST_P(StreamingThreadedClientTester,test_subscribe_onehandler_offsetInMiddle)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 10);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;

        AutoFitTableAppender appender("", "res_STCT", conn);

        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            bool succeeded = false; 
            TableSP tmp = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    appender.append(tmp);
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    Util::sleep(100);
                }
            }
            msg_total+=1;

            int cur_offset = msg.getOffset();
            EXPECT_EQ(cur_offset, msg_total+4);
            if (cur_offset == 9)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, st, "actionTest", 5));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, onehandler, st, "actionTest", 5);
            notify.wait();
            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, st, "actionTest");
            threadedClient.exit();
            EXPECT_TRUE(threadedClient.isExit());
            EXPECT_TRUE(conn.run(("eqObj(res_STCT.rows(), 5)"))->getBool());
            EXPECT_TRUE(conn.run("re = select * from res_STCT order by datetimev;\
                                ex = select * from ex_STCT order by datetimev limit 5, 5;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 5);
        }
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_offsetInMiddle)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 10);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;

        AutoFitTableAppender appender("", "res_STCT", conn);

        auto batchhandler = [&](vector<Message> msgs)
        {
            LockGuard<Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
                msg_total += 1;
                bool succeeded = false; 
                TableSP tmp = AnyVectorToTable(msg);
                while(!succeeded){
                    try
                    {
                        appender.append(tmp);
                        succeeded = true;
                    }
                    catch(const std::exception& e)
                    {
                        Util::sleep(100);
                    }
                }
                int cur_offset = msg.getOffset();
                EXPECT_EQ(cur_offset, msg_total+4);
                if (cur_offset == 9)
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
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, st, "actionTest", 5));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, batchhandler, st, "actionTest", 5);
            notify.wait();
            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, st, "actionTest");
            threadedClient.exit();
            EXPECT_TRUE(threadedClient.isExit());
            EXPECT_TRUE(conn.run(("eqObj(res_STCT.rows(), 5)"))->getBool());
            EXPECT_TRUE(conn.run("re = select * from res_STCT order by datetimev;\
                                ex = select * from ex_STCT order by datetimev limit 5, 5;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 5);
        }
        usedPorts.insert(listenport);
    }


    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_filter)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_STCT", conn);
        int target_rows = conn.run("(exec count(*) from ex_STCT where sym='b')[0]")->getInt();

        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            bool succeeded = false; 
            TableSP tmp = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    appender.append(tmp);
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    Util::sleep(100);
                }
            }
            msg_total+=1;
            if (msg_total == target_rows)
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
            EXPECT_TRUE(conn.run("re = exec sym from res_STCT; all(re == `b)")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_TRUE(msg_total > 0);
        }
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_filter)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_STCT", conn);

        auto batchhandler = [&](vector<Message> msgs)
        {
            LockGuard<Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
                msg_total += 1;
                bool succeeded = false; 
                TableSP tmp = AnyVectorToTable(msg);
                while(!succeeded){
                    try
                    {
                        appender.append(tmp);
                        succeeded = true;
                    }
                    catch(const std::exception& e)
                    {
                        Util::sleep(100);
                    }
                }
                if (msg.getOffset() == 999)
                {
                    notify.set();
                }
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
            EXPECT_TRUE(conn.run("re = exec sym from res_STCT; all(re == `b)")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_TRUE(msg_total > 0);
        }
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_msgAsTable)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_STCT", conn);

        auto onehandler = [&](Message msg)
        {
            ASSERT_EQ(msg->getForm(), DF_TABLE);
            LockGuard<Mutex> lock(&mutex);
            bool succeeded = false; 
            while(!succeeded){
                try
                {
                    appender.append(msg);
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    Util::sleep(100);
                }
            }
            msg_total += msg->rows();
            int first_offset = msg.getOffset();
            int last_offset = first_offset + msg->rows() -1;
            if (last_offset == 999)
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
            EXPECT_TRUE(conn.run("re = select * from res_STCT order by datetimev;\
                                ex = select * from ex_STCT order by datetimev;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_msgAsTable)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 10000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_STCT", conn);

        auto batchhandler = [&](vector<Message> msgs)
        {
            LockGuard<Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
                ASSERT_EQ(msg->getForm(), DF_TABLE);
                msg_total += msg->rows();
                cout << msg->rows() << endl;
                bool succeeded = false; 
                while(!succeeded){
                    try
                    {
                        appender.append(msg);
                        succeeded = true;
                    }
                    catch(const std::exception& e)
                    {
                        Util::sleep(100);
                    }
                }
                int first_offset = msg.getOffset();
                int last_offset = first_offset + msg->rows() -1;
                if (last_offset == 9999)
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
            EXPECT_TRUE(conn.run("re = select * from res_STCT order by datetimev;\
                                ex = select * from ex_STCT order by datetimev;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 10000);
        }
        usedPorts.insert(listenport);
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
            if (msg.getOffset() == 999)
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
                if (msg.getOffset() == 999)
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
                if (msg.getOffset() == 999)
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
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_resub_false)
    {
        auto batchhandler = [&](vector<Message> msgs){};

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
        AutoFitTableAppender appender("", "res_STCT", conn);

        auto batchhandler = [&](vector<Message> msgs)
        {
            LockGuard<Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
                msg_total += 1;
                bool succeeded = false; 
                TableSP tmp = AnyVectorToTable(msg);
                while(!succeeded){
                    try
                    {
                        appender.append(tmp);
                        succeeded = true;
                    }
                    catch(const std::exception& e)
                    {
                        Util::sleep(100);
                    }
                }
                if (msg.getOffset() == 999)
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
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, st, "actionTest", 0, true, nullptr, false, 200));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, batchhandler, st, "actionTest", 0, true, nullptr, false, 200);
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, st, "actionTest");
            threadedClient.exit();
            EXPECT_TRUE(threadedClient.isExit());
            EXPECT_TRUE(conn.run("re = select * from res_STCT order by datetimev;\
                                ex = select * from ex_STCT order by datetimev;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_throttle)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 10);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_STCT", conn);
        long long start, end;

        auto batchhandler = [&](vector<Message> msgs)
        {
            LockGuard<Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
                msg_total += 1;
                bool succeeded = false; 
                TableSP tmp = AnyVectorToTable(msg);
                while(!succeeded){
                    try
                    {
                        appender.append(tmp);
                        succeeded = true;
                    }
                    catch(const std::exception& e)
                    {
                        Util::sleep(100);
                    }
                }
                if (msg.getOffset() == 9)
                {
                    notify.set();
                    end = Util::getEpochTime();
                }
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadedClient threadedClient(listenport);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, st, "actionTest", 0, true, nullptr, false, 5000, 2.5));
        }
        else
        {
            auto thread = threadedClient.subscribe(hostName, port, batchhandler, st, "actionTest", 0, true, nullptr, false, 5000, 2.5);
            start = Util::getEpochTime();
            notify.wait();

            cout << "total size: " << msg_total << endl;
            threadedClient.unsubscribe(hostName, port, st, "actionTest");
            threadedClient.exit();
            EXPECT_TRUE(threadedClient.isExit());
            EXPECT_TRUE(conn.run("re = select * from res_STCT order by datetimev;\
                                ex = select * from ex_STCT order by datetimev;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 10);

            auto duration = end - start;
            EXPECT_TRUE(duration >= 2500);
        }
        usedPorts.insert(listenport);
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
            if (msg.getOffset() == 999)
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
                if (msg.getOffset() == 999)
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
            if (msg.getOffset() == 999)
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
                if (msg.getOffset() == 999)
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
        AutoFitTableAppender appender("", "res_STCT", conn);

        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            bool succeeded = false; 
            TableSP tmp = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    appender.append(tmp);
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    Util::sleep(100);
                }
            }
            msg_total+=1;
            if (msg.getOffset() == 999)
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

            EXPECT_TRUE(conn.run("re = select * from res_STCT order by ind;\
                                ex = select * from ex_STCT order by ind;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_withAllDataType)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay_withAllDataType(st);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_STCT", conn);

        auto batchhandler = [&](vector<Message> msgs)
        {
            LockGuard<Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
                msg_total += 1;
                bool succeeded = false; 
                TableSP tmp = AnyVectorToTable(msg);
                while(!succeeded){
                    try
                    {
                        appender.append(tmp);
                        succeeded = true;
                    }
                    catch(const std::exception& e)
                    {
                        Util::sleep(100);
                    }
                }
                if (msg.getOffset() == 999)
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
            EXPECT_TRUE(conn.run("re = select * from res_STCT order by ind;\
                                ex = select * from ex_STCT order by ind;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_arrayVector)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay_withArrayVector(st);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_STCT", conn);

        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            bool succeeded = false; 
            TableSP tmp = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    appender.append(tmp);
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    Util::sleep(100);
                }
            }
            msg_total+=1;
            if (msg.getOffset() == 999)
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
            EXPECT_TRUE(conn.run("re = select * from res_STCT order by ts;\
                                ex = select * from ex_STCT order by ts;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_arrayVector)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay_withArrayVector(st);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_STCT", conn);

        auto batchhandler = [&](vector<Message> msgs)
        {
            LockGuard<Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
                msg_total += 1;
                bool succeeded = false; 
                TableSP tmp = AnyVectorToTable(msg);
                while(!succeeded){
                    try
                    {
                        appender.append(tmp);
                        succeeded = true;
                    }
                    catch(const std::exception& e)
                    {
                        Util::sleep(100);
                    }
                }
                if (msg.getOffset() == 999)
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
            EXPECT_TRUE(conn.run("re = select * from res_STCT order by ts;\
                                ex = select * from ex_STCT order by ts;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.insert(listenport);
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
            if (msg.getOffset() == 999999)
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
                if (msg.getOffset() == 999999)
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
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_hugetable_onehandler_msgAsTable)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_STCT", conn);

        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            bool succeeded = false; 
            while(!succeeded){
                try
                {
                    appender.append(msg);
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    Util::sleep(100);
                }
            }
            msg_total += msg->rows();
            int first_offset = msg.getOffset();
            int last_offset = first_offset + msg->rows() - 1;
            if (last_offset == 999999)
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
            EXPECT_TRUE(conn.run("re = select * from res_STCT order by datetimev;\
                                ex = select * from ex_STCT order by datetimev;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000000);
        }
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_hugetable_batchhandler_msgAsTable)
    {
        string st = "outTables_" + getRandString(10);
        STCT::createSharedTableAndReplay(st, 1000000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_STCT", conn);

        auto batchhandler = [&](vector<Message> msgs)
        {
            LockGuard<Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
                msg_total += msg->rows();
                bool succeeded = false; 
                while(!succeeded){
                    try
                    {
                        appender.append(msg);
                        succeeded = true;
                    }
                    catch(const std::exception& e)
                    {
                        Util::sleep(100);
                    }
                }
                int first_offset = msg.getOffset();
                int last_offset = first_offset + msg->rows() - 1;
                if (last_offset == 999999)
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
            EXPECT_TRUE(conn.run("re = select * from res_STCT order by datetimev;\
                                ex = select * from ex_STCT order by datetimev;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

            EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
            EXPECT_EQ(msg_total, 1000000);
        }
        usedPorts.insert(listenport);
    }

}