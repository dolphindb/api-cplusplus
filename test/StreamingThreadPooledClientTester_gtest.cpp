#include "config.h"

namespace STPCT
{
    class StreamingThreadPooledClientTester : public testing::Test, public ::testing::WithParamInterface<int>
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

        string replayScript = "n = " + to_string(rows) + ";table1_STPCT = table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);\
                share table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]) as res_STPCT;assert res_STPCT.rows() == 0;go;\
                tableInsert(table1_STPCT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));share table1_STPCT as ex_STPCT;\
                replay(inputTables=table1_STPCT, outputTables=`"+st+", dateColumn=`timestampv, timeColumn=`timestampv)";
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
            "table1_STPCT = table(1:0, colName, colType);"
            "row_num = 1000;"
            "share table(1:0, colName, colType) as res_STPCT;assert res_STPCT.rows() == 0;go;"
            "col1 = rand(2 ,row_num);col2 = rand(256 ,row_num);col3 = rand(-row_num..row_num ,row_num);col4 = rand(-row_num..row_num ,row_num);col5 = rand(-row_num..row_num ,row_num);col6 = rand(0..row_num ,row_num);col7 = rand(0..row_num ,row_num);"
            "col8 = rand(0..row_num ,row_num);col9 = rand(0..row_num ,row_num);col10 = rand(0..row_num ,row_num);col11 = rand(0..row_num ,row_num);col12 = rand(0..row_num ,row_num);col13 = rand(0..row_num ,row_num);col14= rand(0..row_num ,row_num);"
            "col15 = rand(round(row_num,2) ,row_num);col16 = rand(round(row_num,2) ,row_num);col17 =rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);col18 =rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);col19 =rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);col20 = take(ipaddr(\"192.168.1.13\"),row_num);"
            "col21 = take(uuid(\"5d212a78-cc48-e3b1-4235-b4d91473ee87\"),row_num);col22 = take(int128(\"e1671797c52e15f763380b45e841ec32\"),row_num);col23=rand((-1000..1000)/1000.0000$DECIMAL32(" +
            to_string(scale32) + "),row_num);col24=rand((-1000..1000)/1000.0000$DECIMAL64(" + to_string(scale64) + "),row_num);col25=rand((-1000..1000)/1000.0000$DECIMAL128(" + to_string(scale128) + "),row_num);go;"
            "for (i in 0..(row_num-1)){tableInsert(table1_STPCT,i,col1[i],col2[i],col3[i],col4[i],col5[i],col6[i],col7[i],col8[i],col9[i],col10[i],col11[i],col12[i],col13[i],col14[i],col15[i],col16[i],col17[i],col18[i],col19[i],col20[i],col21[i],col22[i],col23[i],col24[i],col25[i])};"
            "share table1_STPCT as ex_STPCT;go;"
            "replay(inputTables=table1_STPCT, outputTables=`"+st+", dateColumn=`ctimestamp, timeColumn=`ctimestamp);";
        conn.run(replayScript);
    }

    static void createSharedTableAndReplay_withArrayVector(const string &st)
    {
        string replayScript = 
            "colName = `ts`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cdatehour`cfloat`cdouble`cipaddr`cuuid`cint128`cdecimal32`cdecimal64`cdecimal128;"
            "colType = [TIMESTAMP, BOOL[], CHAR[], SHORT[], INT[],LONG[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], DATEHOUR[], FLOAT[], DOUBLE[], IPADDR[], UUID[], INT128[],DECIMAL32(6)[],DECIMAL64(16)[],DECIMAL128(26)[]];"
            "st1 = streamTable(1:0,colName, colType);"
            "share table(1:0, colName, colType) as res_STPCT;assert res_STPCT.rows() == 0;go;"
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
            "table1_STPCT=table(ts,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cdatehour,cfloat,cdouble,cipaddr,cuuid,cint128,cdecimal32,cdecimal64,cdecimal128);"
            "share table1_STPCT as ex_STPCT;go;"
            "replay(inputTables=table1_STPCT, outputTables=`"+st+", dateColumn=`ts, timeColumn=`ts);";
        conn.run(replayScript);
    }

    INSTANTIATE_TEST_SUITE_P(, StreamingThreadPooledClientTester, testing::Values(0, rand() % 1000 + 13000));
    TEST_P(StreamingThreadPooledClientTester, test_client_threadCount_1)
    {
        string st = "outTables_" + getRandString(10);
        STPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_STPCT", conn);

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
        ThreadPooledClient client(listenport, 1);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, st, "actionTest", 0));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, st, "actionTest", 0);
            EXPECT_EQ(threadVec.size(), 1);

            notify.wait();
            cout << "total size:" << msg_total << endl;
            client.unsubscribe(hostName, port, st, "actionTest");

            EXPECT_TRUE(conn.run("re = select * from res_STPCT order by datetimev;\
                                ex = select * from ex_STPCT order by datetimev;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

            EXPECT_EQ(msg_total, 1000);
            for (auto &t : threadVec)
            {
                EXPECT_TRUE(t->isComplete());
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_client_threadCount_2)
    {
        string st = "outTables_" + getRandString(10);
        STPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_STPCT", conn);

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
        ThreadPooledClient client(listenport, 2);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, st, "actionTest", 0));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, st, "actionTest", 0);
            EXPECT_EQ(threadVec.size(), 2);

            notify.wait();
            cout << "total size:" << msg_total << endl;
            client.unsubscribe(hostName, port, st, "actionTest");

            EXPECT_TRUE(conn.run("re = select * from res_STPCT order by datetimev;\
                                ex = select * from ex_STPCT order by datetimev;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

            EXPECT_EQ(msg_total, 1000);
            for (auto &t : threadVec)
            {
                EXPECT_TRUE(t->isComplete());
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_client_threadCount_4)
    {
        string st = "outTables_" + getRandString(10);
        STPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_STPCT", conn);

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
        ThreadPooledClient client(listenport, 4);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, st, "actionTest", 0));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, st, "actionTest", 0);
            EXPECT_EQ(threadVec.size(), 4);

            notify.wait();
            cout << "total size:" << msg_total << endl;
            client.unsubscribe(hostName, port, st, "actionTest");

            EXPECT_TRUE(conn.run("re = select * from res_STPCT order by datetimev;\
                                ex = select * from ex_STPCT order by datetimev;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

            EXPECT_EQ(msg_total, 1000);
            for (auto &t : threadVec)
            {
                EXPECT_TRUE(t->isComplete());
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_client_threadCount_8)
    {
        string st = "outTables_" + getRandString(10);
        STPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_STPCT", conn);

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
        ThreadPooledClient client(listenport, 8);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, st, "actionTest", 0));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, st, "actionTest", 0);
            EXPECT_EQ(threadVec.size(), 8);

            notify.wait();
            cout << "total size:" << msg_total << endl;
            client.unsubscribe(hostName, port, st, "actionTest");

            EXPECT_TRUE(conn.run("re = select * from res_STPCT order by datetimev;\
                                ex = select * from ex_STPCT order by datetimev;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

            EXPECT_EQ(msg_total, 1000);
            for (auto &t : threadVec)
            {
                EXPECT_TRUE(t->isComplete());
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_client_threadCount_16)
    {
        string st = "outTables_" + getRandString(10);
        STPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_STPCT", conn);

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
        ThreadPooledClient client(listenport, 16);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, st, "actionTest", 0));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, st, "actionTest", 0);
            EXPECT_EQ(threadVec.size(), 16);

            notify.wait();
            cout << "total size:" << msg_total << endl;
            client.unsubscribe(hostName, port, st, "actionTest");

            EXPECT_TRUE(conn.run("re = select * from res_STPCT order by datetimev;\
                                ex = select * from ex_STPCT order by datetimev;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

            EXPECT_EQ(msg_total, 1000);
            for (auto &t : threadVec)
            {
                EXPECT_TRUE(t->isComplete());
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_client_threadCount_32)
    {
        string st = "outTables_" + getRandString(10);
        STPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_STPCT", conn);

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
        ThreadPooledClient client(listenport, 32);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, st, "actionTest", 0));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, st, "actionTest", 0);
            EXPECT_EQ(threadVec.size(), 32);

            notify.wait();
            cout << "total size:" << msg_total << endl;
            client.unsubscribe(hostName, port, st, "actionTest");

            EXPECT_TRUE(conn.run("re = select * from res_STPCT order by datetimev;\
                                ex = select * from ex_STPCT order by datetimev;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

            EXPECT_EQ(msg_total, 1000);
            for (auto &t : threadVec)
            {
                EXPECT_TRUE(t->isComplete());
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribeUnsubscribe_notExistTable)
    {
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadPooledClient client(listenport, 2);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, [](Message msg){}, "st_notExist", "actionTest", 0));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, [](Message msg){}, "st_notExist", "actionTest");
            Util::sleep(1000);
        }
        client.unsubscribe(hostName, port, "st_notExist", "actionTest");
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribe_onehandler_hostNull)
    {
        string st = "outTables_" + getRandString(10);
        STPCT::createSharedTableAndReplay(st, 1000);

        auto onehandler = [&](Message msg){};

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        Util::sleep(1000);
        EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ThreadPooledClient client(listenport, 2);
        EXPECT_ANY_THROW(client.subscribe("", port, onehandler, st, "actionTest", 0));
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribe_onehandler_portNull)
    {
        string st = "outTables_" + getRandString(10);
        STPCT::createSharedTableAndReplay(st, 1000);
        auto onehandler = [&](Message msg){};

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        Util::sleep(1000);
        EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ThreadPooledClient client(listenport, 2);
        EXPECT_ANY_THROW(client.subscribe(hostName, NULL, onehandler, st, "actionTest", 0));
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribe_onehandler_actionNameNull)
    {
        string st = "outTables_" + getRandString(10);
        STPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        Signal notify;
        Mutex mutex;
        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            msg_total += 1;
            if (msg.getOffset() == 999)
            {
                notify.set();
            }
            // cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadPooledClient client(listenport, 2);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, st, "", 0));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, st, "", 0);
            notify.wait();
            TableSP stat = conn.run("getStreamingStat().pubTables");

            EXPECT_EQ(stat->getColumn(0)->getRow(0)->getString(), st);
            EXPECT_EQ(stat->getColumn(2)->getRow(0)->getInt(), 1000);
            EXPECT_EQ(stat->getColumn(3)->getRow(0)->getString(), "");

            cout << "total size:" << msg_total << endl;
            client.unsubscribe(hostName, port, st, "");

            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

            EXPECT_EQ(msg_total, 1000);
            for (auto &t : threadVec)
            {
                EXPECT_TRUE(t->isComplete());
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribe_onehandler_tableNameNull)
    {
        string st = "outTables_" + getRandString(10);
        STPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        auto onehandler = [&](Message msg)
        {
            msg_total += 1;
            cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadPooledClient client(listenport, 2);
        EXPECT_ANY_THROW(auto threadVec = client.subscribe(hostName, port, onehandler, "", "actionTest", 0, false));
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribe_onehandler_offsetNegative)
    {
        string st = "outTables_" + getRandString(10);
        STPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        auto onehandler = [&](Message msg)
        {
            msg_total += 1;
            // cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadPooledClient client(listenport, 2);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, st, "actionTest", -1));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, st, "actionTest", -1);
            cout << "total size:" << msg_total << endl;
            client.unsubscribe(hostName, port, st, "actionTest");

            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

            EXPECT_EQ(msg_total, 0);
            for (auto &t : threadVec)
            {
                EXPECT_TRUE(t->isComplete());
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribe_onehandler_filter)
    {
        string st = "outTables_" + getRandString(10);
        STPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        int target_rows = conn.run("(exec count(*) from "+st+" where sym='b')[0]")->getInt();

        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_STPCT", conn);

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

        ThreadPooledClient client(listenport, 2);
        VectorSP filter = Util::createVector(DT_SYMBOL, 1, 1);
        filter->setString(0, "b");
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, st, "actionTest", 0, true, filter));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, st, "actionTest", 0, true, filter);
            cout << "total size:" << msg_total << endl;
            notify.wait();

            client.unsubscribe(hostName, port, st, "actionTest");

            EXPECT_TRUE(conn.run("re = exec sym from res_STPCT; all(re == `b)")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

            EXPECT_EQ(msg_total > 0, true);
            for (auto &t : threadVec)
            {
                EXPECT_TRUE(t->isComplete());
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribe_onehandler_msgAsTable)
    {
        string st = "outTables_" + getRandString(10);
        STPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_STPCT", conn);

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
            int last_offset = first_offset + msg->rows() - 1;
            if (last_offset == 999)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadPooledClient client(listenport, 2);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, st, "actionTest", 0, true, nullptr, true));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, st, "actionTest", 0, true, nullptr, true);
            notify.wait();
            cout << "total size:" << msg_total << endl;
            client.unsubscribe(hostName, port, st, "actionTest");

            EXPECT_TRUE(conn.run("re = select * from res_STPCT order by datetimev;\
                                ex = select * from ex_STPCT order by datetimev;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

            EXPECT_EQ(msg_total, 1000);
            for (auto &t : threadVec)
            {
                EXPECT_TRUE(t->isComplete());
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribe_onehandler_allowExists)
    {
        GTEST_SKIP() << "server还没有修复allowExists的问题, jira: https://dolphindb1.atlassian.net/browse/D20-14283";
        string st = "outTables_" + getRandString(10);
        STPCT::createSharedTableAndReplay(st, 1000);
        int msg_total,msg_total2 = 0;

        Signal notify;
        Mutex mutex;
        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            msg_total += 1;
            if (msg.getOffset() == 999)
            {
                notify.set();
            }
            // cout << msg->getString() << endl;
        };
        auto onehandler2 = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            msg_total2 += 1;
            if (msg_total2 == 1000)
            {
                notify.set();
            }
            // cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadPooledClient client(listenport, 2);
        ThreadPooledClient client2(listenport, 5);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, st, "actionTest", 0, true, nullptr, false, true));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, st, "actionTest", 0, false, nullptr, false, true);
            auto threadVec2 = client2.subscribe(hostName, port, onehandler, st, "actionTest", 0, false, nullptr, false, true);
            notify.wait();
            cout << "total size:" << msg_total << endl;
            client.unsubscribe(hostName, port, st, "actionTest");
            client2.unsubscribe(hostName, port, st, "actionTest");

            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

            EXPECT_EQ(msg_total, 1000);
            EXPECT_EQ(msg_total2, 1000);
            for (auto &t : threadVec)
            {
                EXPECT_TRUE(t->isComplete());
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
            for (auto &t : threadVec2)
            {
                EXPECT_TRUE(t->isComplete());
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_client_resub_false)
    {
        auto onehandler = [&](Message msg){};

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadPooledClient client(listenport, 1);
        EXPECT_ANY_THROW(auto threadVec = client.subscribe(hostName, port, onehandler, "st", "actionTest", 0, false));
    }

    TEST_P(StreamingThreadPooledClientTester, test_client_resub_true)
    {
        vector<Message> msgs;
        auto onehandler = [&](Message m)
        {
            msgs.push_back(m);
        };

        int listenport = GetParam();
        string st = "st_" + getRandString(10);
        conn.run("share streamTable(1:0, `sym`val, [SYMBOL, INT]) as "+st+";tableInsert("+st+", `sym1, 1)");
        cout << "current listenport is " << listenport << endl;

        ThreadPooledClient client(listenport, 1);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, st, "resubTest", 0, true));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, st, "resubTest", 0, true);
            Util::sleep(2000);
            conn.run("subinfo = (exec subscriber from getStreamingStat().pubTables where tableName=`"+st+")[0].split(':');"
                     "subClient = subinfo[0];"
                     "subPort=int(subinfo[1]);go;"
                     "stopPublishTable(subClient, subPort, `"+st+", `resubTest)");
            Util::sleep(1000);
            conn.run("tableInsert("+st+", `sym2, 2)");
            Util::sleep(1000);
            client.unsubscribe(hostName, port, st, "resubTest");

            EXPECT_EQ(msgs.size(), 2);
        }
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_client_threadCount_subscribe_twice)
    {
        string st = "outTables_" + getRandString(10);
        STPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_STPCT", conn);

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

        int threadCount = rand() % 10 + 1;
        ThreadPooledClient client(listenport, threadCount);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, st, "actionTest", 0, false));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, st, "actionTest", 0, false);
            EXPECT_ANY_THROW(auto threadVec1 = client.subscribe(hostName, port, onehandler, st, "actionTest", 0, false));
            EXPECT_EQ(threadVec.size(), threadCount);

            notify.wait();
            cout << "total size:" << msg_total << endl;
            client.unsubscribe(hostName, port, st, "actionTest");

            EXPECT_TRUE(conn.run("re = select * from res_STPCT order by datetimev;\
                                ex = select * from ex_STPCT order by datetimev;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

            EXPECT_EQ(msg_total, 1000);
            for (auto &t : threadVec)
            {
                EXPECT_TRUE(t->isComplete());
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_client_threadCount_withAllDataType)
    {
        string st = "outTables_" + getRandString(10);
        STPCT::createSharedTableAndReplay_withAllDataType(st);
        int msg_total = 0;

        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_STPCT", conn);

        auto onehandler = [&](Message msg)
        {
            msg_total+=1;
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
            if (msg.getOffset() == 999)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        int threadCount = rand() % 10 + 1;
        ThreadPooledClient client(listenport, threadCount);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, st, "actionTest", 0));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, st, "actionTest", 0);
            EXPECT_EQ(threadVec.size(), threadCount);

            notify.wait();
            cout << "total size:" << msg_total << endl;
            client.unsubscribe(hostName, port, st, "actionTest");

            EXPECT_TRUE(conn.run("re = select * from res_STPCT order by ind;\
                                ex = select * from ex_STPCT order by ind;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

            EXPECT_EQ(msg_total, 1000);
            for (auto &t : threadVec)
            {
                EXPECT_TRUE(t->isComplete());
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_client_threadCount_arrayVector)
    {
        string st = "arrayVectorTable_" + getRandString(10);
        STPCT::createSharedTableAndReplay_withArrayVector(st);
        int msg_total = 0;

        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_STPCT", conn);

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

        int threadCount = rand() % 10 + 1;
        ThreadPooledClient client(listenport, threadCount);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, st, "arrayVectorTableTest", 0));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, st, "arrayVectorTableTest", 0);
            EXPECT_EQ(threadVec.size(), threadCount);

            notify.wait();
            cout << "total size:" << msg_total << endl;
            client.unsubscribe(hostName, port, st, "arrayVectorTableTest");

            EXPECT_TRUE(conn.run("re = select * from res_STPCT order by ts;\
                                ex = select * from ex_STPCT order by ts;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

            EXPECT_EQ(msg_total, 1000);
            for (auto &t : threadVec)
            {
                EXPECT_TRUE(t->isComplete());
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribe_hugetable)
    {
        string st = "outTables_" + getRandString(10);
        STPCT::createSharedTableAndReplay(st, 1000000);
        int msg_total = 0;

        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_STPCT", conn);

        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            // bool succeeded = false; 
            // TableSP tmp = AnyVectorToTable(msg);
            // while(!succeeded){
            //     try
            //     {
            //         appender.append(tmp);
            //         succeeded = true;
            //     }
            //     catch(const std::exception& e)
            //     {
            //         Util::sleep(100);
            //     }
            // }
            msg_total+=1;
            if (msg.getOffset() == 999999)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;
        ThreadPooledClient client(listenport, 4);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, st, "actionTest", 0));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, st, "actionTest", 0);
            EXPECT_EQ(threadVec.size(), 4);

            notify.wait();
            cout << "total size:" << msg_total << endl;
            client.unsubscribe(hostName, port, st, "actionTest");

            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

            EXPECT_EQ(msg_total, 1000000);
            for (auto &t : threadVec)
            {
                EXPECT_TRUE(t->isComplete());
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribe_hugetable_msgAsTable)
    {
        string st = "outTables_" + getRandString(10);
        STPCT::createSharedTableAndReplay(st, 1000000);
        int msg_total = 0;

        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_STPCT", conn);

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
        ThreadPooledClient client(listenport, 4);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, st, "actionTest", 0, false, nullptr, true));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, st, "actionTest", 0, false, nullptr, true);
            EXPECT_EQ(threadVec.size(), 4);

            notify.wait();
            cout << "total size:" << msg_total << endl;
            client.unsubscribe(hostName, port, st, "actionTest");

            EXPECT_TRUE(conn.run("re = select * from res_STPCT order by datetimev;\
                                ex = select * from ex_STPCT order by datetimev;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

            EXPECT_EQ(msg_total, 1000000);
            for (auto &t : threadVec)
            {
                EXPECT_TRUE(t->isComplete());
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.insert(listenport);
        conn.run("try{dropStreamTable(`"+st+")}catch(ex){}");
    }
}
