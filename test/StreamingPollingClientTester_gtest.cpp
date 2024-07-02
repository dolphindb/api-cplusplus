#include "config.h"

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

    static void createSharedTableAndReplay(const string &st, int rows)
    {
        string script = "login(\"admin\",\"123456\")\n\
                st1 = streamTable(100:0, `datetimev`timestampv`sym`price1`price2,[DATETIME,TIMESTAMP,SYMBOL,DOUBLE,DOUBLE])\n\
                enableTableShareAndPersistence(table=st1, tableName=`"+st+")\n\
                go\n\
                setStreamTableFilterColumn("+st+", `sym)";
        conn.run(script);

        string replayScript = "n = " + to_string(rows) + ";table1_SPCT = table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);\
                share table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]) as res_SPCT;assert res_SPCT.rows() == 0;go;\
                tableInsert(table1_SPCT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));share table1_SPCT as ex_SPCT;\
                replay(inputTables=table1_SPCT, outputTables=`"+st+", dateColumn=`timestampv, timeColumn=`timestampv)";
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
            "table1_SPCT = table(1:0, colName, colType);"
            "row_num = 1000;"
            "share table(1:0, colName, colType) as res_SPCT;assert res_SPCT.rows() == 0;go;"
            "col1 = rand(2 ,row_num);col2 = rand(256 ,row_num);col3 = rand(-row_num..row_num ,row_num);col4 = rand(-row_num..row_num ,row_num);col5 = rand(-row_num..row_num ,row_num);col6 = rand(0..row_num ,row_num);col7 = rand(0..row_num ,row_num);"
            "col8 = rand(0..row_num ,row_num);col9 = rand(0..row_num ,row_num);col10 = rand(0..row_num ,row_num);col11 = rand(0..row_num ,row_num);col12 = rand(0..row_num ,row_num);col13 = rand(0..row_num ,row_num);col14= rand(0..row_num ,row_num);"
            "col15 = rand(round(row_num,2) ,row_num);col16 = rand(round(row_num,2) ,row_num);col17 =rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);col18 =rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);col19 =rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);col20 = take(ipaddr(\"192.168.1.13\"),row_num);"
            "col21 = take(uuid(\"5d212a78-cc48-e3b1-4235-b4d91473ee87\"),row_num);col22 = take(int128(\"e1671797c52e15f763380b45e841ec32\"),row_num);col23=rand((-1000..1000)/1000.0000$DECIMAL32(" +
            to_string(scale32) + "),row_num);col24=rand((-1000..1000)/1000.0000$DECIMAL64(" + to_string(scale64) + "),row_num);col25=rand((-1000..1000)/1000.0000$DECIMAL128(" + to_string(scale128) + "),row_num);go;"
            "for (i in 0..(row_num-1)){tableInsert(table1_SPCT,i,col1[i],col2[i],col3[i],col4[i],col5[i],col6[i],col7[i],col8[i],col9[i],col10[i],col11[i],col12[i],col13[i],col14[i],col15[i],col16[i],col17[i],col18[i],col19[i],col20[i],col21[i],col22[i],col23[i],col24[i],col25[i])};"
            "share table1_SPCT as ex_SPCT;go;"
            "replay(inputTables=table1_SPCT, outputTables=`"+st+", dateColumn=`ctimestamp, timeColumn=`ctimestamp);";
        conn.run(replayScript);
    }

    static void createSharedTableAndReplay_withArrayVector(const string &st)
    {
        string replayScript = 
            "colName = `ts`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cdatehour`cfloat`cdouble`cipaddr`cuuid`cint128`cdecimal32`cdecimal64`cdecimal128;"
            "colType = [TIMESTAMP, BOOL[], CHAR[], SHORT[], INT[],LONG[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], DATEHOUR[], FLOAT[], DOUBLE[], IPADDR[], UUID[], INT128[],DECIMAL32(6)[],DECIMAL64(16)[],DECIMAL128(26)[]];"
            "st1 = streamTable(1:0,colName, colType);"
            "share table(1:0, colName, colType) as res_SPCT;assert res_SPCT.rows() == 0;go;"
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
            "table1_SPCT=table(ts,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cdatehour,cfloat,cdouble,cipaddr,cuuid,cint128,cdecimal32,cdecimal64,cdecimal128);"
            "share table1_SPCT as ex_SPCT;go;"
            "replay(inputTables=table1_SPCT, outputTables=`"+st+", dateColumn=`ts, timeColumn=`ts);";
        conn.run(replayScript);
    }

    INSTANTIATE_TEST_SUITE_P(, StreamingPollingClientTester, testing::Values(-1, rand() % 1000 + 13000));
    TEST_P(StreamingPollingClientTester, test_subscribeUnsubscribe_normal)
    {
        string st = "outTables_" + getRandString(10);
        SPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, st, "actionTest", 0));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, st, "actionTest", 0);

            Message msg;
            Signal notify;
            Mutex mutex;
            AutoFitTableAppender appender("", "res_SPCT", conn);
            thread th1 = thread([&]{
                LockGuard<Mutex> lock(&mutex);
                while (true)
                {
                    queue->pop(msg);
                    if(msg.isNull()) {
                        EXPECT_EQ(msg.getOffset(), -1);
                        break;
                    }
                    else{
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
                        if(msg.getOffset() == 999)
                            notify.set();
                    }
                }
            });

            notify.wait();
            client.unsubscribe(hostName, port, st, "actionTest");
            th1.join();
            cout << "total size: " << msg_total << endl;
            EXPECT_TRUE(conn.run("re = select * from res_SPCT order by datetimev;\
                                ex = select * from ex_SPCT order by datetimev;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(queue->size(), 0);
            EXPECT_EQ(msg_total, 1000);
        }

        usedPorts.insert(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_subscribeUnsubscribe_notExistTable)
    {
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, "st_notExist", "actionTest", 0));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, "st_notExist", "actionTest");
            Util::sleep(1000);
        }
        client.unsubscribe(hostName, port, "st_notExist", "actionTest");
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_hostNull)
    {
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
        EXPECT_ANY_THROW(client.subscribe("", port, "st", "actionTest", 0, false));
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_portNull)
    {
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
        EXPECT_ANY_THROW(client.subscribe(hostName, NULL, "st", "actionTest", 0, false));
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_tableNameNull)
    {
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
        EXPECT_ANY_THROW(auto queue = client.subscribe(hostName, port, "", "actionTest", 0, false));
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_actionNameNull)
    {
        string st = "outTables_" + getRandString(10);
        SPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, st, "", 0));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, st, "", 0);
            Message msg;
            Signal notify;
            Mutex mutex;
            AutoFitTableAppender appender("", "res_SPCT", conn);
            thread th1 = thread([&]{
                LockGuard<Mutex> lock(&mutex);
                while (true)
                {
                    queue->pop(msg);
                    if(msg.isNull()) {
                        EXPECT_EQ(msg.getOffset(), -1);
                        break;
                    }
                    else{
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
                        if(msg.getOffset() == 999)
                            notify.set();
                    }
                }
            });

            notify.wait();
            client.unsubscribe(hostName, port, st, "");
            th1.join();
            cout << "total size: " << msg_total << endl;
            EXPECT_TRUE(conn.run("re = select * from res_SPCT order by datetimev;\
                                ex = select * from ex_SPCT order by datetimev;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(queue->size(), 0);
            EXPECT_EQ(msg_total, 1000);
        }

        usedPorts.insert(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_offsetNegative)
    {
        string st = "outTables_" + getRandString(10);
        SPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, st, "actionTest", -1, false));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, st, "actionTest", -1, false);
            Message msg;
            Signal notify;
            Mutex mutex;
            AutoFitTableAppender appender("", "res_SPCT", conn);
            thread th1 = thread([&]{
                LockGuard<Mutex> lock(&mutex);
                while (true)
                {
                    queue->pop(msg);
                    if(msg.isNull()) {
                        EXPECT_EQ(msg.getOffset(), -1);
                        break;
                    }
                    else{
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
                        if(msg.getOffset() == 999)
                            notify.set();
                    }
                }
            });
            Util::sleep(1000);
            client.unsubscribe(hostName, port, st, "actionTest");
            th1.join();
            cout << "total size: " << msg_total << endl;
            EXPECT_TRUE(conn.run("eqObj(res_SPCT.rows(), 0)")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(queue->size(), 0);
            EXPECT_EQ(msg_total, 0);
        }

        usedPorts.insert(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_filter)
    {
        string st = "outTables_" + getRandString(10);
        SPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        int target_rows = conn.run("(exec count(*) from ex_SPCT where sym = 'a')[0]")->getInt();
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        VectorSP filter = Util::createVector(DT_SYMBOL, 1, 1);
        filter->setString(0, "a");

        PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, st, "actionTest", 0, true, filter));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, st, "actionTest", 0, true, filter);
            Message msg;
            Signal notify;
            Mutex mutex;
            AutoFitTableAppender appender("", "res_SPCT", conn);
            thread th1 = thread([&]{
                LockGuard<Mutex> lock(&mutex);
                while (true)
                {
                    queue->pop(msg);
                    if(msg.isNull()) {
                        EXPECT_EQ(msg.getOffset(), -1);
                        break;
                    }
                    else{
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
                        if(msg.getOffset() == 999)
                            notify.set();
                    }
                }
            });

            while(msg_total != target_rows)
            {
                Util::sleep(500);
            }
            client.unsubscribe(hostName, port, st, "actionTest");
            th1.join();
            cout << "total size: " << msg_total << endl;
            EXPECT_TRUE(conn.run("re = exec sym from res_SPCT; all(re == `a)")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(queue->size(), 0);
        }

        usedPorts.insert(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_msgAsTable)
    {
        string st = "outTables_" + getRandString(10);
        SPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, st, "actionTest", 0, true, nullptr, true));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, st, "actionTest", 0, true, nullptr, true);
            Message msg;
            Signal notify;
            Mutex mutex;
            AutoFitTableAppender appender("", "res_SPCT", conn);
            thread th1 = thread([&]{
                LockGuard<Mutex> lock(&mutex);
                while (true)
                {
                    queue->pop(msg);
                    if(msg.isNull()) {
                        EXPECT_EQ(msg.getOffset(), -1);
                        break;
                    }
                    else{
                        EXPECT_EQ(msg->getForm(), DF_TABLE);
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
                        msg_total+=msg->rows();
                        int first_offset = msg.getOffset();
                        int last_offset = first_offset + msg->rows() -1;
                        if(last_offset == 999)
                            notify.set();
                    }
                }
            });

            notify.wait();
            client.unsubscribe(hostName, port, st, "actionTest");
            th1.join();
            cout << "total size: " << msg_total << endl;
            EXPECT_TRUE(conn.run("re = select * from res_SPCT order by datetimev;\
                                ex = select * from ex_SPCT order by datetimev;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(queue->size(), 0);
            EXPECT_TRUE(msg_total == 1000);
        }

        usedPorts.insert(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_allowExists)
    {
        GTEST_SKIP() << "server还没有修复allowExists的问题, jira: https://dolphindb1.atlassian.net/browse/D20-14283";
        int listenport = GetParam();
        PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
        string st = "outTables_" + getRandString(10);
        SPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0, msg_total2 = 0;
        cout << "current listenport is " << listenport << endl;

        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, st, "actionTest", 0, true, nullptr, false, true));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, st, "actionTest", 0, false, nullptr, false, true);
            auto queue2 = client.subscribe(hostName, port, st, "actionTest", 0, false, nullptr, false, true);
            Message msg;
            Message msg2;
            Signal notify;
            Mutex mutex;
            thread th1 = thread([&]
                                {
            while (true)
            {
                queue->pop(msg);
                queue2->pop(msg2);
                // cout<<msg->getString()<<endl;
                if(!msg.isNull()) { msg_total+=1;}
                else if(!msg2.isNull()) { msg_total2+=1;}
                else if(msg.isNull() && msg2.isNull()){break;}
                if (msg.getOffset() == 999 && msg2.getOffset() == 999)
                    notify.set();
            }});

            notify.wait();
            client.unsubscribe(hostName, port, st, "actionTest");
            th1.join();
            cout << "total size: " << msg_total << endl;
            Util::sleep(100);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(queue->size(), 0);
            EXPECT_EQ(queue2->size(), 0);
            EXPECT_EQ(msg_total, 1000);
            EXPECT_EQ(msg_total2, 1000);
        }
        client.exit();
        EXPECT_TRUE(client.isExit());
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_resub_false)
    {
        // SPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
        EXPECT_ANY_THROW(auto queue = client.subscribe(hostName, port, "st", "actionTest", 0, false));

        usedPorts.insert(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_resub_true)
    {
        // SPCT::createSharedTableAndReplay(1000);
        int listenport = GetParam();
        string st = "st_" + getRandString(10);
        conn.run("share streamTable(1:0, `sym`val, [SYMBOL, INT]) as "+st+";tableInsert("+st+", `sym1, 1)");
        cout << "current listenport is " << listenport << endl;

        PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, st, "resubTest", 0, true));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, st, "resubTest", 0, true);
            Util::sleep(2000);
            conn.run("subinfo = (exec subscriber from getStreamingStat().pubTables where tableName=`"+st+")[0].split(':');"
                     "subClient = subinfo[0];"
                     "subPort=int(subinfo[1]);go;"
                     "stopPublishTable(subClient, subPort, `"+st+", `resubTest)");
            Util::sleep(1000);
            conn.run("tableInsert("+st+", `sym2, 2)");
            Util::sleep(1000);
            client.unsubscribe(hostName, port, st, "resubTest");
            client.exit();
            EXPECT_TRUE(client.isExit());

            Message msg;
            vector<Message> msgs;
            while (true)
            {
                queue->pop(msg);
                if(msg.isNull()) {
                        EXPECT_EQ(msg.getOffset(), -1);
                        break;
                    }
                else{msgs.push_back(msg);}
            };
            EXPECT_EQ(msgs.size(), 2);
        }

        usedPorts.insert(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_unsubscribe_hostNull)
    {
        string st = "outTables_" + getRandString(10);
        SPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, st, "actionTest", 0));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, st, "actionTest", 0);
            Message msg;
            Signal notify;
            Mutex mutex;
            thread th1 = thread([&]
                                {
            while (true)
            {
                queue->pop(msg);
                // cout<<msg->getString()<<endl;
                if(msg.isNull()) {
                        EXPECT_EQ(msg.getOffset(), -1);
                        break;
                }else{
                    msg_total+=1;
                }
                if (msg.getOffset() == 999)
                    notify.set();
            } });

            notify.wait();
            EXPECT_ANY_THROW(client.unsubscribe("", port, st, "actionTest"));

            client.unsubscribe(hostName, port, st, "actionTest");
            th1.join();
            cout << "total size: " << msg_total << endl;
            Util::sleep(100);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(queue->size(), 0);
        }

        usedPorts.insert(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_unsubscribe_portNull)
    {
        string st = "outTables_" + getRandString(10);
        SPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, st, "actionTest", 0));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, st, "actionTest", 0);
            Message msg;
            Signal notify;
            Mutex mutex;
            thread th1 = thread([&]
                                {
            while (true)
            {
                queue->pop(msg);
                // cout<<msg->getString()<<endl;
                if(msg.isNull()) {
                        EXPECT_EQ(msg.getOffset(), -1);
                        break;
                }else{
                    msg_total+=1;
                }
                if (msg.getOffset() == 999)
                    notify.set();
            } });

            notify.wait();
            EXPECT_ANY_THROW(client.unsubscribe(hostName, NULL, st, "actionTest"));

            client.unsubscribe(hostName, port, st, "actionTest");
            th1.join();
            cout << "total size: " << msg_total << endl;
            Util::sleep(100);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(queue->size(), 0);
        }

        usedPorts.insert(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_unsubscribe_tableNameNull)
    {
        string st = "outTables_" + getRandString(10);
        SPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, st, "actionTest", 0));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, st, "actionTest", 0);
            Message msg;
            Signal notify;
            Mutex mutex;
            thread th1 = thread([&]
                                {
            while (true)
            {
                queue->pop(msg);
                // cout<<msg->getString()<<endl;
                if(msg.isNull()) {
                        EXPECT_EQ(msg.getOffset(), -1);
                        break;
                    }else{msg_total+=1;}
                if (msg.getOffset() == 999)
                    notify.set();
            } });

            notify.wait();
            EXPECT_ANY_THROW(client.unsubscribe(hostName, port, "", "actionTest"));

            client.unsubscribe(hostName, port, st, "actionTest");
            th1.join();
            cout << "total size: " << msg_total << endl;
            Util::sleep(100);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(queue->size(), 0);
        }

        usedPorts.insert(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_unsubscribe_actionNameNull)
    {
        string st = "outTables_" + getRandString(10);
        SPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, st, "actionTest", 0));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, st, "actionTest", 0);
            Message msg;
            Signal notify;
            Mutex mutex;
            thread th1 = thread([&]
                                {
            while (true)
            {
                queue->pop(msg);
                // cout<<msg->getString()<<endl;
                if(msg.isNull()) {
                        EXPECT_EQ(msg.getOffset(), -1);
                        break;
                    }else{msg_total+=1;}
                if (msg.getOffset() == 999)
                    notify.set();
            } });

            notify.wait();
            client.unsubscribe(hostName, port, st, "");
            TableSP stat = conn.run("getStreamingStat().pubTables");
            EXPECT_EQ(stat->getColumn(0)->getRow(0)->getString(), st);
            EXPECT_EQ(stat->getColumn(2)->getRow(0)->getInt(), 1000);
            EXPECT_EQ(stat->getColumn(3)->getRow(0)->getString(), "actionTest");
            client.unsubscribe(hostName, port, st, "actionTest");
            th1.join();
            cout << "total size: " << msg_total << endl;
            Util::sleep(100);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(queue->size(), 0);
        }

        usedPorts.insert(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_twice)
    {
        string st = "outTables_" + getRandString(10);
        SPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, st, "actionTest", 0, false));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, st, "actionTest", 0, false);
            Message msg;
            Signal notify;
            Mutex mutex;
            thread th1 = thread([&]
                                {
            while (true)
            {
                queue->pop(msg);
                // cout<<msg->getString()<<endl;
                if(msg.isNull()) {
                        EXPECT_EQ(msg.getOffset(), -1);
                        break;
                    }else{msg_total+=1;}
                if (msg.getOffset() == 999)
                    notify.set();
            } });
            EXPECT_ANY_THROW(auto queue1 = client.subscribe(hostName, port, st, "actionTest", 0, false));

            notify.wait();
            client.unsubscribe(hostName, port, st, "actionTest");
            th1.join();
            cout << "total size: " << msg_total << endl;
            Util::sleep(100);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(queue->size(), 0);
            EXPECT_EQ(msg_total, 1000);
        }

        usedPorts.insert(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_subscribeUnsubscribe_withAllDataType)
    {
        string st = "outTables_" + getRandString(10);
        SPCT::createSharedTableAndReplay_withAllDataType(st);
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, st, "actionTest", 0));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, st, "actionTest", 0);
            int index = 0;

            Message msg;
            Signal notify;
            Mutex mutex;
            AutoFitTableAppender appender("", "res_SPCT", conn);
            thread th1 = thread([&]{
                LockGuard<Mutex> lock(&mutex);
                while (true)
                {
                    queue->pop(msg);
                    if(msg.isNull()) {
                        EXPECT_EQ(msg.getOffset(), -1);
                        break;
                    }
                    else{
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
                        if(msg.getOffset() == 999)
                            notify.set();
                    }
                }
            });
            notify.wait();
            client.unsubscribe(hostName, port, st, "actionTest");
            th1.join();
            cout << "total size: " << msg_total << endl;
            EXPECT_TRUE(conn.run("re = select * from res_SPCT order by ind;\
                                ex = select * from ex_SPCT order by ind;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            

            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(queue->size(), 0);
            EXPECT_EQ(msg_total, 1000);
        }

        usedPorts.insert(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_subscribeUnsubscribe_arrayVector)
    {
        string st = "arrayVectorTable_"+getRandString(10);
        SPCT::createSharedTableAndReplay_withArrayVector(st);
        int msg_total = 0;
        int index = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, st, "arrayVectorTableTest", 0));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, st, "arrayVectorTableTest", 0);

            Util::sleep(1000);
            Message msg;
            Signal notify;
            Mutex mutex;
            AutoFitTableAppender appender("", "res_SPCT", conn);
            thread th1 = thread([&]{
                LockGuard<Mutex> lock(&mutex);
                while (true)
                {
                    queue->pop(msg);
                    if(msg.isNull()) {
                        EXPECT_EQ(msg.getOffset(), -1);
                        break;
                    }
                    else{
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
                        if(msg.getOffset() == 999)
                            notify.set();
                    }
                }
            });
            notify.wait();

            client.unsubscribe(hostName, port, st, "arrayVectorTableTest");
            th1.join();
            cout << "total size: " << msg_total << endl;
            EXPECT_TRUE(conn.run("re = select * from res_SPCT order by ts;\
                                ex = select * from ex_SPCT order by ts;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(msg_total, 1000);
        }

        usedPorts.insert(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_subscribeUnsubscribe_hugetable)
    {
        string st = "outTables_" + getRandString(10);
        SPCT::createSharedTableAndReplay(st, 1000000);
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, st, "actionTest", 0));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, st, "actionTest", 0);

            Message msg;
            Signal notify;
            Mutex mutex;
            // AutoFitTableAppender appender("", "res_SPCT", conn);
            thread th1 = thread([&]{
                LockGuard<Mutex> lock(&mutex);
                while (true)
                {
                    queue->pop(msg);
                    if(msg.isNull()) {
                        EXPECT_EQ(msg.getOffset(), -1);
                        break;
                    }
                    else{
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
                        if(msg.getOffset() == 999)
                            notify.set();
                    }
                }
            });
            while (msg_total != 1000000)
            {
                Util::sleep(500);
            }
            client.unsubscribe(hostName, port, st, "actionTest");
            th1.join();
            cout << "total size: " << msg_total << endl;
            // EXPECT_TRUE(conn.run("re = select * from res_SPCT order by datetimev;\
            //                     ex = select * from ex_SPCT order by datetimev;\
            //                     all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(queue->size(), 0);
            EXPECT_EQ(msg_total, 1000000) << "Streaming msgs may not be recieved absolutely yet";
        }
        usedPorts.insert(listenport);
    }

    TEST_P(StreamingPollingClientTester, test_subscribeUnsubscribe_hugetable_msgAsTable)
    {
        string st = "outTables_" + getRandString(10);
        SPCT::createSharedTableAndReplay(st, 10000000);
        int msg_total = 0;
        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, st, "actionTest", 0, false, nullptr, true));
        }
        else
        {
            auto queue = client.subscribe(hostName, port, st, "actionTest", 0, false, nullptr, true);

            Message msg;
            Signal notify;
            Mutex mutex;
            AutoFitTableAppender appender("", "res_SPCT", conn);
            thread th1 = thread([&]{
                LockGuard<Mutex> lock(&mutex);
                while (true)
                {
                    queue->pop(msg);
                    if(msg.isNull()) {
                        EXPECT_EQ(msg.getOffset(), -1);
                        break;
                    }
                    else{
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
                    }
                    int first_offset = msg.getOffset();
                    int last_offset = first_offset + msg->rows() -1;
                    if(last_offset == 9999999)
                        notify.set();
                }
            });

            notify.wait();
            client.unsubscribe(hostName, port, st, "actionTest");
            th1.join();
            cout << "total size: " << msg_total << endl;
            EXPECT_TRUE(conn.run("re = select * from res_SPCT order by datetimev;\
                                ex = select * from ex_SPCT order by datetimev;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            EXPECT_EQ(queue->size(), 0);
            EXPECT_TRUE(msg_total > 0) << "Streaming msgs may not be recieved absolutely yet";
        }
        usedPorts.insert(listenport);
    }


    class StreamingPollingClientTester_realtime : public testing::Test, public ::testing::WithParamInterface<tuple<int, pair<DATA_TYPE, string>>>
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
        static vector<pair<DATA_TYPE, string>> getAVData()
        {
            return {
                {DT_BOOL_ARRAY, "take([true false], row_num)"},{DT_BOOL_ARRAY, "take([[00i]], row_num)"},
                {DT_CHAR_ARRAY, "take([10c 23c], row_num)"},{DT_CHAR_ARRAY, "take([[00i]], row_num)"},
                {DT_SHORT_ARRAY, "take([10s 23s], row_num)"},{DT_SHORT_ARRAY, "take([[00i]], row_num)"},
                {DT_INT_ARRAY, "take([10 23], row_num)"},{DT_INT_ARRAY, "take([[00i]], row_num)"},
                {DT_LONG_ARRAY, "take([10l 23l], row_num)"},{DT_LONG_ARRAY, "take([[00i]], row_num)"},
                {DT_DATE_ARRAY, "take([2021.01.01 2021.01.02], row_num)"},{DT_DATE_ARRAY, "take([[00i]], row_num)"},
                {DT_MONTH_ARRAY, "take([2021.01M 2021.02M], row_num)"},{DT_MONTH_ARRAY, "take([[00i]], row_num)"},
                {DT_TIME_ARRAY, "take([10:10:10 11:11:11], row_num)"},{DT_TIME_ARRAY, "take([[00i]], row_num)"},
                {DT_MINUTE_ARRAY, "take([10:10m 11:11m], row_num)"},{DT_MINUTE_ARRAY, "take([[00i]], row_num)"},
                {DT_SECOND_ARRAY, "take([10:10:10 11:11:11], row_num)"},{DT_SECOND_ARRAY, "take([[00i]], row_num)"},
                {DT_DATETIME_ARRAY, "take([2021.01.01T10:10:10 2021.01.02T11:11:11], row_num)"},{DT_DATETIME_ARRAY, "take([[00i]], row_num)"},
                {DT_TIMESTAMP_ARRAY, "take([2021.01.01T10:10:10.000 2021.01.02T11:11:11.000], row_num)"},{DT_TIMESTAMP_ARRAY, "take([[00i]], row_num)"},
                {DT_NANOTIME_ARRAY, "take([10:10:10.000000000 11:11:11.000000000], row_num)"},{DT_NANOTIME_ARRAY, "take([[00i]], row_num)"},
                {DT_NANOTIMESTAMP_ARRAY, "take([2021.01.01T10:10:10.000000000 2021.01.02T11:11:11.000000000], row_num)"},{DT_NANOTIMESTAMP_ARRAY, "take([[00i]], row_num)"},
                {DT_DATEHOUR_ARRAY, "take([datehour(1000 2000)], row_num)"},{DT_DATEHOUR_ARRAY, "take([[00i]], row_num)"},
                {DT_FLOAT_ARRAY, "take([10.00f 23.00f], row_num)"},{DT_FLOAT_ARRAY, "take([[00i]], row_num)"},
                {DT_DOUBLE_ARRAY, "take([10.314 23.445], row_num)"},{DT_DOUBLE_ARRAY, "take([[00i]], row_num)"},
                {DT_IP_ARRAY, "take([rand(ipaddr(), 2)], row_num)"},{DT_IP_ARRAY, "take([[ipaddr()]], row_num)"},
                {DT_UUID_ARRAY, "take([rand(uuid(), 2)], row_num)"},{DT_UUID_ARRAY, "take([[uuid()]], row_num)"},
                {DT_INT128_ARRAY, "take([rand(int128(), 2)], row_num)", }, {DT_INT128_ARRAY, "take([[int128()]], row_num)"},
                {DT_DECIMAL32_ARRAY, "take([decimal32(rand('-1.123''''2.23468965412', 2), 8)], row_num)"}, {DT_DECIMAL32_ARRAY, "take([[00i]], row_num)"},
                {DT_DECIMAL64_ARRAY, "take([decimal64(rand('-1.123''''2.123', 2), 15)], row_num)"}, {DT_DECIMAL64_ARRAY, "take([[00i]], row_num)"},
                {DT_DECIMAL128_ARRAY, "take([decimal128(rand('-1.123''''2.123', 2), 25)], row_num)"}, {DT_DECIMAL128_ARRAY, "take([[00i]], row_num)"},
            };
        };
        static vector<pair<DATA_TYPE, string>> getData()
        {
            return {
                {DT_BOOL, "rand(true false, row_num)"}, {DT_BOOL, "take(bool(), row_num)"},
                {DT_CHAR, "rand(127c, row_num)"}, {DT_CHAR, "take(char(), row_num)"},
                {DT_SHORT, "rand(32767h, row_num)"}, {DT_SHORT, "take(short(), row_num)"},
                {DT_INT, "rand(2147483647, row_num)"}, {DT_INT, "take(int(), row_num)"},
                {DT_LONG, "rand(1000l, row_num)"}, {DT_LONG, "take(long(), row_num)"},
                {DT_DATE, "rand(2019.01.01, row_num)"}, {DT_DATE, "take(date(), row_num)"},
                {DT_MONTH, "rand(2019.01M, row_num)"}, {DT_MONTH, "take(month(), row_num)"},
                {DT_TIME, "rand(12:00:00.123, row_num)"}, {DT_TIME, "take(time(), row_num)"},
                {DT_MINUTE, "rand(12:00m, row_num)"}, {DT_MINUTE, "take(minute(), row_num)"},
                {DT_SECOND, "rand(12:00:00, row_num)"}, {DT_SECOND, "take(second(), row_num)"},
                {DT_DATETIME, "rand(2019.01.01 12:00:00, row_num)"}, {DT_DATETIME, "take(datetime(), row_num)"},
                {DT_DATEHOUR, "rand(datehour(1000), row_num)"}, {DT_DATETIME, "take(datehour(), row_num)"},
                {DT_TIMESTAMP, "rand(2019.01.01 12:00:00.123, row_num)"}, {DT_TIMESTAMP, "take(timestamp(), row_num)"},
                {DT_NANOTIME, "rand(12:00:00.123456789, row_num)"}, {DT_NANOTIME, "take(nanotime(), row_num)"},
                {DT_NANOTIMESTAMP, "rand(2019.01.01 12:00:00.123456789, row_num)"}, {DT_NANOTIMESTAMP, "take(nanotimestamp(), row_num)"},
                {DT_DATEHOUR, "rand(datehour(100), row_num)"}, {DT_DATEHOUR, "take(datehour(), row_num)"},
                {DT_FLOAT, "rand(10.00f, row_num)"}, {DT_FLOAT, "take(float(), row_num)"},
                {DT_DOUBLE, "rand(10.00, row_num)"}, {DT_DOUBLE, "take(double(), row_num)"},
                {DT_IP, "rand(ipaddr(), row_num)"}, {DT_IP, "take(ipaddr(), row_num)"},
                {DT_UUID, "rand(uuid(), row_num)"}, {DT_UUID, "take(uuid(), row_num)"},
                {DT_INT128, "rand(int128(), row_num)"}, {DT_INT128, "take(int128(), row_num)"},
                {DT_DECIMAL32, "decimal32(rand('-1.123''''2.23468965412', row_num), 8)"}, {DT_DECIMAL32, "take(decimal32(NULL, 8), row_num)"},
                {DT_DECIMAL64, "decimal64(rand('-1.123''''2.123123123123123123', row_num), 15)"}, {DT_DECIMAL64, "take(decimal64(NULL, 15), row_num)"},
                {DT_DECIMAL128, "decimal128(rand('-1.123''''2.123', row_num), 25)"}, {DT_DECIMAL128, "take(decimal128(NULL, 25), row_num)"},
                {DT_STRING, "rand(`str1`str2, row_num)"}, {DT_STRING, "take(string(), row_num)"},
                {DT_SYMBOL, "rand(symbol(`sym1`sym2), row_num)"}, {DT_SYMBOL, "symbol(take(string(), row_num))"},
                {DT_BLOB, "rand(blob(`b1`b2`b3), row_num)"}, {DT_BLOB, "take(blob(''), row_num)"},
                };
        };
        void createST(DBConnection& conn, const string& name, const string& dtStr){
            string s = 
                "colName = `ts`testCol;"
                "colType = [TIMESTAMP, "+dtStr+"];"
                "share streamTable(1:0, colName, colType) as "+name+";"
                "share table(1:0, colName, colType) as res_SPCT;go;";
            conn.run(s);
        };
        void insertData(DBConnection& conn, const string& name, const string& colScript){
            string s = 
                "row_num = 1000;"
                "col0 = now()..(now()+row_num-1);"
                "col1 = "+colScript+";"
                "for (i in 0..(row_num-1)){insert into "+name+" values([col0[i]], [col1[i]]);};";
            conn.run(s);
        };

    };
    INSTANTIATE_TEST_SUITE_P(basicTypes, StreamingPollingClientTester_realtime, testing::Combine(
        testing::Values(0, rand() % 1000 + 13000),
        testing::ValuesIn(StreamingPollingClientTester_realtime::getData())));
    INSTANTIATE_TEST_SUITE_P(arrayVectorTypes, StreamingPollingClientTester_realtime, testing::Combine(
        testing::Values(0, rand() % 1000 + 13000),
        testing::ValuesIn(StreamingPollingClientTester_realtime::getAVData())));
    TEST_P(StreamingPollingClientTester_realtime, test_realtime)
    {
        DATA_TYPE ttp = std::get<1>(GetParam()).first;
        string typeString = Util::getDataTypeString(ttp);
        if (typeString.compare(0, 9, "DECIMAL32") == 0)
            typeString = typeString.substr(0, 9) + "(8)";
        else if (typeString.compare(0, 9, "DECIMAL64") == 0)
            typeString = typeString.substr(0, 9) + "(15)";
        else if (typeString.compare(0, 10, "DECIMAL128") == 0)
            typeString = typeString.substr(0, 10) + "(25)";
        
        if (ttp > ARRAY_TYPE_BASE && typeString.compare(0, 7, "DECIMAL") == 0){
            typeString = typeString + "[]";
        }
        cout << "test type: " << typeString << endl;

        const string st = "test_SD_" + getRandString(10);
        createST(conn, st, typeString);

        int msg_total = 0;
        Message msg;
        AutoFitTableAppender appender("", "res_SPCT", conn);

        int listenport = std::get<0>(GetParam());
        cout << "current listenport is " << listenport << endl;

        PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
        if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, st, "actionTest", 0));
        }
        else
        {
            string dataScript = std::get<1>(GetParam()).second;
            std::thread th = std::thread([&]() {
                insertData(conn, st, dataScript);
            });
            
            auto queue = client.subscribe(hostName, port, st, "actionTest", 0);
            th.join();
            thread th1 = thread([&]{
                while (true)
                {
                    queue->pop(msg);
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
                    if (msg.getOffset() == 999) break;
                }
            });
            th1.join();
            cout << "total size:" << msg_total << endl;
            client.unsubscribe(hostName, port, st, "actionTest");
            EXPECT_TRUE(conn.run("re = select * from res_SPCT order by ts;\
                                ex = select * from "+st+" order by ts;\
                                all(each(eqObj, re.values(), ex.values()))")->getBool());
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

            EXPECT_EQ(msg_total, 1000);
        }
        usedPorts.insert(listenport);
    }

}