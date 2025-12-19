#include <gtest/gtest.h>
#include "config.h"
#include "Streaming.h"

namespace STPCT
{
    class StreamingThreadPooledClientTester : public testing::Test, public testing::WithParamInterface<int>
    {
        public:
            static dolphindb::DBConnection conn;
            // Suite
            static void SetUpTestSuite()
            {
                bool ret = conn.connect(HOST, PORT, USER, PASSWD);
                if (!ret)
                {
                    std::cout << "Failed to connect to the server" << std::endl;
                }
                else
                {
                    std::cout << "connect to " + HOST + ":" + std::to_string(PORT) << std::endl;
                }
            }
            static void TearDownTestSuite()
            {
                conn.close();
            }

        protected:
            // Case
            virtual void SetUp()
            {

            }
            virtual void TearDown()
            {

            }
    };

    dolphindb::DBConnection StreamingThreadPooledClientTester::conn(false, false);
    static void createSharedTableAndReplay(const std::string &st, int rows)
    {
        std::string script = "\
                share streamTable(100:0, `datetimev`timestampv`sym`price1`price2,[DATETIME,TIMESTAMP,SYMBOL,DOUBLE,DOUBLE]) as "+st+";\
                go;\n\
                setStreamTableFilterColumn("+st+", `sym)";
        StreamingThreadPooledClientTester::conn.run(script);

        std::string replayScript = "n = " + std::to_string(rows) + ";table1_STPCT = table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);\
                share table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]) as "+st+"_res_STPCT;go;\
                tableInsert(table1_STPCT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));share table1_STPCT as "+st+"_ex_STPCT;\
                replay(inputTables=table1_STPCT, outputTables=`"+st+", dateColumn=`timestampv, timeColumn=`timestampv)";
        StreamingThreadPooledClientTester::conn.run(replayScript);
    }

    static void createSharedTableAndReplay_withAllDataType(const std::string &st)
    {
        int scale32 = rand() % 9;
        int scale64 = rand() % 18;
        int scale128 = rand() % 38;
        printf("#[PARAM] scale32: %d, scale64: %d, scale128: %d\n", scale32, scale64, scale128);
        std::string replayScript =
            "colName =  `ind`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring`cblob`cipaddr`cuuid`cint128`cdecimal32`cdecimal64`cdecimal128;"
            "colType = [INT, BOOL, CHAR, SHORT, INT,LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING, BLOB, IPADDR, UUID, INT128, DECIMAL32(" +
            std::to_string(scale32) + "), DECIMAL64(" + std::to_string(scale64) + "), DECIMAL128(" + std::to_string(scale128) + ")];"
            "share streamTable(100:0,colName, colType) as "+st+";"
            "setStreamTableFilterColumn("+st+", `csymbol);go;"
            "table1_STPCT = table(1:0, colName, colType);"
            "row_num = 1000;"
            "share table(1:0, colName, colType) as "+st+"_res_STPCT;go;"
            "col1 = rand(2 ,row_num);col2 = rand(256 ,row_num);col3 = rand(-row_num..row_num ,row_num);col4 = rand(-row_num..row_num ,row_num);col5 = rand(-row_num..row_num ,row_num);col6 = rand(0..row_num ,row_num);col7 = rand(0..row_num ,row_num);"
            "col8 = rand(0..row_num ,row_num);col9 = rand(0..row_num ,row_num);col10 = rand(0..row_num ,row_num);col11 = rand(0..row_num ,row_num);col12 = rand(0..row_num ,row_num);col13 = rand(0..row_num ,row_num);col14= rand(0..row_num ,row_num);"
            "col15 = rand(round(row_num,2) ,row_num);col16 = rand(round(row_num,2) ,row_num);col17 =rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);col18 =rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);col19 =rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);col20 = take(ipaddr(\"192.168.1.13\"),row_num);"
            "col21 = take(uuid(\"5d212a78-cc48-e3b1-4235-b4d91473ee87\"),row_num);col22 = take(int128(\"e1671797c52e15f763380b45e841ec32\"),row_num);col23=rand((-1000..1000)/1000.0000$DECIMAL32(" +
            std::to_string(scale32) + "),row_num);col24=rand((-1000..1000)/1000.0000$DECIMAL64(" + std::to_string(scale64) + "),row_num);col25=rand((-1000..1000)/1000.0000$DECIMAL128(" + std::to_string(scale128) + "),row_num);go;"
            "for (i in 0..(row_num-1)){tableInsert(table1_STPCT,i,col1[i],col2[i],col3[i],col4[i],col5[i],col6[i],col7[i],col8[i],col9[i],col10[i],col11[i],col12[i],col13[i],col14[i],col15[i],col16[i],col17[i],col18[i],col19[i],col20[i],col21[i],col22[i],col23[i],col24[i],col25[i])};"
            "share table1_STPCT as "+st+"_ex_STPCT;go;"
            "replay(inputTables=table1_STPCT, outputTables=`"+st+", dateColumn=`ctimestamp, timeColumn=`ctimestamp);";
        StreamingThreadPooledClientTester::conn.run(replayScript);
    }

    static void createSharedTableAndReplay_withArrayVector(const std::string &st)
    {
        std::string replayScript =
            "colName = `ts`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cdatehour`cfloat`cdouble`cipaddr`cuuid`cint128`cdecimal32`cdecimal64`cdecimal128;"
            "colType = [TIMESTAMP, BOOL[], CHAR[], SHORT[], INT[],LONG[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], DATEHOUR[], FLOAT[], DOUBLE[], IPADDR[], UUID[], INT128[],DECIMAL32(6)[],DECIMAL64(16)[],DECIMAL128(26)[]];"
            "share streamTable(1:0,colName, colType) as "+st+";"
            "share table(1:0, colName, colType) as "+st+"_res_STPCT;go;"
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
            "share table1_STPCT as "+st+"_ex_STPCT;go;"
            "replay(inputTables=table1_STPCT, outputTables=`"+st+", dateColumn=`ts, timeColumn=`ts);";
        StreamingThreadPooledClientTester::conn.run(replayScript);
    }

    INSTANTIATE_TEST_SUITE_P(, StreamingThreadPooledClientTester, testing::Values(0, rand() % 1000 + 13000));
    TEST_P(StreamingThreadPooledClientTester, test_client_threadCount_1)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_STPCT", conn);

        auto onehandler = [&](dolphindb::Message msg)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            bool succeeded = false;
            dolphindb::TableSP tmp = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    appender.append(tmp);
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    dolphindb::Util::sleep(100);
                }
            }
            msg_total+=1;
            if (msg.getOffset() == 999)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;
        dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, 1);
        auto threadVec = client.subscribe(HOST, PORT, onehandler, st, "actionTest", 0, true, nullptr, false, false, USER, PASSWD);
        ASSERT_EQ(threadVec.size(), 1);

        notify.wait();
        std::cout << "total size:" << msg_total << std::endl;
        client.unsubscribe(HOST, PORT, st, "actionTest");

        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_STPCT order by datetimev;\
                            ex = select * from "+case_+"_ex_STPCT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

        ASSERT_EQ(msg_total, 1000);
        for (auto &t : threadVec)
        {
            ASSERT_TRUE(t->isComplete());
            ASSERT_EQ(client.getQueueDepth(t), 0);
        }
    }

    TEST_P(StreamingThreadPooledClientTester, test_client_threadCount_2)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_STPCT", conn);

        auto onehandler = [&](dolphindb::Message msg)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            bool succeeded = false;
            dolphindb::TableSP tmp = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    appender.append(tmp);
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    dolphindb::Util::sleep(100);
                }
            }
            msg_total+=1;
            if (msg.getOffset() == 999)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;
        dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, 2);
        auto threadVec = client.subscribe(HOST, PORT, onehandler, st, "actionTest", 0, true, nullptr, false, false, USER, PASSWD);
        ASSERT_EQ(threadVec.size(), 2);

        notify.wait();
        std::cout << "total size:" << msg_total << std::endl;
        client.unsubscribe(HOST, PORT, st, "actionTest");

        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_STPCT order by datetimev;\
                            ex = select * from "+case_+"_ex_STPCT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

        ASSERT_EQ(msg_total, 1000);
        for (auto &t : threadVec)
        {
            ASSERT_TRUE(t->isComplete());
            ASSERT_EQ(client.getQueueDepth(t), 0);
        }
    }

    TEST_P(StreamingThreadPooledClientTester, test_client_threadCount_4)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_STPCT", conn);

        auto onehandler = [&](dolphindb::Message msg)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            bool succeeded = false;
            dolphindb::TableSP tmp = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    appender.append(tmp);
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    dolphindb::Util::sleep(100);
                }
            }
            msg_total+=1;
            if (msg.getOffset() == 999)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;
        dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, 4);
        auto threadVec = client.subscribe(HOST, PORT, onehandler, st, "actionTest", 0, true, nullptr, false, false, USER, PASSWD);
        ASSERT_EQ(threadVec.size(), 4);

        notify.wait();
        std::cout << "total size:" << msg_total << std::endl;
        client.unsubscribe(HOST, PORT, st, "actionTest");

        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_STPCT order by datetimev;\
                            ex = select * from "+case_+"_ex_STPCT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

        ASSERT_EQ(msg_total, 1000);
        for (auto &t : threadVec)
        {
            ASSERT_TRUE(t->isComplete());
            ASSERT_EQ(client.getQueueDepth(t), 0);
        }
    }

    TEST_P(StreamingThreadPooledClientTester, test_client_threadCount_8)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_STPCT", conn);

        auto onehandler = [&](dolphindb::Message msg)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            bool succeeded = false;
            dolphindb::TableSP tmp = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    appender.append(tmp);
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    dolphindb::Util::sleep(100);
                }
            }
            msg_total+=1;
            if (msg.getOffset() == 999)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;
        dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, 8);
        auto threadVec = client.subscribe(HOST, PORT, onehandler, st, "actionTest", 0, true, nullptr, false, false, USER, PASSWD);
        ASSERT_EQ(threadVec.size(), 8);

        notify.wait();
        std::cout << "total size:" << msg_total << std::endl;
        client.unsubscribe(HOST, PORT, st, "actionTest");

        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_STPCT order by datetimev;\
                            ex = select * from "+case_+"_ex_STPCT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

        ASSERT_EQ(msg_total, 1000);
        for (auto &t : threadVec)
        {
            ASSERT_TRUE(t->isComplete());
            ASSERT_EQ(client.getQueueDepth(t), 0);
        }
    }

    TEST_P(StreamingThreadPooledClientTester, test_client_threadCount_16)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_STPCT", conn);

        auto onehandler = [&](dolphindb::Message msg)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            bool succeeded = false;
            dolphindb::TableSP tmp = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    appender.append(tmp);
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    dolphindb::Util::sleep(100);
                }
            }
            msg_total+=1;
            if (msg.getOffset() == 999)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;
        dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, 16);
        auto threadVec = client.subscribe(HOST, PORT, onehandler, st, "actionTest", 0, true, nullptr, false, false, USER, PASSWD);
        ASSERT_EQ(threadVec.size(), 16);

        notify.wait();
        std::cout << "total size:" << msg_total << std::endl;
        client.unsubscribe(HOST, PORT, st, "actionTest");

        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_STPCT order by datetimev;\
                            ex = select * from "+case_+"_ex_STPCT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

        ASSERT_EQ(msg_total, 1000);
        for (auto &t : threadVec)
        {
            ASSERT_TRUE(t->isComplete());
            ASSERT_EQ(client.getQueueDepth(t), 0);
        }
    }

    TEST_P(StreamingThreadPooledClientTester, test_client_threadCount_32)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_STPCT", conn);

        auto onehandler = [&](dolphindb::Message msg)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            bool succeeded = false;
            dolphindb::TableSP tmp = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    appender.append(tmp);
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    dolphindb::Util::sleep(100);
                }
            }
            msg_total+=1;
            if (msg.getOffset() == 999)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;
        dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, 32);
        auto threadVec = client.subscribe(HOST, PORT, onehandler, st, "actionTest", 0, true, nullptr, false, false, USER, PASSWD);
        ASSERT_EQ(threadVec.size(), 32);

        notify.wait();
        std::cout << "total size:" << msg_total << std::endl;
        client.unsubscribe(HOST, PORT, st, "actionTest");

        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_STPCT order by datetimev;\
                            ex = select * from "+case_+"_ex_STPCT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

        ASSERT_EQ(msg_total, 1000);
        for (auto &t : threadVec)
        {
            ASSERT_TRUE(t->isComplete());
            ASSERT_EQ(client.getQueueDepth(t), 0);
        }
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribeUnsubscribe_notExistTable)
    {
        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, 2);
        auto queue = client.subscribe(HOST, PORT, [](dolphindb::Message msg){}, "st_notExist", "actionTest", 0, true, nullptr, false, false, USER, PASSWD);
        dolphindb::Util::sleep(1000);
        client.unsubscribe(HOST, PORT, "st_notExist", "actionTest");
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribe_onehandler_hostNull)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STPCT::createSharedTableAndReplay(st, 1000);

        auto onehandler = [&](dolphindb::Message msg){};

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, 2);
        ASSERT_ANY_THROW(client.subscribe("", PORT, onehandler, st, "actionTest", 0, true, nullptr, false, false, USER, PASSWD));
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribe_onehandler_portNull)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STPCT::createSharedTableAndReplay(st, 1000);
        auto onehandler = [&](dolphindb::Message msg){};

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, 2);
        ASSERT_ANY_THROW(client.subscribe(HOST, NULL, onehandler, st, "actionTest", 0, true, nullptr, false, false, USER, PASSWD));
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribe_onehandler_actionNameNull)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        auto onehandler = [&](dolphindb::Message msg)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            msg_total += 1;
            if (msg.getOffset() == 999)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, 2);
        auto threadVec = client.subscribe(HOST, PORT, onehandler, st, "", 0, true, nullptr, false, false, USER, PASSWD);
        notify.wait();
        dolphindb::TableSP stat = conn.run("select * from getStreamingStat().pubTables where tableName=`"+case_);

        ASSERT_EQ(stat->getColumn(0)->getRow(0)->getString(), st);
        ASSERT_EQ(stat->getColumn(2)->getRow(0)->getInt(), 1000);
        ASSERT_EQ(stat->getColumn(3)->getRow(0)->getString(), "");

        std::cout << "total size:" << msg_total << std::endl;
        client.unsubscribe(HOST, PORT, st, "");

        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

        ASSERT_EQ(msg_total, 1000);
        for (auto &t : threadVec)
        {
            ASSERT_TRUE(t->isComplete());
            ASSERT_EQ(client.getQueueDepth(t), 0);
        }
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribe_onehandler_tableNameNull)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        auto onehandler = [&](dolphindb::Message msg)
        {
            msg_total += 1;
            std::cout << msg->getString() << std::endl;
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, 2);
        ASSERT_ANY_THROW(auto threadVec = client.subscribe(HOST, PORT, onehandler, "", "actionTest", 0, false, nullptr, false, false, USER, PASSWD));
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribe_onehandler_userNameNull)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;
        dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, 2);
        bool enableClientAuth = conn.run("bool(getConfig('enableClientAuth'))")->getBool();
        if (enableClientAuth){
            ASSERT_ANY_THROW(auto threadVec = client.subscribe(HOST, PORT, [=](dolphindb::Message msg){}, st, DEFAULT_ACTION_NAME, -1, false));
        }else{
            auto threadVec = client.subscribe(HOST, PORT, [=](dolphindb::Message msg){}, st, DEFAULT_ACTION_NAME, 0, false);
            dolphindb::Util::sleep(1000);
            ASSERT_FALSE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            client.unsubscribe(HOST, PORT, st, DEFAULT_ACTION_NAME);
            dolphindb::Util::sleep(1000);
            ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        }
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribe_onehandler_offsetNegative)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        auto onehandler = [&](dolphindb::Message msg)
        {
            msg_total += 1;
            // std::cout << msg->getString() << std::endl;
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, 2);
        auto threadVec = client.subscribe(HOST, PORT, onehandler, st, "actionTest", -1, true, nullptr, false, false, USER, PASSWD);
        std::cout << "total size:" << msg_total << std::endl;
        client.unsubscribe(HOST, PORT, st, "actionTest");

        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

        ASSERT_EQ(msg_total, 0);
        for (auto &t : threadVec)
        {
            ASSERT_TRUE(t->isComplete());
            ASSERT_EQ(client.getQueueDepth(t), 0);
        }
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribe_onehandler_filter)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        int target_rows = conn.run("(exec count(*) from "+st+" where sym='b')[0]")->getInt();

        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_STPCT", conn);

        auto onehandler = [&](dolphindb::Message msg)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            bool succeeded = false;
            dolphindb::TableSP tmp = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    appender.append(tmp);
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    dolphindb::Util::sleep(100);
                }
            }
            msg_total+=1;
            if (msg_total == target_rows)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, 2);
        dolphindb::VectorSP filter = dolphindb::Util::createVector(dolphindb::DT_SYMBOL, 1, 1);
        filter->setString(0, "b");
        auto threadVec = client.subscribe(HOST, PORT, onehandler, st, "actionTest", 0, true, filter, false, false, USER, PASSWD);
        std::cout << "total size:" << msg_total << std::endl;
        notify.wait();

        client.unsubscribe(HOST, PORT, st, "actionTest");

        ASSERT_TRUE(conn.run("re = exec sym from "+case_+"_res_STPCT; all(re == `b)")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

        ASSERT_EQ(msg_total > 0, true);
        for (auto &t : threadVec)
        {
            ASSERT_TRUE(t->isComplete());
            ASSERT_EQ(client.getQueueDepth(t), 0);
        }
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribe_onehandler_msgAsTable)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_STPCT", conn);

        auto onehandler = [&](dolphindb::Message msg)
        {
            ASSERT_EQ(msg->getForm(), dolphindb::DF_TABLE);
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            bool succeeded = false;
            while(!succeeded){
                try
                {
                    appender.append(msg);
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    dolphindb::Util::sleep(100);
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
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, 2);
        auto threadVec = client.subscribe(HOST, PORT, onehandler, st, "actionTest", 0, true, nullptr, true, false, USER, PASSWD);
        notify.wait();
        std::cout << "total size:" << msg_total << std::endl;
        client.unsubscribe(HOST, PORT, st, "actionTest");

        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_STPCT order by datetimev;\
                            ex = select * from "+case_+"_ex_STPCT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

        ASSERT_EQ(msg_total, 1000);
        for (auto &t : threadVec)
        {
            ASSERT_TRUE(t->isComplete());
            ASSERT_EQ(client.getQueueDepth(t), 0);
        }
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribe_onehandler_allowExists)
    {
        GTEST_SKIP() << "server还没有修复allowExists的问题, jira: https://dolphindb1.atlassian.net/browse/D20-14283";
        std::string case_=getCaseName();
        std::string st = case_;
        STPCT::createSharedTableAndReplay(st, 1000);
        int msg_total,msg_total2 = 0;

        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        auto onehandler = [&](dolphindb::Message msg)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            msg_total += 1;
            if (msg.getOffset() == 999)
            {
                notify.set();
            }
            // std::cout << msg->getString() << std::endl;
        };
        auto onehandler2 = [&](dolphindb::Message msg)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            msg_total2 += 1;
            if (msg_total2 == 1000)
            {
                notify.set();
            }
            // std::cout << msg->getString() << std::endl;
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, 2);
        dolphindb::ThreadPooledClient client2(listenport, 5);
        auto threadVec = client.subscribe(HOST, PORT, onehandler, st, "actionTest", 0, false, nullptr, false, true, USER, PASSWD);
        auto threadVec2 = client2.subscribe(HOST, PORT, onehandler, st, "actionTest", 0, false, nullptr, false, true, USER, PASSWD);
        notify.wait();
        std::cout << "total size:" << msg_total << std::endl;
        client.unsubscribe(HOST, PORT, st, "actionTest");
        client2.unsubscribe(HOST, PORT, st, "actionTest");

        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

        ASSERT_EQ(msg_total, 1000);
        ASSERT_EQ(msg_total2, 1000);
        for (auto &t : threadVec)
        {
            ASSERT_TRUE(t->isComplete());
            ASSERT_EQ(client.getQueueDepth(t), 0);
        }
        for (auto &t : threadVec2)
        {
            ASSERT_TRUE(t->isComplete());
            ASSERT_EQ(client.getQueueDepth(t), 0);
        }
    }

    TEST_P(StreamingThreadPooledClientTester, test_client_resub_false)
    {
        auto onehandler = [&](dolphindb::Message msg){};

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, 1);
        ASSERT_ANY_THROW(auto threadVec = client.subscribe(HOST, PORT, onehandler, "st", "actionTest", 0, false, nullptr, false, false, USER, PASSWD));
    }

    TEST_P(StreamingThreadPooledClientTester, test_client_resub_true)
    {
        std::vector<dolphindb::Message> msgs;
        auto onehandler = [&](dolphindb::Message m)
        {
            msgs.push_back(m);
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, 1);
        auto threadVec = client.subscribe(HOST, PORT, onehandler, "nonExistTable", "resubTest", 0, true, nullptr, false, false, USER, PASSWD);
        dolphindb::Util::sleep(1000);
        client.unsubscribe(HOST, PORT, "nonExistTable", "resubTest");
    }

    TEST_P(StreamingThreadPooledClientTester, test_client_threadCount_subscribe_twice)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_STPCT", conn);

        auto onehandler = [&](dolphindb::Message msg)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            bool succeeded = false;
            dolphindb::TableSP tmp = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    appender.append(tmp);
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    dolphindb::Util::sleep(100);
                }
            }
            msg_total+=1;
            if (msg.getOffset() == 999)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        int threadCount = rand() % 10 + 1;
        dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, threadCount);
        auto threadVec = client.subscribe(HOST, PORT, onehandler, st, "actionTest", 0, false, nullptr, false, false, USER, PASSWD);
        ASSERT_ANY_THROW(auto threadVec1 = client.subscribe(HOST, PORT, onehandler, st, "actionTest", 0, false, nullptr, false, false, USER, PASSWD));
        ASSERT_EQ(threadVec.size(), threadCount);

        notify.wait();
        std::cout << "total size:" << msg_total << std::endl;
        client.unsubscribe(HOST, PORT, st, "actionTest");

        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_STPCT order by datetimev;\
                            ex = select * from "+case_+"_ex_STPCT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

        ASSERT_EQ(msg_total, 1000);
        for (auto &t : threadVec)
        {
            ASSERT_TRUE(t->isComplete());
            ASSERT_EQ(client.getQueueDepth(t), 0);
        }
    }

    TEST_P(StreamingThreadPooledClientTester, test_client_threadCount_withAllDataType)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STPCT::createSharedTableAndReplay_withAllDataType(st);
        int msg_total = 0;

        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_STPCT", conn);

        auto onehandler = [&](dolphindb::Message msg)
        {
            msg_total+=1;
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            bool succeeded = false;
            dolphindb::TableSP tmp = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    appender.append(tmp);
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    dolphindb::Util::sleep(100);
                }
            }
            if (msg.getOffset() == 999)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        int threadCount = rand() % 10 + 1;
        dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, threadCount);
        auto threadVec = client.subscribe(HOST, PORT, onehandler, st, "actionTest", 0, true, nullptr, false, false, USER, PASSWD);
        ASSERT_EQ(threadVec.size(), threadCount);

        notify.wait();
        std::cout << "total size:" << msg_total << std::endl;
        client.unsubscribe(HOST, PORT, st, "actionTest");

        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_STPCT order by ind;\
                            ex = select * from "+case_+"_ex_STPCT order by ind;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

        ASSERT_EQ(msg_total, 1000);
        for (auto &t : threadVec)
        {
            ASSERT_TRUE(t->isComplete());
            ASSERT_EQ(client.getQueueDepth(t), 0);
        }
    }

    TEST_P(StreamingThreadPooledClientTester, test_client_threadCount_arrayVector)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STPCT::createSharedTableAndReplay_withArrayVector(st);
        int msg_total = 0;

        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_STPCT", conn);

        auto onehandler = [&](dolphindb::Message msg)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            bool succeeded = false;
            dolphindb::TableSP tmp = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    appender.append(tmp);
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    dolphindb::Util::sleep(100);
                }
            }
            msg_total+=1;
            if (msg.getOffset() == 999)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        int threadCount = rand() % 10 + 1;
        dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, threadCount);
        auto threadVec = client.subscribe(HOST, PORT, onehandler, st, "arrayVectorTableTest", 0, true, nullptr, false, false, USER, PASSWD);
        ASSERT_EQ(threadVec.size(), threadCount);

        notify.wait();
        std::cout << "total size:" << msg_total << std::endl;
        client.unsubscribe(HOST, PORT, st, "arrayVectorTableTest");

        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_STPCT order by ts;\
                            ex = select * from "+case_+"_ex_STPCT order by ts;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

        ASSERT_EQ(msg_total, 1000);
        for (auto &t : threadVec)
        {
            ASSERT_TRUE(t->isComplete());
            ASSERT_EQ(client.getQueueDepth(t), 0);
        }
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribe_hugetable)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STPCT::createSharedTableAndReplay(st, 1000000);
        int msg_total = 0;

        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_STPCT", conn);

        auto onehandler = [&](dolphindb::Message msg)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            msg_total+=1;
            if (msg.getOffset() == 999999)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;
        dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport);
        auto threadVec = client.subscribe(HOST, PORT, onehandler, st, "actionTest", 0, true, nullptr, false, false, USER, PASSWD);
        ASSERT_EQ(threadVec.size(), 3);

        notify.wait();
        std::cout << "total size:" << msg_total << std::endl;
        client.unsubscribe(HOST, PORT, st, "actionTest");

        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

        ASSERT_EQ(msg_total, 1000000);
        for (auto &t : threadVec)
        {
            ASSERT_TRUE(t->isComplete());
            ASSERT_EQ(client.getQueueDepth(t), 0);
        }
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribe_hugetable_msgAsTable)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STPCT::createSharedTableAndReplay(st, 1000000);
        int msg_total = 0;

        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_STPCT", conn);

        auto onehandler = [&](dolphindb::Message msg)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            bool succeeded = false;
            while(!succeeded){
                try
                {
                    appender.append(msg);
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    dolphindb::Util::sleep(100);
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
        std::cout << "current listenport is " << listenport << std::endl;
        dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport);
        auto threadVec = client.subscribe(HOST, PORT, onehandler, st, "actionTest", 0, false, nullptr, true, false, USER, PASSWD);
        ASSERT_EQ(threadVec.size(), 3);

        notify.wait();
        std::cout << "total size:" << msg_total << std::endl;
        client.unsubscribe(HOST, PORT, st, "actionTest");

        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_STPCT order by datetimev;\
                            ex = select * from "+case_+"_ex_STPCT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

        ASSERT_EQ(msg_total, 1000000);
        for (auto &t : threadVec)
        {
            ASSERT_TRUE(t->isComplete());
            ASSERT_EQ(client.getQueueDepth(t), 0);
        }
    }

    class StreamingThreadPooledClientTester_realtime : public testing::Test, public ::testing::WithParamInterface<std::tuple<int, std::pair<dolphindb::DATA_TYPE, std::string>>>
    {
    public:
        static dolphindb::DBConnection conn;
        // Suite
        static void SetUpTestSuite()
        {
            bool ret = conn.connect(HOST, PORT, USER, PASSWD);
            if (!ret)
            {
                std::cout << "Failed to connect to the server" << std::endl;
            }
            else
            {
                std::cout << "connect to " + HOST + ":" + std::to_string(PORT) << std::endl;
            }
        }
        static void TearDownTestSuite()
        {
            conn.close();
        }

        // Case
        virtual void SetUp()
        {

        }
        virtual void TearDown()
        {

        }
        static std::vector<std::pair<dolphindb::DATA_TYPE, std::string>> getAVData()
        {
            return {
                {dolphindb::DT_BOOL_ARRAY, "take([true false], row_num)"},{dolphindb::DT_BOOL_ARRAY, "take([[00i]], row_num)"},
                {dolphindb::DT_CHAR_ARRAY, "take([10c 23c], row_num)"},{dolphindb::DT_CHAR_ARRAY, "take([[00i]], row_num)"},
                {dolphindb::DT_SHORT_ARRAY, "take([10s 23s], row_num)"},{dolphindb::DT_SHORT_ARRAY, "take([[00i]], row_num)"},
                {dolphindb::DT_INT_ARRAY, "take([10 23], row_num)"},{dolphindb::DT_INT_ARRAY, "take([[00i]], row_num)"},
                {dolphindb::DT_LONG_ARRAY, "take([10l 23l], row_num)"},{dolphindb::DT_LONG_ARRAY, "take([[00i]], row_num)"},
                {dolphindb::DT_DATE_ARRAY, "take([2021.01.01 2021.01.02], row_num)"},{dolphindb::DT_DATE_ARRAY, "take([[00i]], row_num)"},
                {dolphindb::DT_MONTH_ARRAY, "take([2021.01M 2021.02M], row_num)"},{dolphindb::DT_MONTH_ARRAY, "take([[00i]], row_num)"},
                {dolphindb::DT_TIME_ARRAY, "take([10:10:10 11:11:11], row_num)"},{dolphindb::DT_TIME_ARRAY, "take([[00i]], row_num)"},
                {dolphindb::DT_MINUTE_ARRAY, "take([10:10m 11:11m], row_num)"},{dolphindb::DT_MINUTE_ARRAY, "take([[00i]], row_num)"},
                {dolphindb::DT_SECOND_ARRAY, "take([10:10:10 11:11:11], row_num)"},{dolphindb::DT_SECOND_ARRAY, "take([[00i]], row_num)"},
                {dolphindb::DT_DATETIME_ARRAY, "take([2021.01.01T10:10:10 2021.01.02T11:11:11], row_num)"},{dolphindb::DT_DATETIME_ARRAY, "take([[00i]], row_num)"},
                {dolphindb::DT_TIMESTAMP_ARRAY, "take([2021.01.01T10:10:10.000 2021.01.02T11:11:11.000], row_num)"},{dolphindb::DT_TIMESTAMP_ARRAY, "take([[00i]], row_num)"},
                {dolphindb::DT_NANOTIME_ARRAY, "take([10:10:10.000000000 11:11:11.000000000], row_num)"},{dolphindb::DT_NANOTIME_ARRAY, "take([[00i]], row_num)"},
                {dolphindb::DT_NANOTIMESTAMP_ARRAY, "take([2021.01.01T10:10:10.000000000 2021.01.02T11:11:11.000000000], row_num)"},{dolphindb::DT_NANOTIMESTAMP_ARRAY, "take([[00i]], row_num)"},
                {dolphindb::DT_DATEHOUR_ARRAY, "take([datehour(1000 2000)], row_num)"},{dolphindb::DT_DATEHOUR_ARRAY, "take([[00i]], row_num)"},
                {dolphindb::DT_FLOAT_ARRAY, "take([10.00f 23.00f], row_num)"},{dolphindb::DT_FLOAT_ARRAY, "take([[00i]], row_num)"},
                {dolphindb::DT_DOUBLE_ARRAY, "take([10.314 23.445], row_num)"},{dolphindb::DT_DOUBLE_ARRAY, "take([[00i]], row_num)"},
                {dolphindb::DT_IP_ARRAY, "take([rand(ipaddr(), 2)], row_num)"},{dolphindb::DT_IP_ARRAY, "take([[ipaddr()]], row_num)"},
                {dolphindb::DT_UUID_ARRAY, "take([rand(uuid(), 2)], row_num)"},{dolphindb::DT_UUID_ARRAY, "take([[uuid()]], row_num)"},
                {dolphindb::DT_INT128_ARRAY, "take([rand(int128(), 2)], row_num)", }, {dolphindb::DT_INT128_ARRAY, "take([[int128()]], row_num)"},
                {dolphindb::DT_DECIMAL32_ARRAY, "take([decimal32(rand('-1.123''''2.23468965412', 2), 8)], row_num)"}, {dolphindb::DT_DECIMAL32_ARRAY, "take([[00i]], row_num)"},
                {dolphindb::DT_DECIMAL64_ARRAY, "take([decimal64(rand('-1.123''''2.123', 2), 15)], row_num)"}, {dolphindb::DT_DECIMAL64_ARRAY, "take([[00i]], row_num)"},
                {dolphindb::DT_DECIMAL128_ARRAY, "take([decimal128(rand('-1.123''''2.123', 2), 25)], row_num)"}, {dolphindb::DT_DECIMAL128_ARRAY, "take([[00i]], row_num)"},
            };
        };
        static std::vector<std::pair<dolphindb::DATA_TYPE, std::string>> getData()
        {
            return {
                {dolphindb::DT_BOOL, "rand(true false, row_num)"}, {dolphindb::DT_BOOL, "take(bool(), row_num)"},
                {dolphindb::DT_CHAR, "rand(127c, row_num)"}, {dolphindb::DT_CHAR, "take(char(), row_num)"},
                {dolphindb::DT_SHORT, "rand(32767h, row_num)"}, {dolphindb::DT_SHORT, "take(short(), row_num)"},
                {dolphindb::DT_INT, "rand(2147483647, row_num)"}, {dolphindb::DT_INT, "take(int(), row_num)"},
                {dolphindb::DT_LONG, "rand(1000l, row_num)"}, {dolphindb::DT_LONG, "take(long(), row_num)"},
                {dolphindb::DT_DATE, "rand(2019.01.01, row_num)"}, {dolphindb::DT_DATE, "take(date(), row_num)"},
                {dolphindb::DT_MONTH, "rand(2019.01M, row_num)"}, {dolphindb::DT_MONTH, "take(month(), row_num)"},
                {dolphindb::DT_TIME, "rand(12:00:00.123, row_num)"}, {dolphindb::DT_TIME, "take(time(), row_num)"},
                {dolphindb::DT_MINUTE, "rand(12:00m, row_num)"}, {dolphindb::DT_MINUTE, "take(minute(), row_num)"},
                {dolphindb::DT_SECOND, "rand(12:00:00, row_num)"}, {dolphindb::DT_SECOND, "take(second(), row_num)"},
                {dolphindb::DT_DATETIME, "rand(2019.01.01 12:00:00, row_num)"}, {dolphindb::DT_DATETIME, "take(datetime(), row_num)"},
                {dolphindb::DT_DATEHOUR, "rand(datehour(1000), row_num)"}, {dolphindb::DT_DATETIME, "take(datehour(), row_num)"},
                {dolphindb::DT_TIMESTAMP, "rand(2019.01.01 12:00:00.123, row_num)"}, {dolphindb::DT_TIMESTAMP, "take(timestamp(), row_num)"},
                {dolphindb::DT_NANOTIME, "rand(12:00:00.123456789, row_num)"}, {dolphindb::DT_NANOTIME, "take(nanotime(), row_num)"},
                {dolphindb::DT_NANOTIMESTAMP, "rand(2019.01.01 12:00:00.123456789, row_num)"}, {dolphindb::DT_NANOTIMESTAMP, "take(nanotimestamp(), row_num)"},
                {dolphindb::DT_DATEHOUR, "rand(datehour(100), row_num)"}, {dolphindb::DT_DATEHOUR, "take(datehour(), row_num)"},
                {dolphindb::DT_FLOAT, "rand(10.00f, row_num)"}, {dolphindb::DT_FLOAT, "take(float(), row_num)"},
                {dolphindb::DT_DOUBLE, "rand(10.00, row_num)"}, {dolphindb::DT_DOUBLE, "take(double(), row_num)"},
                {dolphindb::DT_IP, "rand(ipaddr(), row_num)"}, {dolphindb::DT_IP, "take(ipaddr(), row_num)"},
                {dolphindb::DT_UUID, "rand(uuid(), row_num)"}, {dolphindb::DT_UUID, "take(uuid(), row_num)"},
                {dolphindb::DT_INT128, "rand(int128(), row_num)"}, {dolphindb::DT_INT128, "take(int128(), row_num)"},
                {dolphindb::DT_DECIMAL32, "decimal32(rand('-1.123''''2.23468965412', row_num), 8)"}, {dolphindb::DT_DECIMAL32, "take(decimal32(NULL, 8), row_num)"},
                {dolphindb::DT_DECIMAL64, "decimal64(rand('-1.123''''2.123123123123123123', row_num), 15)"}, {dolphindb::DT_DECIMAL64, "take(decimal64(NULL, 15), row_num)"},
                {dolphindb::DT_DECIMAL128, "decimal128(rand('-1.123''''2.123', row_num), 25)"}, {dolphindb::DT_DECIMAL128, "take(decimal128(NULL, 25), row_num)"},
                {dolphindb::DT_STRING, "rand(`str1`str2, row_num)"}, {dolphindb::DT_STRING, "take(string(), row_num)"},
                {dolphindb::DT_SYMBOL, "rand(symbol(`sym1`sym2), row_num)"}, {dolphindb::DT_SYMBOL, "symbol(take(string(), row_num))"},
                {dolphindb::DT_BLOB, "rand(blob(`b1`b2`b3), row_num)"}, {dolphindb::DT_BLOB, "take(blob(''), row_num)"},
                };
        };
        void createST(dolphindb::DBConnection& conn, const std::string& name, const std::string& dtStr){
            std::string s =
                "colName = `ts`testCol;"
                "colType = [TIMESTAMP, "+dtStr+"];"
                "share streamTable(1:0, colName, colType) as "+name+";"
                "share table(1:0, colName, colType) as "+name+"_res_STPCT;go;";
            conn.run(s);
        };
        void insertData(dolphindb::DBConnection& conn, const std::string& name, const std::string& colScript){
            std::string s =
                "row_num = 1000;"
                "col0 = now()..(now()+row_num-1);"
                "col1 = "+colScript+";"
                "for (i in 0..(row_num-1)){insert into "+name+" values([col0[i]], [col1[i]]);};";
            conn.run(s);
        };

    };
    dolphindb::DBConnection StreamingThreadPooledClientTester_realtime::conn(false, false);

    INSTANTIATE_TEST_SUITE_P(basicTypes, StreamingThreadPooledClientTester_realtime, testing::Combine(
        testing::Values(0, rand() % 1000 + 13000),
        testing::ValuesIn(StreamingThreadPooledClientTester_realtime::getData())));
    INSTANTIATE_TEST_SUITE_P(arrayVectorTypes, StreamingThreadPooledClientTester_realtime, testing::Combine(
        testing::Values(0, rand() % 1000 + 13000),
        testing::ValuesIn(StreamingThreadPooledClientTester_realtime::getAVData())));
    TEST_P(StreamingThreadPooledClientTester_realtime, test_realtime)
    {
        std::string case_=getCaseName();
        dolphindb::DATA_TYPE ttp = std::get<1>(GetParam()).first;
        std::string typeString = dolphindb::Util::getDataTypeString(ttp);
        if (typeString.compare(0, 9, "DECIMAL32") == 0)
            typeString = typeString.substr(0, 9) + "(8)";
        else if (typeString.compare(0, 9, "DECIMAL64") == 0)
            typeString = typeString.substr(0, 9) + "(15)";
        else if (typeString.compare(0, 10, "DECIMAL128") == 0)
            typeString = typeString.substr(0, 10) + "(25)";

        if (ttp > dolphindb::ARRAY_TYPE_BASE && typeString.compare(0, 7, "DECIMAL") == 0){
            typeString = typeString + "[]";
        }
        std::cout << "test type: " << typeString << std::endl;

        const std::string st = case_;
        createST(conn, st, typeString);

        int msg_total = 0;

        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_STPCT", conn);

        auto onehandler = [&](dolphindb::Message msg)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            msg_total += 1;
            bool succeeded = false;
            dolphindb::TableSP tmp = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    appender.append(tmp);
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    dolphindb::Util::sleep(300);
                }
            }
            if (msg.getOffset() == 999){
                notify.set();
            }
        };

        int listenport = std::get<0>(GetParam());
        std::cout << "current listenport is " << listenport << std::endl;

        int threadCount = rand() % 10 + 1;
        dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, threadCount);
        std::string dataScript = std::get<1>(GetParam()).second;
        std::thread th = std::thread([&]() {
            insertData(conn, st, dataScript);
        });
        auto threadVec = client.subscribe(HOST, PORT, onehandler, st, "actionTest", 0, true, nullptr, false, false, USER, PASSWD);
        ASSERT_EQ(threadVec.size(), threadCount);
        th.join();

        notify.wait();
        std::cout << "total size:" << msg_total << std::endl;
        client.unsubscribe(HOST, PORT, st, "actionTest");

        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_STPCT order by ts;\
                            ex = select * from "+st+" order by ts;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

        ASSERT_EQ(msg_total, 1000);
        for (auto &t : threadVec)
        {
            ASSERT_TRUE(t->isComplete());
            ASSERT_EQ(client.getQueueDepth(t), 0);
        }
    }


    TEST_P(StreamingThreadPooledClientTester, test_resub_true_with_resubscribeTimeout)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STPCT::createSharedTableAndReplay(st, 1000);

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;
        dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, 2);
        unsigned int resubscribeTimeout = 500;
        dolphindb::ThreadPooledClient client1 = dolphindb::ThreadPooledClient(listenport, 2);
        client.subscribe(HOST, PORT, [](dolphindb::Message msg){}, st, DEFAULT_ACTION_NAME, 0, true, nullptr, false, false, USER, PASSWD);
        auto ths = client1.subscribe(HOST, PORT, [](dolphindb::Message msg){}, st, DEFAULT_ACTION_NAME, 0, true, nullptr, false, false, USER, PASSWD, nullptr, {}, 100, false, resubscribeTimeout);

        dolphindb::Util::sleep(resubscribeTimeout+1000);
        client.unsubscribe(HOST, PORT, st, DEFAULT_ACTION_NAME);
        client1.exit();
        dolphindb::Util::sleep(1000);
        for (auto t : ths){
            ASSERT_TRUE(t->isComplete());
        }
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
    }

}
