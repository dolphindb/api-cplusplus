#include <gtest/gtest.h>
#include "config.h"
#include "Streaming.h"

namespace STCT
{
    class StreamingThreadedClientTester : public testing::Test, public ::testing::WithParamInterface<int>
    {
        public:
            static dolphindb::DBConnection conn;
            static dolphindb::StreamingClientConfig* scfg;
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
                scfg->protocol = dolphindb::TransportationProtocol::TCP;
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
    dolphindb::StreamingClientConfig* StreamingThreadedClientTester::scfg = new dolphindb::StreamingClientConfig();
    dolphindb::DBConnection StreamingThreadedClientTester::conn(false, false);

    static void createSharedTableAndReplay(const std::string &st, int rows)
    {
        std::string script = "\
                share streamTable(100:0, `datetimev`timestampv`sym`price1`price2,[DATETIME,TIMESTAMP,SYMBOL,DOUBLE,DOUBLE]) as "+st+"\n\
                go\n\
                setStreamTableFilterColumn("+st+", `sym)";
        StreamingThreadedClientTester::conn.run(script);

        std::string replayScript = "n = " + std::to_string(rows) + ";table1_STCT = table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);\
                share table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]) as "+st+"_res_STCT;go;\
                tableInsert(table1_STCT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));share table1_STCT as "+st+"_ex_STCT;\
                replay(inputTables=table1_STCT, outputTables=`"+st+", dateColumn=`timestampv, timeColumn=`timestampv)";
        StreamingThreadedClientTester::conn.run(replayScript);
    }

    static void createSharedTableAndReplay_withAllDataType(const std::string &st)
    {
        srand(time(NULL));
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
            "table1_STCT = table(1:0, colName, colType);"
            "row_num = 1000;"
            "share table(1:0, colName, colType) as "+st+"_res_STCT;go;"
            "col1 = rand(2 ,row_num);col2 = rand(256 ,row_num);col3 = rand(-row_num..row_num ,row_num);col4 = rand(-row_num..row_num ,row_num);col5 = rand(-row_num..row_num ,row_num);col6 = rand(0..row_num ,row_num);col7 = rand(0..row_num ,row_num);"
            "col8 = rand(0..row_num ,row_num);col9 = rand(0..row_num ,row_num);col10 = rand(0..row_num ,row_num);col11 = rand(0..row_num ,row_num);col12 = rand(0..row_num ,row_num);col13 = rand(0..row_num ,row_num);col14= rand(0..row_num ,row_num);"
            "col15 = rand(round(row_num,2) ,row_num);col16 = rand(round(row_num,2) ,row_num);col17 =rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);col18 =rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);col19 =rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);col20 = take(ipaddr(\"192.168.1.13\"),row_num);"
            "col21 = take(uuid(\"5d212a78-cc48-e3b1-4235-b4d91473ee87\"),row_num);col22 = take(int128(\"e1671797c52e15f763380b45e841ec32\"),row_num);col23=rand((-1000..1000)/1000.0000$DECIMAL32(" +
            std::to_string(scale32) + "),row_num);col24=rand((-1000..1000)/1000.0000$DECIMAL64(" + std::to_string(scale64) + "),row_num);col25=rand((-1000..1000)/1000.0000$DECIMAL128(" + std::to_string(scale128) + "),row_num);go;"
            "for (i in 0..(row_num-1)){tableInsert(table1_STCT,i,col1[i],col2[i],col3[i],col4[i],col5[i],col6[i],col7[i],col8[i],col9[i],col10[i],col11[i],col12[i],col13[i],col14[i],col15[i],col16[i],col17[i],col18[i],col19[i],col20[i],col21[i],col22[i],col23[i],col24[i],col25[i])};"
            "share table1_STCT as "+st+"_ex_STCT;go;"
            "replay(inputTables=table1_STCT, outputTables=`"+st+", dateColumn=`ctimestamp, timeColumn=`ctimestamp);";
        StreamingThreadedClientTester::conn.run(replayScript);
    }

    static void createSharedTableAndReplay_withArrayVector(const std::string &st)
    {
        std::string replayScript =
            "colName = `ts`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cdatehour`cfloat`cdouble`cipaddr`cuuid`cint128`cdecimal32`cdecimal64`cdecimal128;"
            "colType = [TIMESTAMP, BOOL[], CHAR[], SHORT[], INT[],LONG[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], DATEHOUR[], FLOAT[], DOUBLE[], IPADDR[], UUID[], INT128[],DECIMAL32(6)[],DECIMAL64(16)[],DECIMAL128(26)[]];"
            "share streamTable(1:0,colName, colType) as "+st+";"
            "share table(1:0, colName, colType) as "+st+"_res_STCT;go;"
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
            "share table1_STCT as "+st+"_ex_STCT;go;"
            "replay(inputTables=table1_STCT, outputTables=`"+st+", dateColumn=`ts, timeColumn=`ts);";
        StreamingThreadedClientTester::conn.run(replayScript);
    }

    INSTANTIATE_TEST_SUITE_P(, StreamingThreadedClientTester, testing::Values(0, rand() % 1000 + 13000));
    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_STCT", conn);

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
        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        auto thread = threadedClient.subscribe(HOST, PORT, onehandler, st, "mutiSchemaOne", 0, true, nullptr, false, false, USER, PASSWD);
        notify.wait();

        std::cout << "total size: " << msg_total << std::endl;
        threadedClient.unsubscribe(HOST, PORT, st, "mutiSchemaOne");

        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_STCT order by datetimev;\
                            ex = select * from "+case_+"_ex_STCT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(threadedClient.getQueueDepth(thread), 0);
        ASSERT_EQ(msg_total, 1000);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_STCT", conn);

        auto batchhandler = [&](std::vector<dolphindb::Message> msgs)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
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
                        dolphindb::Util::sleep(100);
                    }
                }
                if (msg.getOffset() == 999)
                {
                    notify.set();
                }
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        auto thread = threadedClient.subscribe(HOST, PORT, batchhandler, st, "mutiSchemaBatch", 0, true, nullptr, false, 1, 1.0, false, USER, PASSWD);
        notify.wait();

        std::cout << "total size: " << msg_total << std::endl;
        threadedClient.unsubscribe(HOST, PORT, st, "mutiSchemaBatch");

        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_STCT order by datetimev;\
                            ex = select * from "+case_+"_ex_STCT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(threadedClient.getQueueDepth(thread), 0);
        ASSERT_EQ(msg_total, 1000);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribeUnsubscribe_notExistTable)
    {
        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        auto queue = threadedClient.subscribe(HOST, PORT, [](dolphindb::Message msg){}, "st_notExist", "actionTest", 0, true, nullptr, false, false, USER, PASSWD);
        dolphindb::Util::sleep(1000);
        threadedClient.unsubscribe(HOST, PORT, "st_notExist", "actionTest");
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_tableNameNull)
    {
        auto onehandler = [](dolphindb::Message msg){};

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);

        ASSERT_ANY_THROW(auto thread = threadedClient.subscribe(HOST, PORT, onehandler, "", DEFAULT_ACTION_NAME, 0, false, nullptr, false, false, USER, PASSWD));
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_tableNameNull)
    {
        auto batchhandler = [](std::vector<dolphindb::Message> msgs){};

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        ASSERT_ANY_THROW(auto thread = threadedClient.subscribe(HOST, PORT, batchhandler, "", DEFAULT_ACTION_NAME, 0, false, nullptr, false, 1, 1.0, false, USER, PASSWD));
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_userNameNull)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        bool enableClientAuth = conn.run("bool(getConfig('enableClientAuth'))")->getBool();
        if (enableClientAuth){
            ASSERT_ANY_THROW(threadedClient.subscribe(HOST, PORT, [=](dolphindb::Message msg){}, st, DEFAULT_ACTION_NAME, -1, false));
        }else{
            auto thread = threadedClient.subscribe(HOST, PORT, [=](dolphindb::Message msg){}, st, DEFAULT_ACTION_NAME);
            dolphindb::Util::sleep(1000);
            ASSERT_FALSE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            threadedClient.unsubscribe(HOST, PORT, st, DEFAULT_ACTION_NAME);
            dolphindb::Util::sleep(1000);
            ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        }
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_offsetNegative)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        auto onehandler = [&](dolphindb::Message msg)
        {
            msg_total += 1;
            // std::cout << msg->getString() << std::endl;
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        auto thread = threadedClient.subscribe(HOST, PORT, onehandler, st, "actionTest", -1, true, nullptr, false, false, USER, PASSWD);

        std::cout << "total size: " << msg_total << std::endl;
        threadedClient.unsubscribe(HOST, PORT, st, "actionTest");
        threadedClient.exit();
        ASSERT_TRUE(threadedClient.isExit());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(threadedClient.getQueueDepth(thread), 0);
        ASSERT_EQ(msg_total, 0);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_offsetNegative)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        auto batchhandler = [&](std::vector<dolphindb::Message> msgs)
        {
            for (auto &msg : msgs)
            {
                msg_total += 1;
                // std::cout  << msg->getString() << std::endl;
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;
        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        auto thread = threadedClient.subscribe(HOST, PORT, batchhandler, st, "actionTest", -1, true, nullptr, false, 1, 1.0, false, USER, PASSWD);

        std::cout << "total size: " << msg_total << std::endl;
        threadedClient.unsubscribe(HOST, PORT, st, "actionTest");
        threadedClient.exit();
        ASSERT_TRUE(threadedClient.isExit());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(threadedClient.getQueueDepth(thread), 0);
        ASSERT_EQ(msg_total, 0);
    }

    TEST_P(StreamingThreadedClientTester,test_subscribe_onehandler_offsetInMiddle)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay(st, 10);
        int msg_total = 0;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;

        dolphindb::AutoFitTableAppender appender("", case_+"_res_STCT", conn);

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

            int cur_offset = msg.getOffset();
            ASSERT_EQ(cur_offset, msg_total+4);
            if (cur_offset == 9)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        auto thread = threadedClient.subscribe(HOST, PORT, onehandler, st, "actionTest", 5, true, nullptr, false, false, USER, PASSWD);
        notify.wait();
        std::cout << "total size: " << msg_total << std::endl;
        threadedClient.unsubscribe(HOST, PORT, st, "actionTest");
        threadedClient.exit();
        ASSERT_TRUE(threadedClient.isExit());
        ASSERT_TRUE(conn.run(("eqObj("+case_+"_res_STCT.rows(), 5)"))->getBool());
        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_STCT order by datetimev;\
                            ex = select * from "+case_+"_ex_STCT order by datetimev limit 5, 5;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(threadedClient.getQueueDepth(thread), 0);
        ASSERT_EQ(msg_total, 5);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_offsetInMiddle)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay(st, 10);
        int msg_total = 0;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;

        dolphindb::AutoFitTableAppender appender("", case_+"_res_STCT", conn);

        auto batchhandler = [&](std::vector<dolphindb::Message> msgs)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
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
                        dolphindb::Util::sleep(100);
                    }
                }
                int cur_offset = msg.getOffset();
                ASSERT_EQ(cur_offset, msg_total+4);
                if (cur_offset == 9)
                {
                    notify.set();
                }
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        auto thread = threadedClient.subscribe(HOST, PORT, batchhandler, st, "actionTest", 5, true, nullptr, false, 1, 1.0, false, USER, PASSWD);
        notify.wait();
        std::cout << "total size: " << msg_total << std::endl;
        threadedClient.unsubscribe(HOST, PORT, st, "actionTest");
        threadedClient.exit();
        ASSERT_TRUE(threadedClient.isExit());
        ASSERT_TRUE(conn.run(("eqObj("+case_+"_res_STCT.rows(), 5)"))->getBool());
        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_STCT order by datetimev;\
                            ex = select * from "+case_+"_ex_STCT order by datetimev limit 5, 5;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(threadedClient.getQueueDepth(thread), 0);
        ASSERT_EQ(msg_total, 5);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_filter)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_STCT", conn);
        int target_rows = conn.run("(exec count(*) from "+case_+"_ex_STCT where sym='b')[0]")->getInt();

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

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        dolphindb::VectorSP filter = dolphindb::Util::createVector(dolphindb::DT_SYMBOL, 1, 1);
        filter->setString(0, "b");
        auto thread = threadedClient.subscribe(HOST, PORT, onehandler, st, "actionTest", 0, true, filter, false, false, USER, PASSWD);
        notify.wait();

        std::cout << "total size: " << msg_total << std::endl;
        threadedClient.unsubscribe(HOST, PORT, st, "actionTest");
        threadedClient.exit();
        ASSERT_TRUE(threadedClient.isExit());
        ASSERT_TRUE(conn.run("re = exec sym from "+case_+"_res_STCT; all(re == `b)")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(threadedClient.getQueueDepth(thread), 0);
        ASSERT_TRUE(msg_total > 0);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_filter)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_STCT", conn);

        auto batchhandler = [&](std::vector<dolphindb::Message> msgs)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
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
                        dolphindb::Util::sleep(100);
                    }
                }
                if (msg.getOffset() == 999)
                {
                    notify.set();
                }
            }
        };
        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        dolphindb::VectorSP filter = dolphindb::Util::createVector(dolphindb::DT_SYMBOL, 1, 1);
        filter->setString(0, "b");
        auto thread = threadedClient.subscribe(HOST, PORT, batchhandler, st, "actionTest", 0, true, filter, false, 1, 1.0, false, USER, PASSWD);
        dolphindb::Util::sleep(2000);

        std::cout << "total size: " << msg_total << std::endl;
        threadedClient.unsubscribe(HOST, PORT, st, "actionTest");
        threadedClient.exit();
        ASSERT_TRUE(threadedClient.isExit());
        ASSERT_TRUE(conn.run("re = exec sym from "+case_+"_res_STCT; all(re == `b)")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(threadedClient.getQueueDepth(thread), 0);
        ASSERT_TRUE(msg_total > 0);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_msgAsTable)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_STCT", conn);

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
            int last_offset = first_offset + msg->rows() -1;
            if (last_offset == 999)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        auto thread = threadedClient.subscribe(HOST, PORT, onehandler, st, "actionTest", 0, false, nullptr, true, false, USER, PASSWD);
        notify.wait();

        std::cout << "total size: " << msg_total << std::endl;
        threadedClient.unsubscribe(HOST, PORT, st, "actionTest");
        threadedClient.exit();
        ASSERT_TRUE(threadedClient.isExit());
        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_STCT order by datetimev;\
                            ex = select * from "+case_+"_ex_STCT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(threadedClient.getQueueDepth(thread), 0);
        ASSERT_EQ(msg_total, 1000);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_msgAsTable)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay(st, 10000);
        int msg_total = 0;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_STCT", conn);

        auto batchhandler = [&](std::vector<dolphindb::Message> msgs)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
                ASSERT_EQ(msg->getForm(), dolphindb::DF_TABLE);
                msg_total += msg->rows();
                std::cout << msg->rows() << std::endl;
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
                int first_offset = msg.getOffset();
                int last_offset = first_offset + msg->rows() -1;
                if (last_offset == 9999)
                {
                    notify.set();
                }
            }

        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        auto thread = threadedClient.subscribe(HOST, PORT, batchhandler, st, "actionTest", 0, false, nullptr, false, 2000, 0.1, true, USER, PASSWD);
        notify.wait();

        std::cout << "total size: " << msg_total << std::endl;
        threadedClient.unsubscribe(HOST, PORT, st, "actionTest");
        threadedClient.exit();
        ASSERT_TRUE(threadedClient.isExit());
        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_STCT order by datetimev;\
                            ex = select * from "+case_+"_ex_STCT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(threadedClient.getQueueDepth(thread), 0);
        ASSERT_EQ(msg_total, 10000);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_allowExists)
    {
        GTEST_SKIP() << "server还没有修复allowExists的问题, jira: https://dolphindb1.atlassian.net/browse/D20-14283";
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total,msg_total2 = 0;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;

        auto onehandler = [&](dolphindb::Message msg)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            msg_total += 1;
            // std::cout << msg->getString() << std::endl;
            if (msg.getOffset() == 999)
            {
                notify.set();
            }
        };
        auto onehandler2 = [&](dolphindb::Message msg)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            msg_total2 += 1;
            // std::cout << msg->getString() << std::endl;
            if (msg_total2 == 1000)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        dolphindb::ThreadedClient threadedClient2(listenport);
        auto thread = threadedClient.subscribe(HOST, PORT, onehandler, st, "actionTest", 0, false, nullptr, false, true, USER, PASSWD);
        auto thread2 = threadedClient2.subscribe(HOST, PORT, onehandler2, st, "actionTest", 0, false, nullptr, false, true, USER, PASSWD);
        notify.wait();

        std::cout << "total size: " << msg_total << std::endl;
        threadedClient.unsubscribe(HOST, PORT, st, "actionTest");
        threadedClient2.unsubscribe(HOST, PORT, st, "actionTest");
        threadedClient.exit();
        ASSERT_TRUE(threadedClient.isExit());
        threadedClient2.exit();
        ASSERT_TRUE(threadedClient2.isExit());

        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(threadedClient.getQueueDepth(thread), 0);
        ASSERT_EQ(threadedClient2.getQueueDepth(thread2), 0);
        ASSERT_EQ(msg_total, 1000);
        ASSERT_EQ(msg_total2, 1000);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_allowExists)
    {
        GTEST_SKIP() << "server还没有修复allowExists的问题, jira: https://dolphindb1.atlassian.net/browse/D20-14283";
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total,msg_total2 = 0;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;

        auto batchhandler = [&](std::vector<dolphindb::Message> msgs)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
                msg_total += 1;
                // std::cout  << msg->getString() << std::endl;
                if (msg.getOffset() == 999)
                    notify.set();
            }
        };
        auto batchhandler2 = [&](std::vector<dolphindb::Message> msgs)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
                msg_total2 += 1;
                // std::cout  << msg->getString() << std::endl;
                if (msg.getOffset() == 999)
                    notify.set();
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        dolphindb::ThreadedClient threadedClient2(listenport);
        auto thread = threadedClient.subscribe(HOST, PORT, batchhandler, st, "actionTest", 0, false, nullptr, false, 1, 1.0, false, USER, PASSWD);
        auto thread2 = threadedClient2.subscribe(HOST, PORT, batchhandler2, st, "actionTest", 0, false, nullptr, false, 1, 1.0, false, USER, PASSWD);
        notify.wait();

        std::cout << "total size: " << msg_total << std::endl;
        threadedClient.unsubscribe(HOST, PORT, st, "actionTest");
        threadedClient.exit();
        ASSERT_TRUE(threadedClient.isExit());
        threadedClient2.unsubscribe(HOST, PORT, st, "actionTest");
        threadedClient2.exit();
        ASSERT_TRUE(threadedClient2.isExit());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(threadedClient.getQueueDepth(thread), 0);
        ASSERT_EQ(msg_total, 1000);
        ASSERT_EQ(threadedClient2.getQueueDepth(thread2), 0);
        ASSERT_EQ(msg_total2, 1000);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_resub_false)
    {
        auto batchhandler = [&](std::vector<dolphindb::Message> msgs){};

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        ASSERT_ANY_THROW(auto th = threadedClient.subscribe(HOST, PORT, batchhandler, "st", "actionTest", 0, false, nullptr, false, 1, 1.0, false, USER, PASSWD));
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_resub_true)
    {
        std::vector<dolphindb::Message> msgs;
        auto batchhandler = [&](std::vector<dolphindb::Message> ms)
        {
            for (auto &msg : ms)
            {
                msgs.push_back(msg);
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        auto thread = threadedClient.subscribe(HOST, PORT, batchhandler, "nonExistTable", "resubTest", 0, true, nullptr, false, 1, 1.0, false, USER, PASSWD);
        dolphindb::Util::sleep(1000);
        threadedClient.unsubscribe(HOST, PORT, "nonExistTable", "resubTest");
        ASSERT_EQ(msgs.size(), 0);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_batchSize)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_STCT", conn);

        auto batchhandler = [&](std::vector<dolphindb::Message> msgs)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
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
                        dolphindb::Util::sleep(100);
                    }
                }
                if (msg.getOffset() == 999)
                {
                    notify.set();
                }
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        auto thread = threadedClient.subscribe(HOST, PORT, batchhandler, st, "actionTest", 0, true, nullptr, false, 200, 0.1, false, USER, PASSWD);
        notify.wait();

        std::cout << "total size: " << msg_total << std::endl;
        threadedClient.unsubscribe(HOST, PORT, st, "actionTest");
        threadedClient.exit();
        ASSERT_TRUE(threadedClient.isExit());
        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_STCT order by datetimev;\
                            ex = select * from "+case_+"_ex_STCT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(threadedClient.getQueueDepth(thread), 0);
        ASSERT_EQ(msg_total, 1000);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_throttle)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay(st, 10);
        int msg_total = 0;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_STCT", conn);
        long long start, end;

        auto batchhandler = [&](std::vector<dolphindb::Message> msgs)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
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
                        dolphindb::Util::sleep(100);
                    }
                }
                if (msg.getOffset() == 9)
                {
                    notify.set();
                    end = dolphindb::Util::getEpochTime();
                }
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        auto thread = threadedClient.subscribe(HOST, PORT, batchhandler, st, "actionTest", 0, true, nullptr, false, 5000, 2.5, false, USER, PASSWD);
        start = dolphindb::Util::getEpochTime();
        notify.wait();

        std::cout << "total size: " << msg_total << std::endl;
        threadedClient.unsubscribe(HOST, PORT, st, "actionTest");
        threadedClient.exit();
        ASSERT_TRUE(threadedClient.isExit());
        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_STCT order by datetimev;\
                            ex = select * from "+case_+"_ex_STCT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(threadedClient.getQueueDepth(thread), 0);
        ASSERT_EQ(msg_total, 10);

        auto duration = end - start;
        ASSERT_TRUE(duration >= 2500);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_hostNull)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        auto onehandler = [&](dolphindb::Message msg)
        {
            msg_total += 1;
            // std::cout << msg->getString() << std::endl;
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        ASSERT_ANY_THROW(threadedClient.subscribe("", PORT, onehandler, st, "actionTest", 0, false, nullptr, false, true, USER, PASSWD));
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_hostNull)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        auto batchhandler = [&](std::vector<dolphindb::Message> msgs)
        {
            for (auto &msg : msgs)
            {
                msg_total += 1;
                // std::cout  << msg->getString() << std::endl;
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        ASSERT_ANY_THROW(threadedClient.subscribe("", PORT, batchhandler, st, "actionTest", 0, false, nullptr, false, 1, 1.0, false, USER, PASSWD));
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_portNull)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        auto onehandler = [&](dolphindb::Message msg)
        {
            msg_total += 1;
            // std::cout << msg->getString() << std::endl;
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        ASSERT_ANY_THROW(threadedClient.subscribe(HOST, NULL, onehandler, st, "actionTest", 0, false, nullptr, false, true, USER, PASSWD));
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_actionNameNull)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;

        auto onehandler = [&](dolphindb::Message msg)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            msg_total += 1;
            // std::cout << msg->getString() << std::endl;
            if (msg.getOffset() == 999)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        auto thread = threadedClient.subscribe(HOST, PORT, onehandler, st, "", 0, false, nullptr, false, true, USER, PASSWD);
        notify.wait();

        dolphindb::TableSP stat = conn.run("select * from getStreamingStat().pubTables where tableName=`"+case_);

        ASSERT_EQ(stat->getColumn(0)->getRow(0)->getString(), st);
        ASSERT_EQ(stat->getColumn(2)->getRow(0)->getInt(), 1000);
        ASSERT_EQ(stat->getColumn(3)->getRow(0)->getString(), "");

        std::cout << "total size: " << msg_total << std::endl;
        threadedClient.unsubscribe(HOST, PORT, st, "");
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(threadedClient.getQueueDepth(thread), 0);
        ASSERT_EQ(msg_total, 1000);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_actionNameNull)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;

        auto batchhandler = [&](std::vector<dolphindb::Message> msgs)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
                msg_total += 1;
                // std::cout  << msg->getString() << std::endl;
                if (msg.getOffset() == 999)
                {
                    notify.set();
                }
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        auto thread = threadedClient.subscribe(HOST, PORT, batchhandler, st, "", 0, false, nullptr, false, 1, 1.0, false, USER, PASSWD);
        notify.wait();

        dolphindb::TableSP stat = conn.run("select * from getStreamingStat().pubTables where tableName=`"+case_);

        ASSERT_EQ(stat->getColumn(0)->getRow(0)->getString(), st);
        ASSERT_EQ(stat->getColumn(2)->getRow(0)->getInt(), 1000);
        ASSERT_EQ(stat->getColumn(3)->getRow(0)->getString(), "");

        std::cout << "total size: " << msg_total << std::endl;
        threadedClient.unsubscribe(HOST, PORT, st, "");
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(threadedClient.getQueueDepth(thread), 0);
        ASSERT_EQ(msg_total, 1000);
    }

    TEST_P(StreamingThreadedClientTester, test_unsubscribe_onehandler_hostNull)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        auto onehandler = [&](dolphindb::Message msg)
        {
            msg_total += 1;
            // std::cout << msg->getString() << std::endl;
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        auto thread = threadedClient.subscribe(HOST, PORT, onehandler, st, "actionTest", 0, false, nullptr, false, true, USER, PASSWD);

        std::cout << "total size: " << msg_total << std::endl;
        ASSERT_FALSE(threadedClient.unsubscribe("", PORT, st, "actionTest"));

        threadedClient.unsubscribe(HOST, PORT, st, "actionTest");
        threadedClient.exit();
        ASSERT_TRUE(threadedClient.isExit());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
    }

    TEST_P(StreamingThreadedClientTester, test_unsubscribe_portNull)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        auto onehandler = [&](dolphindb::Message msg)
        {
            msg_total += 1;
            // std::cout << msg->getString() << std::endl;
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        auto thread = threadedClient.subscribe(HOST, PORT, onehandler, st, "actionTest", 0, false, nullptr, false, true, USER, PASSWD);
        std::cout << "total size: " << msg_total << std::endl;

        ASSERT_FALSE(threadedClient.unsubscribe(HOST, NULL, st, "actionTest"));
        threadedClient.unsubscribe(HOST, PORT, st, "actionTest");
        threadedClient.exit();
        ASSERT_TRUE(threadedClient.isExit());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
    }

    TEST_P(StreamingThreadedClientTester, test_unsubscribe_tableNameNull)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        auto onehandler = [&](dolphindb::Message msg)
        {
            msg_total += 1;
            // std::cout << msg->getString() << std::endl;
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        auto thread = threadedClient.subscribe(HOST, PORT, onehandler, st, "actionTest", 0, false, nullptr, false, true, USER, PASSWD);

        std::cout << "total size: " << msg_total << std::endl;
        ASSERT_FALSE(threadedClient.unsubscribe(HOST, PORT, "", "actionTest"));

        threadedClient.unsubscribe(HOST, PORT, st, "actionTest");
        threadedClient.exit();
        ASSERT_TRUE(threadedClient.isExit());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
    }

    TEST_P(StreamingThreadedClientTester, test_unsubscribe_actionNameNull)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;

        auto onehandler = [&](dolphindb::Message msg)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            msg_total += 1;
            // handle msg
            if (msg.getOffset() == 999)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        auto thread = threadedClient.subscribe(HOST, PORT, onehandler, st, "actionTest", 0, false, nullptr, false, true, USER, PASSWD);
        notify.wait();

        std::cout << "total size: " << msg_total << std::endl;
        threadedClient.unsubscribe(HOST, PORT, st, "");
        dolphindb::TableSP stat = conn.run("select * from getStreamingStat().pubTables where tableName=`"+case_);

        ASSERT_EQ(stat->getColumn(0)->getRow(0)->getString(), st);
        ASSERT_EQ(stat->getColumn(2)->getRow(0)->getInt(), 1000);
        ASSERT_EQ(stat->getColumn(3)->getRow(0)->getString(), "actionTest");

        threadedClient.unsubscribe(HOST, PORT, st, "actionTest");
        threadedClient.exit();
        ASSERT_TRUE(threadedClient.isExit());

        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
    }

    TEST_P(StreamingThreadedClientTester, tes_onehandler_subscribe_twice)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        auto onehandler = [&](dolphindb::Message msg)
        {
            msg_total += 1;
            // handle msg
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;
        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        auto thread1 = threadedClient.subscribe(HOST, PORT, onehandler, st, "mutiSchemaOne", 0, false, nullptr, false, true, USER, PASSWD);
        ASSERT_ANY_THROW(auto thread2 = threadedClient.subscribe(HOST, PORT, onehandler, st, "mutiSchemaOne", 0, false, nullptr, false, true, USER, PASSWD));

        threadedClient.unsubscribe(HOST, PORT, st, "mutiSchemaOne");
        dolphindb::Util::sleep(1000);
        std::cout << "total size: " << msg_total << std::endl;
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(threadedClient.getQueueDepth(thread1), 0);
        ASSERT_EQ(msg_total, 1000);
    }

    TEST_P(StreamingThreadedClientTester, test_batchhandler_subscribe_twice)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;

        auto batchhandler = [&](std::vector<dolphindb::Message> msgs)
        {
            for (auto &msg : msgs)
            {
                dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
                msg_total += 1;
                // handle msg
                if (msg.getOffset() == 999)
                {
                    notify.set();
                }
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        auto thread1 = threadedClient.subscribe(HOST, PORT, batchhandler, st, "mutiSchemaBatch", 0, false, nullptr, false, 1, 1.0, false, USER, PASSWD);
        ASSERT_ANY_THROW(auto thread2 = threadedClient.subscribe(HOST, PORT, batchhandler, st, "mutiSchemaBatch", 0, false, nullptr, false, 1, 1.0, false, USER, PASSWD));
        notify.wait();

        std::cout << "total size: " << msg_total << std::endl;
        threadedClient.unsubscribe(HOST, PORT, st, "mutiSchemaBatch");
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(threadedClient.getQueueDepth(thread1), 0);
        ASSERT_EQ(msg_total, 1000);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_withAllDataType)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay_withAllDataType(st);
        int msg_total = 0;
        dolphindb::TableSP ex_table = conn.run("select * from "+st+"");
        int index = 0;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_STCT", conn);

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
        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        auto thread = threadedClient.subscribe(HOST, PORT, onehandler, st, "mutiSchemaOne", 0, false, nullptr, false, true, USER, PASSWD);
        notify.wait();
        std::cout << "total size: " << msg_total << std::endl;
        threadedClient.unsubscribe(HOST, PORT, st, "mutiSchemaOne");

        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_STCT order by ind;\
                            ex = select * from "+case_+"_ex_STCT order by ind;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(threadedClient.getQueueDepth(thread), 0);
        ASSERT_EQ(msg_total, 1000);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_withAllDataType)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay_withAllDataType(st);
        int msg_total = 0;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_STCT", conn);

        auto batchhandler = [&](std::vector<dolphindb::Message> msgs)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
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
                        dolphindb::Util::sleep(100);
                    }
                }
                if (msg.getOffset() == 999)
                {
                    notify.set();
                }
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        auto thread = threadedClient.subscribe(HOST, PORT, batchhandler, st, "mutiSchemaBatch", 0, false, nullptr, false, 1, 1.0, false, USER, PASSWD);
        notify.wait();

        std::cout << "total size: " << msg_total << std::endl;
        threadedClient.unsubscribe(HOST, PORT, st, "mutiSchemaBatch");
        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_STCT order by ind;\
                            ex = select * from "+case_+"_ex_STCT order by ind;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(threadedClient.getQueueDepth(thread), 0);
        ASSERT_EQ(msg_total, 1000);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_onehandler_arrayVector)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay_withArrayVector(st);
        int msg_total = 0;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_STCT", conn);

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
        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        auto thread = threadedClient.subscribe(HOST, PORT, onehandler, st, "arrayVectorTableTest", 0, false, nullptr, false, true, USER, PASSWD);
        notify.wait();

        std::cout << "total size: " << msg_total << std::endl;
        threadedClient.unsubscribe(HOST, PORT, st, "arrayVectorTableTest");
        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_STCT order by ts;\
                            ex = select * from "+case_+"_ex_STCT order by ts;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(threadedClient.getQueueDepth(thread), 0);
        ASSERT_EQ(msg_total, 1000);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_batchhandler_arrayVector)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay_withArrayVector(st);
        int msg_total = 0;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_STCT", conn);

        auto batchhandler = [&](std::vector<dolphindb::Message> msgs)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
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
                        dolphindb::Util::sleep(100);
                    }
                }
                if (msg.getOffset() == 999)
                {
                    notify.set();
                }
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        auto thread = threadedClient.subscribe(HOST, PORT, batchhandler, st, "arrayVectorTableTest", 0, false, nullptr, false, 1, 1.0, false, USER, PASSWD);
        notify.wait();

        std::cout << "total size: " << msg_total << std::endl;
        threadedClient.unsubscribe(HOST, PORT, st, "arrayVectorTableTest");
        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_STCT order by ts;\
                            ex = select * from "+case_+"_ex_STCT order by ts;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(threadedClient.getQueueDepth(thread), 0);
        ASSERT_EQ(msg_total, 1000);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_hugetable_onehandler)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay(st, 1000000);
        int msg_total = 0;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;

        auto onehandler = [&](dolphindb::Message msg)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            msg_total += 1;
            if (msg_total % 100000 == 0)
                std::cout << "now subscribed rows: " << msg_total << std::endl;
            // handle msg
            if (msg.getOffset() == 999999)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;
        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        auto thread = threadedClient.subscribe(HOST, PORT, onehandler, st, "mutiSchemaOne", 0, false, nullptr, false, true, USER, PASSWD);
        notify.wait();

        std::cout << "total size: " << msg_total << std::endl;
        threadedClient.unsubscribe(HOST, PORT, st, "mutiSchemaOne");
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

        ASSERT_EQ(threadedClient.getQueueDepth(thread), 0);
        ASSERT_EQ(msg_total, 1000000);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_hugetable_batchhandler)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay(st, 1000000);
        int msg_total = 0;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;

        auto batchhandler = [&](std::vector<dolphindb::Message> msgs)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
                msg_total += 1;
                if (msg_total % 100000 == 0)
                    std::cout << "now subscribed rows: " << msg_total << std::endl;
                // handle msg
                if (msg.getOffset() == 999999)
                {
                    notify.set();
                }
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        auto thread = threadedClient.subscribe(HOST, PORT, batchhandler, st, "mutiSchemaBatch", 0, false, nullptr, false, 1, 1.0, false, USER, PASSWD);
        notify.wait();

        std::cout << "total size: " << msg_total << std::endl;
        threadedClient.unsubscribe(HOST, PORT, st, "mutiSchemaBatch");
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

        ASSERT_EQ(threadedClient.getQueueDepth(thread), 0);
        ASSERT_EQ(msg_total, 1000000);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_hugetable_onehandler_msgAsTable)
    {
        std::string case_=getCaseName();
        std::string st =case_;
        STCT::createSharedTableAndReplay(st, 1000000);
        int msg_total = 0;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_STCT", conn);

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
        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        auto thread = threadedClient.subscribe(HOST, PORT, onehandler, st, "mutiSchemaOne", 0, false, nullptr, true, false, USER, PASSWD);
        notify.wait();

        std::cout << "total size: " << msg_total << std::endl;
        threadedClient.unsubscribe(HOST, PORT, st, "mutiSchemaOne");
        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_STCT order by datetimev;\
                            ex = select * from "+case_+"_ex_STCT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

        ASSERT_EQ(threadedClient.getQueueDepth(thread), 0);
        ASSERT_EQ(msg_total, 1000000);
    }

    TEST_P(StreamingThreadedClientTester, test_subscribe_hugetable_batchhandler_msgAsTable_batchSize_throttle)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay(st, 1000000);
        int msg_total = 0;
        int test_batchSize = rand() % 65535 + 1;
        float test_throttle = (float)rand() / RAND_MAX * 10;

        int actual_batchSize = 1024;
        while (actual_batchSize <= test_batchSize) {
            actual_batchSize += 1024;
        }
        int batch_count = 0;
        int max_batch = 1000000 / actual_batchSize;
        int remain_rows = 1000000 % actual_batchSize;
        long long start_time;
        long long end_time;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_STCT", conn);

        auto batchhandler = [&](std::vector<dolphindb::Message> msgs)
        {
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
                batch_count++;
                int rows = msg->rows();
                ASSERT_TRUE(rows == actual_batchSize || rows == remain_rows);
                msg_total += rows;
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
                if (batch_count == max_batch){
                    start_time = dolphindb::Util::getEpochTime();
                }

                int first_offset = msg.getOffset();
                int last_offset = first_offset + rows - 1;
                if (last_offset == 999999)
                {
                    notify.set();
                }
            }
        };

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        auto thread = threadedClient.subscribe(HOST, PORT, batchhandler, st, "mutiSchemaBatch", 0, false, nullptr, false, test_batchSize, test_throttle, true, USER, PASSWD);
        notify.wait();
        end_time = dolphindb::Util::getEpochTime();

        std::cout << "total size: " << msg_total << std::endl;
        threadedClient.unsubscribe(HOST, PORT, st, "mutiSchemaBatch");
        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_STCT order by datetimev;\
                            ex = select * from "+case_+"_ex_STCT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());

        ASSERT_EQ(threadedClient.getQueueDepth(thread), 0);
        ASSERT_EQ(msg_total, 1000000);
        ASSERT_TRUE(end_time - start_time >= test_throttle * 1000);
    }


    class StreamingThreadedClientTester_realtime : public testing::Test, public ::testing::WithParamInterface<std::tuple<int, std::pair<dolphindb::DATA_TYPE, std::string>>>
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
                "share table(1:0, colName, colType) as "+name+"_res_STCT;go;";
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
    dolphindb::DBConnection StreamingThreadedClientTester_realtime::conn(false, false);

    INSTANTIATE_TEST_SUITE_P(basicTypes, StreamingThreadedClientTester_realtime, testing::Combine(
        testing::Values(0, rand() % 1000 + 13000),
        testing::ValuesIn(StreamingThreadedClientTester_realtime::getData())));
    INSTANTIATE_TEST_SUITE_P(arrayVectorTypes, StreamingThreadedClientTester_realtime, testing::Combine(
        testing::Values(0, rand() % 1000 + 13000),
        testing::ValuesIn(StreamingThreadedClientTester_realtime::getAVData())));
    TEST_P(StreamingThreadedClientTester_realtime, test_realtime)
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
        dolphindb::AutoFitTableAppender appender("", case_+"_res_STCT", conn);

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
            if (msg.getOffset() == 999){
                notify.set();
            }
        };

        int listenport = std::get<0>(GetParam());
        std::cout << "current listenport is " << listenport << std::endl;

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        std::string dataScript = std::get<1>(GetParam()).second;
        std::thread th = std::thread([&]() {
            insertData(conn, st, dataScript);
        });

        auto thread = threadedClient.subscribe(HOST, PORT, onehandler, st, "test_realtime", 0, true, nullptr, false, false, USER, PASSWD, nullptr, {}, 100, false, 0);
        th.join();
        notify.wait();
        std::cout << "total size: " << msg_total << std::endl;
        threadedClient.unsubscribe(HOST, PORT, st, "test_realtime");

        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_STCT order by ts;\
                            ex = select * from "+st+" order by ts;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(threadedClient.getQueueDepth(thread), 0);
        ASSERT_EQ(msg_total, 1000);
    }

    TEST_P(StreamingThreadedClientTester, test_resub_true_with_resubscribeTimeout)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        STCT::createSharedTableAndReplay(st, 1000);

        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;
        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
        unsigned int resubscribeTimeout = 500;
        dolphindb::ThreadedClient threadedClient1 = dolphindb::ThreadedClient(listenport);
        threadedClient.subscribe(HOST, PORT, [](dolphindb::Message msg){}, st, DEFAULT_ACTION_NAME, 0, true, nullptr, false, false, USER, PASSWD);
        auto t = threadedClient1.subscribe(HOST, PORT, [](dolphindb::Message msg){}, st, DEFAULT_ACTION_NAME, 0, true, nullptr, false, false, USER, PASSWD, nullptr, {}, 100, false, resubscribeTimeout);

        dolphindb::Util::sleep(resubscribeTimeout+1000);
        threadedClient.unsubscribe(HOST, PORT, st, DEFAULT_ACTION_NAME);
        threadedClient1.exit();
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(t->isComplete());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
    }

    TEST_F(StreamingThreadedClientTester, test_new_subscribe_resub_true)
    {
        std::string case_=getCaseName();
        std::vector<dolphindb::Message> msgs;
        auto batchhandler = [&](std::vector<dolphindb::Message> ms)
        {
            for (auto &msg : ms)
            {
                msgs.push_back(msg);
            }
        };
        std::string st = case_;
        conn.run("share streamTable(1:0, `sym`val, [SYMBOL, INT]) as "+st+";tableInsert("+st+", `sym1, 1)");
        std::vector<dolphindb::SubscribeState> states;

        scfg->callback = [&](const dolphindb::SubscribeState state, const dolphindb::SubscribeInfo &info) {
            states.emplace_back(state);
            EXPECT_EQ(info.tableName, st);
            EXPECT_EQ(info.actionName, "resubTest");
            EXPECT_EQ(info.hostName, HOST);
            EXPECT_EQ(info.port, PORT);
            return true;
        };

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(*scfg);
        auto thread = threadedClient.subscribe(HOST, PORT, batchhandler, st, "resubTest", 0, true, nullptr, false, 1, 1.0, false, USER, PASSWD);
        dolphindb::Util::sleep(2000);
        conn.run("subinfo = (exec subscriber from getStreamingStat().pubTables where tableName=`"+st+")[0].split(':');"
                    "subClient = subinfo[0];"
                    "subPort=int(subinfo[1]);go;"
                    "stopPublishTable(subClient, subPort, `"+st+", `resubTest)");
        conn.run("tableInsert("+st+", `sym2, 2)");
        dolphindb::Util::sleep(1000);
        threadedClient.unsubscribe(HOST, PORT, st, "resubTest");

        ASSERT_EQ(msgs.size(), 2);
        ASSERT_EQ(states.size(), 4);
        ASSERT_EQ(states[0],dolphindb::SubscribeState::Connected);
        ASSERT_EQ(states[1],dolphindb::SubscribeState::Disconnected);
        ASSERT_EQ(states[2],dolphindb::SubscribeState::Resubscribing);
        ASSERT_EQ(states[3],dolphindb::SubscribeState::Connected);
    }


    TEST_F(StreamingThreadedClientTester, test_new_subscribe_resub_false)
    {
        std::string case_=getCaseName();
        std::vector<dolphindb::Message> msgs;
        auto batchhandler = [&](std::vector<dolphindb::Message> ms)
        {
            for (auto &msg : ms)
            {
                msgs.push_back(msg);
            }
        };
        std::string st = case_;
        conn.run("share streamTable(1:0, `sym`val, [SYMBOL, INT]) as "+st+";tableInsert("+st+", `sym1, 1)");
        std::vector<dolphindb::SubscribeState> states;

        scfg->callback = [&](const dolphindb::SubscribeState state, const dolphindb::SubscribeInfo &info) {
            states.emplace_back(state);
            EXPECT_EQ(info.tableName, st);
            EXPECT_EQ(info.actionName, "resubTest");
            EXPECT_EQ(info.hostName, HOST);
            EXPECT_EQ(info.port, PORT);
            return true;
        };

        dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(*scfg);
        auto thread = threadedClient.subscribe(HOST, PORT, batchhandler, st, "resubTest", 0, false, nullptr, false, 1, 1.0, false, USER, PASSWD);
        dolphindb::Util::sleep(2000);
        conn.run("subinfo = (exec subscriber from getStreamingStat().pubTables where tableName=`"+st+")[0].split(':');"
                    "subClient = subinfo[0];"
                    "subPort=int(subinfo[1]);go;"
                    "stopPublishTable(subClient, subPort, `"+st+", `resubTest)");
        conn.run("tableInsert("+st+", `sym2, 2)");
        dolphindb::Util::sleep(1000);
        threadedClient.unsubscribe(HOST, PORT, st, "resubTest");
        threadedClient.exit();

        ASSERT_TRUE(threadedClient.isExit());
        ASSERT_EQ(msgs.size(), 1);
        ASSERT_EQ(states.size(), 2);
        ASSERT_EQ(states[0],dolphindb::SubscribeState::Connected);
        ASSERT_EQ(states[1],dolphindb::SubscribeState::Disconnected);
    }
}