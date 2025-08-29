#include <gtest/gtest.h>
#include "config.h"
#include "Streaming.h"

namespace SPCT
{
    class StreamingPollingClientTester : public testing::Test, public ::testing::WithParamInterface<int>
    {
        public:
            static dolphindb::StreamingClientConfig* scfg;
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

    dolphindb::DBConnection StreamingPollingClientTester::conn(false, false);

    dolphindb::StreamingClientConfig* StreamingPollingClientTester::scfg = new dolphindb::StreamingClientConfig();

    static void createSharedTableAndReplay(const std::string &st, int rows)
    {
        std::string script = "\
                share streamTable(100:0, `datetimev`timestampv`sym`price1`price2,[DATETIME,TIMESTAMP,SYMBOL,DOUBLE,DOUBLE]) as "+st+"\n\
                go;\n\
                setStreamTableFilterColumn("+st+", `sym)";
        StreamingPollingClientTester::conn.run(script);

        std::string replayScript = "n = " + std::to_string(rows) + ";table1_SPCT = table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);\
                share table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]) as "+st+"_res_SPCT;go;\
                tableInsert(table1_SPCT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));share table1_SPCT as "+st+"_ex_SPCT;\
                replay(inputTables=table1_SPCT, outputTables=`"+st+", dateColumn=`timestampv, timeColumn=`timestampv)";
        StreamingPollingClientTester::conn.run(replayScript);
    }

    static void createSharedTableAndReplay_withAllDataType(const std::string &st)
    {
        int scale32 = rand() % 9;
        int scale64 = rand() % 18;
        int scale128 = rand() % 38;
        std::string replayScript =
            "colName =  `ind`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring`cblob`cipaddr`cuuid`cint128`cdecimal32`cdecimal64`cdecimal128;"
            "colType = [INT, BOOL, CHAR, SHORT, INT,LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING, BLOB, IPADDR, UUID, INT128, DECIMAL32(" +
            std::to_string(scale32) + "), DECIMAL64(" + std::to_string(scale64) + "), DECIMAL128(" + std::to_string(scale128) + ")];"
            "share streamTable(100:0,colName, colType) as "+st+";go;"
            "setStreamTableFilterColumn("+st+", `csymbol);go;"
            "table1_SPCT = table(1:0, colName, colType);"
            "row_num = 1000;"
            "share table(1:0, colName, colType) as "+st+"_res_SPCT;go;"
            "col1 = rand(2 ,row_num);col2 = rand(256 ,row_num);col3 = rand(-row_num..row_num ,row_num);col4 = rand(-row_num..row_num ,row_num);col5 = rand(-row_num..row_num ,row_num);col6 = rand(0..row_num ,row_num);col7 = rand(0..row_num ,row_num);"
            "col8 = rand(0..row_num ,row_num);col9 = rand(0..row_num ,row_num);col10 = rand(0..row_num ,row_num);col11 = rand(0..row_num ,row_num);col12 = rand(0..row_num ,row_num);col13 = rand(0..row_num ,row_num);col14= rand(0..row_num ,row_num);"
            "col15 = rand(round(row_num,2) ,row_num);col16 = rand(round(row_num,2) ,row_num);col17 =rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);col18 =rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);col19 =rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);col20 = take(ipaddr(\"192.168.1.13\"),row_num);"
            "col21 = take(uuid(\"5d212a78-cc48-e3b1-4235-b4d91473ee87\"),row_num);col22 = take(int128(\"e1671797c52e15f763380b45e841ec32\"),row_num);col23=rand((-1000..1000)/1000.0000$DECIMAL32(" +
            std::to_string(scale32) + "),row_num);col24=rand((-1000..1000)/1000.0000$DECIMAL64(" + std::to_string(scale64) + "),row_num);col25=rand((-1000..1000)/1000.0000$DECIMAL128(" + std::to_string(scale128) + "),row_num);go;"
            "for (i in 0..(row_num-1)){tableInsert(table1_SPCT,i,col1[i],col2[i],col3[i],col4[i],col5[i],col6[i],col7[i],col8[i],col9[i],col10[i],col11[i],col12[i],col13[i],col14[i],col15[i],col16[i],col17[i],col18[i],col19[i],col20[i],col21[i],col22[i],col23[i],col24[i],col25[i])};"
            "share table1_SPCT as "+st+"_ex_SPCT;go;"
            "replay(inputTables=table1_SPCT, outputTables=`"+st+", dateColumn=`ctimestamp, timeColumn=`ctimestamp);";
        StreamingPollingClientTester::conn.run(replayScript);
    }

    static void createSharedTableAndReplay_withArrayVector(const std::string &st)
    {
        std::string replayScript =
            "colName = `ts`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cdatehour`cfloat`cdouble`cipaddr`cuuid`cint128`cdecimal32`cdecimal64`cdecimal128;"
            "colType = [TIMESTAMP, BOOL[], CHAR[], SHORT[], INT[],LONG[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], DATEHOUR[], FLOAT[], DOUBLE[], IPADDR[], UUID[], INT128[],DECIMAL32(6)[],DECIMAL64(16)[],DECIMAL128(26)[]];"
            "share streamTable(1:0,colName, colType) as "+st+";"
            "share table(1:0, colName, colType) as "+st+"_res_SPCT;go;"
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
            "share table1_SPCT as "+st+"_ex_SPCT;go;"
            "replay(inputTables=table1_SPCT, outputTables=`"+st+", dateColumn=`ts, timeColumn=`ts);";
        StreamingPollingClientTester::conn.run(replayScript);
    }

    INSTANTIATE_TEST_SUITE_P(, StreamingPollingClientTester, testing::Values(-1, rand() % 1000 + 13000));
    TEST_P(StreamingPollingClientTester, test_subscribeUnsubscribe_normal)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        SPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        int listenport = GetParam();
        dolphindb::PollingClient client = listenport == -1? dolphindb::PollingClient() : dolphindb::PollingClient(listenport);
        auto queue = client.subscribe(HOST, PORT, st, DEFAULT_ACTION_NAME, 0, true, nullptr, false, false, USER, PASSWD);
        dolphindb::Message msg;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_SPCT", conn);
        std::thread th1 = std::thread([&]{
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            while (true)
            {
                queue->pop(msg);
                if(msg.isNull()) {
                    ASSERT_EQ(msg.getOffset(), -1);
                    break;
                }
                else{
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
                    if(msg.getOffset() == 999)
                        notify.set();
                }
            }
        });

        notify.wait();
        client.unsubscribe(HOST, PORT, st, DEFAULT_ACTION_NAME);
        th1.join();
        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_SPCT order by datetimev;\
                            ex = select * from "+case_+"_ex_SPCT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(queue->size(), 0);
        ASSERT_EQ(msg_total, 1000);
    }

    TEST_P(StreamingPollingClientTester, test_subscribeUnsubscribe_notExistTable)
    {
        int listenport = GetParam();
        dolphindb::PollingClient client = listenport == -1? dolphindb::PollingClient() : dolphindb::PollingClient(listenport);
        auto queue = client.subscribe(HOST, PORT, "st_notExist", DEFAULT_ACTION_NAME, 0, true, nullptr, false, false, USER, PASSWD);
        dolphindb::Util::sleep(1000);
        client.unsubscribe(HOST, PORT, "st_notExist", DEFAULT_ACTION_NAME);
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_hostNull)
    {
        int msg_total = 0;
        int listenport = GetParam();
        dolphindb::PollingClient client = listenport == -1? dolphindb::PollingClient() : dolphindb::PollingClient(listenport);
        ASSERT_ANY_THROW(client.subscribe("", PORT, "st", DEFAULT_ACTION_NAME, 0, false, nullptr, false, false, USER, PASSWD));
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_portNull)
    {
        int msg_total = 0;
        int listenport = GetParam();
        dolphindb::PollingClient client = listenport == -1? dolphindb::PollingClient() : dolphindb::PollingClient(listenport);
        ASSERT_ANY_THROW(client.subscribe(HOST, NULL, "st", DEFAULT_ACTION_NAME, 0, false, nullptr, false, false, USER, PASSWD));
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_tableNameNull)
    {
        int msg_total = 0;
        int listenport = GetParam();
        dolphindb::PollingClient client = listenport == -1? dolphindb::PollingClient() : dolphindb::PollingClient(listenport);
        ASSERT_ANY_THROW(auto queue = client.subscribe(HOST, PORT, "", DEFAULT_ACTION_NAME, 0, false, nullptr, false, false, USER, PASSWD));
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_userNameNull)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        SPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        int listenport = GetParam();
        dolphindb::PollingClient client = listenport == -1? dolphindb::PollingClient() : dolphindb::PollingClient(listenport);
        bool enableClientAuth = conn.run("bool(getConfig('enableClientAuth'))")->getBool();
        if (enableClientAuth){
            ASSERT_ANY_THROW(auto queue = client.subscribe(HOST, PORT, st, DEFAULT_ACTION_NAME, 0, false));
        }else{
            auto queue = client.subscribe(HOST, PORT, st, DEFAULT_ACTION_NAME, 0);
            dolphindb::Util::sleep(1000);
            ASSERT_FALSE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
            client.unsubscribe(HOST, PORT, st, DEFAULT_ACTION_NAME);
            dolphindb::Util::sleep(1000);
            ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        }
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_actionNameNull)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        SPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        int listenport = GetParam();
        dolphindb::PollingClient client = listenport == -1? dolphindb::PollingClient() : dolphindb::PollingClient(listenport);
        auto queue = client.subscribe(HOST, PORT, st, "", 0, true, nullptr, false, false, USER, PASSWD);
        dolphindb::Message msg;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_SPCT", conn);
        std::thread th1 = std::thread([&]{
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            while (true)
            {
                queue->pop(msg);
                if(msg.isNull()) {
                    ASSERT_EQ(msg.getOffset(), -1);
                    break;
                }
                else{
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
                    if(msg.getOffset() == 999)
                        notify.set();
                }
            }
        });

        notify.wait();
        client.unsubscribe(HOST, PORT, st, "");
        th1.join();
        std::cout << "total size: " << msg_total << std::endl;
        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_SPCT order by datetimev;\
                            ex = select * from "+case_+"_ex_SPCT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(queue->size(), 0);
        ASSERT_EQ(msg_total, 1000);
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_offsetNegative)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        SPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        int listenport = GetParam();
        dolphindb::PollingClient client = listenport == -1? dolphindb::PollingClient() : dolphindb::PollingClient(listenport);
        auto queue = client.subscribe(HOST, PORT, st, DEFAULT_ACTION_NAME, -1, true, nullptr, false, false, USER, PASSWD);
        dolphindb::Message msg;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_SPCT", conn);
        std::thread th1 = std::thread([&]{
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            while (true)
            {
                queue->pop(msg);
                if(msg.isNull()) {
                    ASSERT_EQ(msg.getOffset(), -1);
                    break;
                }
                else{
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
                    if(msg.getOffset() == 999)
                        notify.set();
                }
            }
        });
        dolphindb::Util::sleep(1000);
        client.unsubscribe(HOST, PORT, st, DEFAULT_ACTION_NAME);
        th1.join();
        std::cout << "total size: " << msg_total << std::endl;
        ASSERT_TRUE(conn.run("eqObj("+case_+"_res_SPCT.rows(), 0)")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(queue->size(), 0);
        ASSERT_EQ(msg_total, 0);
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_filter)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        SPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        int target_rows = conn.run("(exec count(*) from "+case_+"_ex_SPCT where sym = 'a')[0]")->getInt();
        int listenport = GetParam();
        dolphindb::VectorSP filter = dolphindb::Util::createVector(dolphindb::DT_SYMBOL, 1, 1);
        filter->setString(0, "a");

        dolphindb::PollingClient client = listenport == -1? dolphindb::PollingClient() : dolphindb::PollingClient(listenport);
            auto queue = client.subscribe(HOST, PORT, st, DEFAULT_ACTION_NAME, 0, true, filter, false, false, USER, PASSWD);
            dolphindb::Message msg;
            dolphindb::Signal notify;
            dolphindb::Mutex mutex;
            dolphindb::AutoFitTableAppender appender("", case_+"_res_SPCT", conn);
            std::thread th1 = std::thread([&]{
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            while (true)
            {
                queue->pop(msg);
                if(msg.isNull()) {
                    ASSERT_EQ(msg.getOffset(), -1);
                    break;
                }
                else{
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
                    if(msg.getOffset() == 999)
                        notify.set();
                }
            }
        });

        while(msg_total != target_rows)
        {
            dolphindb::Util::sleep(500);
        }
        client.unsubscribe(HOST, PORT, st, DEFAULT_ACTION_NAME);
        th1.join();
        std::cout << "total size: " << msg_total << std::endl;
        ASSERT_TRUE(conn.run("re = exec sym from "+case_+"_res_SPCT; all(re == `a)")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(queue->size(), 0);
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_msgAsTable)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        SPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        int listenport = GetParam();
        dolphindb::PollingClient client = listenport == -1? dolphindb::PollingClient() : dolphindb::PollingClient(listenport);
        auto queue = client.subscribe(HOST, PORT, st, DEFAULT_ACTION_NAME, 0, true, nullptr, true, false, USER, PASSWD);
        dolphindb::Message msg;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_SPCT", conn);
        std::thread th1 = std::thread([&]{
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            while (true)
            {
                queue->pop(msg);
                if(msg.isNull()) {
                    ASSERT_EQ(msg.getOffset(), -1);
                    break;
                }
                else{
                    ASSERT_EQ(msg->getForm(), dolphindb::DF_TABLE);
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
                    msg_total+=msg->rows();
                    int first_offset = msg.getOffset();
                    int last_offset = first_offset + msg->rows() -1;
                    if(last_offset == 999)
                        notify.set();
                }
            }
        });

        notify.wait();
        client.unsubscribe(HOST, PORT, st, DEFAULT_ACTION_NAME);
        th1.join();
        std::cout << "total size: " << msg_total << std::endl;
        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_SPCT order by datetimev;\
                            ex = select * from "+case_+"_ex_SPCT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(queue->size(), 0);
        ASSERT_TRUE(msg_total == 1000);
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_allowExists)
    {
        GTEST_SKIP() << "server还没有修复allowExists的问题, jira: https://dolphindb1.atlassian.net/browse/D20-14283";
        std::string case_=getCaseName();
        int listenport = GetParam();
        dolphindb::PollingClient client = listenport == -1? dolphindb::PollingClient() : dolphindb::PollingClient(listenport);
        std::string st = case_;
        SPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0, msg_total2 = 0;
        auto queue = client.subscribe(HOST, PORT, st, DEFAULT_ACTION_NAME, 0, false, nullptr, false, true, USER, PASSWD);
        auto queue2 = client.subscribe(HOST, PORT, st, DEFAULT_ACTION_NAME, 0, false, nullptr, false, true, USER, PASSWD);
        dolphindb::Message msg;
        dolphindb::Message msg2;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        std::thread th1 = std::thread([&]
                            {
        while (true)
        {
            queue->pop(msg);
            queue2->pop(msg2);
            // std::cout<<msg->getString()<<std::endl;
            if(!msg.isNull()) { msg_total+=1;}
            else if(!msg2.isNull()) { msg_total2+=1;}
            else if(msg.isNull() && msg2.isNull()){break;}
            if (msg.getOffset() == 999 && msg2.getOffset() == 999)
                notify.set();
        }});

        notify.wait();
        client.unsubscribe(HOST, PORT, st, DEFAULT_ACTION_NAME);
        th1.join();
        std::cout << "total size: " << msg_total << std::endl;
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(queue->size(), 0);
        ASSERT_EQ(queue2->size(), 0);
        ASSERT_EQ(msg_total, 1000);
        ASSERT_EQ(msg_total2, 1000);
        client.exit();
        ASSERT_TRUE(client.isExit());
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_resub_false)
    {
        int msg_total = 0;
        int listenport = GetParam();
        dolphindb::PollingClient client = listenport == -1? dolphindb::PollingClient() : dolphindb::PollingClient(listenport);
        ASSERT_ANY_THROW(auto queue = client.subscribe(HOST, PORT, "st", DEFAULT_ACTION_NAME, 0, false, nullptr, false, false, USER, PASSWD));
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_resub_true)
    {
        int listenport = GetParam();
        std::cout << "current listenport is " << listenport << std::endl;
        dolphindb::PollingClient client = listenport == -1? dolphindb::PollingClient() : dolphindb::PollingClient(listenport);
        auto queue = client.subscribe(HOST, PORT, "nonExistTable", "resubTest", 0, true, nullptr, false, false, USER, PASSWD);
        dolphindb::Util::sleep(1000);
        client.unsubscribe(HOST, PORT, "nonExistTable", "resubTest");
    }

    TEST_P(StreamingPollingClientTester, test_unsubscribe_hostNull)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        SPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        int listenport = GetParam();
        dolphindb::PollingClient client = listenport == -1? dolphindb::PollingClient() : dolphindb::PollingClient(listenport);
        auto queue = client.subscribe(HOST, PORT, st, DEFAULT_ACTION_NAME, 0, true, nullptr, false, false, USER, PASSWD);
        dolphindb::Message msg;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        std::thread th1 = std::thread([&]
                            {
        while (true)
        {
            queue->pop(msg);
            // std::cout<<msg->getString()<<std::endl;
            if(msg.isNull()) {
                    ASSERT_EQ(msg.getOffset(), -1);
                    break;
            }else{
                msg_total+=1;
            }
            if (msg.getOffset() == 999)
                notify.set();
        } });

        notify.wait();
        ASSERT_FALSE(client.unsubscribe("", PORT, st, DEFAULT_ACTION_NAME));

        client.unsubscribe(HOST, PORT, st, DEFAULT_ACTION_NAME);
        th1.join();
        std::cout << "total size: " << msg_total << std::endl;
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(queue->size(), 0);
    }

    TEST_P(StreamingPollingClientTester, test_unsubscribe_portNull)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        SPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        int listenport = GetParam();
        dolphindb::PollingClient client = listenport == -1? dolphindb::PollingClient() : dolphindb::PollingClient(listenport);
        auto queue = client.subscribe(HOST, PORT, st, DEFAULT_ACTION_NAME, 0, true, nullptr, false, false, USER, PASSWD);
        dolphindb::Message msg;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        std::thread th1 = std::thread([&]
                            {
        while (true)
        {
            queue->pop(msg);
            // std::cout<<msg->getString()<<std::endl;
            if(msg.isNull()) {
                    ASSERT_EQ(msg.getOffset(), -1);
                    break;
            }else{
                msg_total+=1;
            }
            if (msg.getOffset() == 999)
                notify.set();
        } });

        notify.wait();
        ASSERT_FALSE(client.unsubscribe(HOST, NULL, st, DEFAULT_ACTION_NAME));

        client.unsubscribe(HOST, PORT, st, DEFAULT_ACTION_NAME);
        th1.join();
        std::cout << "total size: " << msg_total << std::endl;
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(queue->size(), 0);
    }

    TEST_P(StreamingPollingClientTester, test_unsubscribe_tableNameNull)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        SPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        int listenport = GetParam();
        dolphindb::PollingClient client = listenport == -1? dolphindb::PollingClient() : dolphindb::PollingClient(listenport);
        auto queue = client.subscribe(HOST, PORT, st, DEFAULT_ACTION_NAME, 0, true, nullptr, false, false, USER, PASSWD);
        dolphindb::Message msg;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        std::thread th1 = std::thread([&]
                            {
        while (true)
        {
            queue->pop(msg);
            // std::cout<<msg->getString()<<std::endl;
            if(msg.isNull()) {
                    ASSERT_EQ(msg.getOffset(), -1);
                    break;
                }else{msg_total+=1;}
            if (msg.getOffset() == 999)
                notify.set();
        } });

        notify.wait();
        ASSERT_FALSE(client.unsubscribe(HOST, PORT, "", DEFAULT_ACTION_NAME));

        client.unsubscribe(HOST, PORT, st, DEFAULT_ACTION_NAME);
        th1.join();
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(queue->size(), 0);
    }

    TEST_P(StreamingPollingClientTester, test_unsubscribe_actionNameNull)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        SPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        int listenport = GetParam();
        dolphindb::PollingClient client = listenport == -1? dolphindb::PollingClient() : dolphindb::PollingClient(listenport);
        auto queue = client.subscribe(HOST, PORT, st, DEFAULT_ACTION_NAME, 0, true, nullptr, false, false, USER, PASSWD);
        dolphindb::Message msg;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        std::thread th1 = std::thread([&]
                            {
        while (true)
        {
            queue->pop(msg);
            // std::cout<<msg->getString()<<std::endl;
            if(msg.isNull()) {
                    ASSERT_EQ(msg.getOffset(), -1);
                    break;
                }else{msg_total+=1;}
            if (msg.getOffset() == 999)
                notify.set();
        } });

        notify.wait();
        client.unsubscribe(HOST, PORT, st, "");
        dolphindb::TableSP stat = conn.run("select * from getStreamingStat().pubTables where tableName=`"+st);
        ASSERT_EQ(stat->getColumn(0)->getRow(0)->getString(), st);
        ASSERT_EQ(stat->getColumn(2)->getRow(0)->getInt(), 1000);
        ASSERT_EQ(stat->getColumn(3)->getRow(0)->getString(), DEFAULT_ACTION_NAME);
        client.unsubscribe(HOST, PORT, st, DEFAULT_ACTION_NAME);
        th1.join();
        std::cout << "total size: " << msg_total << std::endl;
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(queue->size(), 0);
    }

    TEST_P(StreamingPollingClientTester, test_subscribe_twice)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        SPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        int listenport = GetParam();
        dolphindb::PollingClient client = listenport == -1? dolphindb::PollingClient() : dolphindb::PollingClient(listenport);
        auto queue = client.subscribe(HOST, PORT, st, DEFAULT_ACTION_NAME, 0, false, nullptr, false, false, USER, PASSWD);
        dolphindb::Message msg;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        std::thread th1 = std::thread([&]
                            {
        while (true)
        {
            queue->pop(msg);
            // std::cout<<msg->getString()<<std::endl;
            if(msg.isNull()) {
                    ASSERT_EQ(msg.getOffset(), -1);
                    break;
                }else{msg_total+=1;}
            if (msg.getOffset() == 999)
                notify.set();
        } });
        ASSERT_ANY_THROW(auto queue1 = client.subscribe(HOST, PORT, st, DEFAULT_ACTION_NAME, 0, false));

        notify.wait();
        client.unsubscribe(HOST, PORT, st, DEFAULT_ACTION_NAME);
        th1.join();
        std::cout << "total size: " << msg_total << std::endl;
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(queue->size(), 0);
        ASSERT_EQ(msg_total, 1000);
    }

    TEST_P(StreamingPollingClientTester, test_subscribeUnsubscribe_withAllDataType)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        SPCT::createSharedTableAndReplay_withAllDataType(st);
        int msg_total = 0;
        int listenport = GetParam();
        dolphindb::PollingClient client = listenport == -1? dolphindb::PollingClient() : dolphindb::PollingClient(listenport);
        auto queue = client.subscribe(HOST, PORT, st, DEFAULT_ACTION_NAME, 0, true, nullptr, false, false, USER, PASSWD);
        int index = 0;
        dolphindb::Message msg;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_SPCT", conn);
        std::thread th1 = std::thread([&]{
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            while (true)
            {
                queue->pop(msg);
                if(msg.isNull()) {
                    ASSERT_EQ(msg.getOffset(), -1);
                    break;
                }
                else{
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
                    if(msg.getOffset() == 999)
                        notify.set();
                }
            }
        });
        notify.wait();
        client.unsubscribe(HOST, PORT, st, DEFAULT_ACTION_NAME);
        th1.join();
        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_SPCT order by ind;\
                    ex = select * from "+case_+"_ex_SPCT order by ind;\
                    all(each(eqObj, re.values(), ex.values()))")->getBool());

        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(queue->size(), 0);
        ASSERT_EQ(msg_total, 1000);
    }

    TEST_P(StreamingPollingClientTester, test_subscribeUnsubscribe_arrayVector)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        SPCT::createSharedTableAndReplay_withArrayVector(st);
        int msg_total = 0;
        int index = 0;
        int listenport = GetParam();
        dolphindb::PollingClient client = listenport == -1? dolphindb::PollingClient() : dolphindb::PollingClient(listenport);
        auto queue = client.subscribe(HOST, PORT, st, "arrayVectorTableTest", 0, true, nullptr, false, false, USER, PASSWD);
        dolphindb::Util::sleep(1000);
        dolphindb::Message msg;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_SPCT", conn);
        std::thread th1 = std::thread([&]{
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            while (true)
            {
                queue->pop(msg);
                if(msg.isNull()) {
                    ASSERT_EQ(msg.getOffset(), -1);
                    break;
                }
                else{
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
                    if(msg.getOffset() == 999)
                        notify.set();
                }
            }
        });
        notify.wait();
        client.unsubscribe(HOST, PORT, st, "arrayVectorTableTest");
        th1.join();
        std::cout << "total size: " << msg_total << std::endl;
        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_SPCT order by ts;\
                            ex = select * from "+case_+"_ex_SPCT order by ts;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(msg_total, 1000);
    }

    TEST_P(StreamingPollingClientTester, test_subscribeUnsubscribe_hugetable)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        SPCT::createSharedTableAndReplay(st, 1000000);
        int msg_total = 0;
        int listenport = GetParam();
        dolphindb::PollingClient client = listenport == -1? dolphindb::PollingClient() : dolphindb::PollingClient(listenport);
        auto queue = client.subscribe(HOST, PORT, st, DEFAULT_ACTION_NAME, 0, true, nullptr, false, false, USER, PASSWD);

        dolphindb::Message msg;
        std::thread th1 = std::thread([&]{
            while (true)
            {
                queue->pop(msg);
                if(msg.isNull()) {
                    ASSERT_EQ(msg.getOffset(), -1);
                    break;
                }
                else{
                    // bool succeeded = false;
                    // dolphindb::TableSP tmp = AnyVectorToTable(msg);
                    // while(!succeeded){
                    //     try
                    //     {
                    //         appender.append(tmp);
                    //         succeeded = true;
                    //     }
                    //     catch(const std::exception& e)
                    //     {
                    //         dolphindb::Util::sleep(100);
                    //     }
                    // }
                    msg_total+=1;
                }
            }
        });
        while (msg_total != 1000000)
        {
            dolphindb::Util::sleep(500);
        }
        client.unsubscribe(HOST, PORT, st, DEFAULT_ACTION_NAME);
        th1.join();
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(queue->size(), 0);
        ASSERT_EQ(msg_total, 1000000) << "Streaming msgs may not be recieved absolutely yet";
    }

    TEST_P(StreamingPollingClientTester, test_subscribeUnsubscribe_hugetable_msgAsTable)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        SPCT::createSharedTableAndReplay(st, 10000000);
        int msg_total = 0;
        int listenport = GetParam();
        dolphindb::PollingClient client = listenport == -1? dolphindb::PollingClient() : dolphindb::PollingClient(listenport);
        auto queue = client.subscribe(HOST, PORT, st, DEFAULT_ACTION_NAME, 0, false, nullptr, true, false, USER, PASSWD);

        dolphindb::Message msg;
        dolphindb::Signal notify;
        dolphindb::Mutex mutex;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_SPCT", conn);
        std::thread th1 = std::thread([&]{
            dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
            while (true)
            {
                queue->pop(msg);
                if(msg.isNull()) {
                    ASSERT_EQ(msg.getOffset(), -1);
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
                            dolphindb::Util::sleep(100);
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
        client.unsubscribe(HOST, PORT, st, DEFAULT_ACTION_NAME);
        th1.join();
        std::cout << "total size: " << msg_total << std::endl;
        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_SPCT order by datetimev;\
                            ex = select * from "+case_+"_ex_SPCT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(queue->size(), 0);
        ASSERT_TRUE(msg_total > 0) << "Streaming msgs may not be recieved absolutely yet";
    }

    class StreamingPollingClientTester_realtime : public testing::Test, public ::testing::WithParamInterface<std::tuple<int, std::pair<dolphindb::DATA_TYPE, std::string>>>
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
                "share table(1:0, colName, colType) as "+name+"_res_SPCT;";
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
    dolphindb::DBConnection StreamingPollingClientTester_realtime::conn(false, false);

    INSTANTIATE_TEST_SUITE_P(basicTypes, StreamingPollingClientTester_realtime, testing::Combine(
        testing::Values(0, rand() % 1000 + 13000),
        testing::ValuesIn(StreamingPollingClientTester_realtime::getData())));
    INSTANTIATE_TEST_SUITE_P(arrayVectorTypes, StreamingPollingClientTester_realtime, testing::Combine(
        testing::Values(0, rand() % 1000 + 13000),
        testing::ValuesIn(StreamingPollingClientTester_realtime::getAVData())));
    TEST_P(StreamingPollingClientTester_realtime, test_realtime)
    {
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
        std::string case_=getCaseName();
        const std::string st = case_;
        createST(conn, st, typeString);
        int msg_total = 0;
        dolphindb::Message msg;
        dolphindb::AutoFitTableAppender appender("", case_+"_res_SPCT", conn);
        int listenport = std::get<0>(GetParam());
        dolphindb::PollingClient client = listenport == -1? dolphindb::PollingClient() : dolphindb::PollingClient(listenport);
        std::string dataScript = std::get<1>(GetParam()).second;
        std::thread th = std::thread([&]() {
            insertData(conn, st, dataScript);
        });
        auto queue = client.subscribe(HOST, PORT, st, DEFAULT_ACTION_NAME, 0, true, nullptr, false, false, USER, PASSWD);
        th.join();
        std::thread th1 = std::thread([&]{
            while (true)
            {
                queue->pop(msg);
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
                if (msg.getOffset() == 999) break;
            }
        });
        th1.join();
        std::cout << "total size:" << msg_total << std::endl;
        client.unsubscribe(HOST, PORT, st, DEFAULT_ACTION_NAME);
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("re = select * from "+case_+"_res_SPCT order by ts;\
                            ex = select * from "+st+" order by ts;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
        ASSERT_EQ(msg_total, 1000);
    }


    TEST_P(StreamingPollingClientTester, test_resub_true_with_resubscribeTimeout)
    {
        std::string case_=getCaseName();
        std::string st = case_;
        SPCT::createSharedTableAndReplay(st, 1000);
        int listenport = GetParam();
        dolphindb::PollingClient client = listenport == -1? dolphindb::PollingClient() : dolphindb::PollingClient(listenport);
        dolphindb::PollingClient client1 = listenport == -1? dolphindb::PollingClient() : dolphindb::PollingClient(listenport);
        unsigned int resubscribeTimeout = 500;
        client.subscribe(HOST, PORT, st, DEFAULT_ACTION_NAME, 0, true, nullptr, false, false, USER, PASSWD);
        auto queue1 = client1.subscribe(HOST, PORT, st, DEFAULT_ACTION_NAME, 0, true, nullptr, false, false, USER, PASSWD, nullptr, {}, 100, resubscribeTimeout);
        dolphindb::Util::sleep(resubscribeTimeout+1000);
        client.unsubscribe(HOST, PORT, st, DEFAULT_ACTION_NAME);
        client1.exit();
        dolphindb::Util::sleep(1000);
        ASSERT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+st+") ==0")->getBool());
    }

    TEST_F(StreamingPollingClientTester, test_new_subscribe_resub_true)
    {
        std::string case_=getCaseName();
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
        dolphindb::PollingClient client = dolphindb::PollingClient(*scfg);
        auto queue = client.subscribe(HOST, PORT, st, "resubTest", 0, true, nullptr, false, false, USER, PASSWD);
        dolphindb::Util::sleep(2000);
        conn.run("subinfo = (exec subscriber from getStreamingStat().pubTables where tableName=`"+st+")[0].split(':');"
                    "subClient = subinfo[0];"
                    "subPort=int(subinfo[1]);go;"
                    "stopPublishTable(subClient, subPort, `"+st+", `resubTest)");
        dolphindb::Util::sleep(1000);
        conn.run("tableInsert("+st+", `sym2, 2)");
        dolphindb::Util::sleep(1000);
        client.unsubscribe(HOST, PORT, st, "resubTest");
        dolphindb::Message msg;
        std::vector<dolphindb::Message> msgs;
        while (true)
        {
            queue->pop(msg);
            if(msg.isNull()) {
                    ASSERT_EQ(msg.getOffset(), -1);
                    break;
                }
            else{msgs.push_back(msg);}
        };
        ASSERT_EQ(msgs.size(), 2);
        ASSERT_EQ(states.size(), 4);
        ASSERT_EQ(states[0],dolphindb::SubscribeState::Connected);
        ASSERT_EQ(states[1],dolphindb::SubscribeState::Disconnected);
        ASSERT_EQ(states[2],dolphindb::SubscribeState::Resubscribing);
        ASSERT_EQ(states[3],dolphindb::SubscribeState::Connected);
    }

    TEST_F(StreamingPollingClientTester, test_new_subscribe_resub_false)
    {
        std::string case_=getCaseName();
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

        dolphindb::PollingClient client = dolphindb::PollingClient(*scfg);
        auto queue = client.subscribe(HOST, PORT, st, "resubTest", 0, false, nullptr, false, false, USER, PASSWD);
        dolphindb::Util::sleep(2000);
        conn.run("subinfo = (exec subscriber from getStreamingStat().pubTables where tableName=`"+st+")[0].split(':');"
                    "subClient = subinfo[0];"
                    "subPort=int(subinfo[1]);go;"
                    "stopPublishTable(subClient, subPort, `"+st+", `resubTest)");
        dolphindb::Util::sleep(1000);
        conn.run("tableInsert("+st+", `sym2, 2)");
        dolphindb::Util::sleep(1000);
        client.unsubscribe(HOST, PORT, st, "resubTest");
        client.exit();
        ASSERT_TRUE(client.isExit());

        dolphindb::Message msg;
        std::vector<dolphindb::Message> msgs;
        while (true)
        {
            queue->pop(msg);
            if(msg.isNull()) {
                    ASSERT_EQ(msg.getOffset(), -1);
                    break;
                }
            else{msgs.push_back(msg);}
        };
        ASSERT_EQ(msgs.size(), 1);
        ASSERT_EQ(states.size(), 2);
        ASSERT_EQ(states[0],dolphindb::SubscribeState::Connected);
        ASSERT_EQ(states[1],dolphindb::SubscribeState::Disconnected);
    }
}