#include <gtest/gtest.h>
#include "config.h"
#include "Streaming.h"

class StreamingDeserilizerTester : public testing::Test
{
    public:
        static dolphindb::StreamingClientConfig scfg;
        static dolphindb::DBConnectionSP pConn;

        // Suite
        static void SetUpTestSuite()
        {
            bool ret = pConn->connect(HOST, PORT, USER, PASSWD);
            if (!ret)
            {
                std::cout << "Failed to connect to the server" << std::endl;
            }
            else
            {
                std::cout << "connect to " + HOST + ":" + std::to_string(PORT) << std::endl;
            }
            
            scfg.protocol = dolphindb::TransportationProtocol::UDP;
        }
        static void TearDownTestSuite()
        {
            pConn->close();
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

dolphindb::DBConnectionSP StreamingDeserilizerTester::pConn(new dolphindb::DBConnection());


dolphindb::StreamingClientConfig StreamingDeserilizerTester::scfg;
class StreamingDeserilizerTester_basic : public StreamingDeserilizerTester, public ::testing::WithParamInterface<int>
{
};

dolphindb::StreamDeserializerSP createStreamDeserializer(dolphindb::DBConnection &conn, const std::string &st)
{
    std::string case_=getCaseName();
    std::string script = "\
            share streamTable(1:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE]) as "+st+"\n\
            go\n\
            setStreamTableFilterColumn("+st+", `sym)";
    conn.run(script);

    std::string replayScript = "n = 1000;\
            table1_SDT = table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);\
            share table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]) as "+case_+"_res1_SDT;\
            table2_SDT = table(1:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);\
            share table(1:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]) as "+case_+"_res2_SDT;\
            go;\
            tableInsert(table1_SDT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));\
            tableInsert(table2_SDT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));\
            share table1_SDT as "+case_+"_ex1_SDT;\
            share table2_SDT as "+case_+"_ex2_SDT;\
            go;\
            d = dict(['msg1','msg2'], [table1_SDT, table2_SDT]);\
            replay(inputTables=d, outputTables=`"+st+", dateColumn=`timestampv, timeColumn=`timestampv)";
    conn.run(replayScript);

    dolphindb::DictionarySP t1s = conn.run("schema(table1_SDT)");
    dolphindb::DictionarySP t2s = conn.run("schema(table2_SDT)");
    std::unordered_map<std::string, dolphindb::DictionarySP> sym2schema;
    sym2schema["msg1"] = t1s;
    sym2schema["msg2"] = t2s;
    dolphindb::StreamDeserializerSP sdsp = new dolphindb::StreamDeserializer(sym2schema);
    return sdsp;
}

dolphindb::StreamDeserializerSP createStreamDeserializer_2(dolphindb::DBConnection &conn, const std::string &st)
{
    std::string case_=getCaseName();
    std::string script = "\
            share streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE]) as "+st+"\n\
            go\n\
            setStreamTableFilterColumn("+st+", `sym)";
    conn.run(script);

    std::string replayScript = "n = 1000;\
            table1_SDT = table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);\
            share table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]) as "+case_+"_res1_SDT;\
            table2_SDT = table(1:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);\
            share table(1:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]) as "+case_+"_res2_SDT;\
            go;\
            tableInsert(table1_SDT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));\
            tableInsert(table2_SDT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));\
            share table1_SDT as "+case_+"_ex1_SDT;\
            share table2_SDT as "+case_+"_ex2_SDT;\
            go;\
            d = dict(['msg1','msg2'], [table1_SDT, table2_SDT]);\
            replay(inputTables=d, outputTables=`"+st+", dateColumn=`timestampv, timeColumn=`timestampv)";
    conn.run(replayScript);

    dolphindb::TableSP t1 = conn.run("table1_SDT");
    dolphindb::TableSP t2 = conn.run("table2_SDT");
    std::vector<dolphindb::DATA_TYPE> t1s;
    std::vector<dolphindb::DATA_TYPE> t2s;
    for (auto i = 0; i < t1->columns(); i++)
        t1s.emplace_back(t1->getColumnType(i));
    for (auto i = 0; i < t2->columns(); i++)
        t2s.emplace_back(t2->getColumnType(i));
    std::unordered_map<std::string, std::vector<dolphindb::DATA_TYPE>> sym2schema;
    sym2schema["msg1"] = t1s;
    sym2schema["msg2"] = t2s;
    dolphindb::StreamDeserializerSP sdsp = new dolphindb::StreamDeserializer(sym2schema);
    return sdsp;
}

dolphindb::StreamDeserializerSP createStreamDeserializer_3(dolphindb::DBConnection &conn, const std::string &st)
{
    std::string case_=getCaseName();
    std::string script = "\
            share streamTable(1:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE]) as "+st+"\n\
            go\n\
            setStreamTableFilterColumn("+st+", `sym)";
    conn.run(script);

    std::string replayScript = "n = 1000;\
            table1_SDT = table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);\
            share table1_SDT as "+case_+"_res1_SDT;\
            table2_SDT = table(1:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);\
            share table2_SDT as "+case_+"_res2_SDT;\
            go;\
            tableInsert(table1_SDT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));\
            tableInsert(table2_SDT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));\
            share table1_SDT as "+case_+"_ex1_SDT;\
            share table2_SDT as "+case_+"_ex2_SDT;\
            go;\
            dbpath=\"dfs://test_"+st+"\";\
            if(existsDatabase(dbpath)){dropDatabase(dbpath)};\
            db=database(dbpath, VALUE, `a`b`c);\
            db.createPartitionedTable(table2_SDT,`table2_SDT,`sym).append!(table2_SDT);\
            d = dict(['msg1','msg2'], [table1_SDT, table2_SDT]);\
            replay(inputTables=d, outputTables=`"+st+", dateColumn=`timestampv, timeColumn=`timestampv)";
    conn.run(replayScript);

    std::unordered_map<std::string, std::pair<std::string, std::string>> sym2schema;
    sym2schema["msg1"] = {"", "table1_SDT"};
    sym2schema["msg2"] = {"dfs://test_"+st, "table2_SDT"};
    dolphindb::StreamDeserializerSP sdsp = new dolphindb::StreamDeserializer(sym2schema, &conn);
    return sdsp;
}

dolphindb::StreamDeserializerSP createStreamDeserializer_withallTypes(dolphindb::DBConnection &conn, const std::string &st, const dolphindb::DATA_TYPE &dataType, const std::string &vecVal)
{
    std::string case_=getCaseName();
    std::string typeString = dolphindb::Util::getDataTypeString(dataType);
    if (typeString.compare(0, 9, "DECIMAL32") == 0)
        typeString = typeString.substr(0, 9) + "(8)";
    else if (typeString.compare(0, 9, "DECIMAL64") == 0)
        typeString = typeString.substr(0, 9) + "(15)";
    else if (typeString.compare(0, 10, "DECIMAL128") == 0)
        typeString = typeString.substr(0, 10) + "(25)";
    std::cout << "test type: " << typeString << std::endl;
    std::string script = "\
            share streamTable(1:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB," + typeString + "]) as "+st+"\n\
            go\n\
            setStreamTableFilterColumn("+st+", `sym)";
    conn.run(script);

    std::string replayScript = "n = 1000;\
            table1_SDT = table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, " + typeString + ", DOUBLE]);\
            share table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, " + typeString + ", DOUBLE]) as "+case_+"_res1_SDT;\
            table2_SDT = table(1:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, " + typeString + "]);\
            share table(1:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, " + typeString + "]) as "+case_+"_res2_SDT;\
            go;\
            tableInsert(table1_SDT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), take(array(" + typeString + ").append!(" + vecVal + "),n), rand(100.00,n));\
            tableInsert(table2_SDT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), take(array(" + typeString + ").append!(" + vecVal + "),n));\
            share table1_SDT as "+case_+"_ex1_SDT;\
            share table2_SDT as "+case_+"_ex2_SDT;\
            go;\
            d = dict(['msg1','msg2'], [table1_SDT, table2_SDT]);\
            replay(inputTables=d, outputTables=`"+st+", dateColumn=`timestampv, timeColumn=`timestampv)";
    conn.run(replayScript);

    dolphindb::DictionarySP t1s = conn.run("schema(table1_SDT)");
    dolphindb::DictionarySP t2s = conn.run("schema(table2_SDT)");
    std::unordered_map<std::string, dolphindb::DictionarySP> sym2schema;
    sym2schema["msg1"] = t1s;
    sym2schema["msg2"] = t2s;
    dolphindb::StreamDeserializerSP sdsp = new dolphindb::StreamDeserializer(sym2schema);
    return sdsp;
}

dolphindb::StreamDeserializerSP createStreamDeserializer_witharrayVector(dolphindb::DBConnection &conn, const std::string &st, const dolphindb::DATA_TYPE &dataType, const std::string &vecVal)
{
    std::string case_=getCaseName();
    std::string typeString = dolphindb::Util::getDataTypeString(dataType);
    if (typeString.compare(0, 9, "DECIMAL32") == 0)
        typeString = typeString.substr(0, 9) + "(8)[]";
    else if (typeString.compare(0, 9, "DECIMAL64") == 0)
        typeString = typeString.substr(0, 9) + "(15)[]";
    else if (typeString.compare(0, 10, "DECIMAL128") == 0)
        typeString = typeString.substr(0, 10) + "(25)[]";
    std::cout << "test type: " << typeString << std::endl;
    std::string script = "\
            share streamTable(1:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB," + typeString + "]) as "+st+"\n\
            go\n\
            setStreamTableFilterColumn("+st+", `sym)";
    conn.run(script);

    std::string replayScript = "n = 1000;\
            table1_SDT = table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, " + typeString + ", DOUBLE]);\
            share table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, " + typeString + ", DOUBLE]) as "+case_+"_res1_SDT;\
            table2_SDT = table(1:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, " + typeString + "]);\
            share table(1:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, " + typeString + "]) as "+case_+"_res2_SDT;\
            go;\
            tableInsert(table1_SDT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), take(array(" + typeString + ").append!([" + vecVal + "]),n), rand(100.00,n));\
            tableInsert(table2_SDT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), take(array(" + typeString + ").append!([" + vecVal + "]),n));\
            share table1_SDT as "+case_+"_ex1_SDT;\
            share table2_SDT as "+case_+"_ex2_SDT;\
            go;\
            d = dict(['msg1','msg2'], [table1_SDT, table2_SDT]);\
            replay(inputTables=d, outputTables=`"+st+", dateColumn=`timestampv, timeColumn=`timestampv)";
    conn.run(replayScript);

    dolphindb::DictionarySP t1s = conn.run("schema(table1_SDT)");
    dolphindb::DictionarySP t2s = conn.run("schema(table2_SDT)");
    std::unordered_map<std::string, dolphindb::DictionarySP> sym2schema;
    sym2schema["msg1"] = t1s;
    sym2schema["msg2"] = t2s;
    dolphindb::StreamDeserializerSP sdsp = new dolphindb::StreamDeserializer(sym2schema);
    return sdsp;
}

INSTANTIATE_TEST_SUITE_P(, StreamingDeserilizerTester_basic, testing::Values(0, rand() % 1000 + 13000));
TEST_P(StreamingDeserilizerTester_basic, test_Threadclient_onehandler_subscribeWithstreamDeserilizer)
{
    std::string case_=getCaseName();
    int msg1_total = 0, msg2_total = 0;
    int listenport = GetParam();

    dolphindb::Signal notify;
    dolphindb::Mutex mutex;
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer(*pConn, st);

    dolphindb::AutoFitTableAppender appender1("", case_+"_res1_SDT", *pConn);
    dolphindb::AutoFitTableAppender appender2("", case_+"_res2_SDT", *pConn);

    auto onehandler = [&](dolphindb::Message msg)
    {
        const std::string &symbol = msg.getSymbol();
        dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
        bool succeeded = false;
        dolphindb::TableSP tmp = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                if (symbol == "msg1")
                {
                    appender1.append(tmp);
                    msg1_total += 1;
                }
                else if (symbol == "msg2")
                {
                    appender2.append(tmp);
                    msg2_total += 1;
                }
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                dolphindb::Util::sleep(100);
            }
        }

        if (msg.getOffset() == 1999)
        {
            notify.set();
        }
    };

    dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
    auto thread1 = threadedClient.subscribe(HOST, PORT, onehandler, st, "test_SD", 0, true, nullptr, false, false, USER, PASSWD, sdsp);
    notify.wait();

    threadedClient.unsubscribe(HOST, PORT, st, "test_SD");
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res1_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex1_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res2_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex2_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_EQ(msg1_total, 1000);
    ASSERT_EQ(msg2_total, 1000);
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadclient_batchhandler_subscribeWithstreamDeserilizer)
{
    std::string case_=getCaseName();
    int msg1_total = 0, msg2_total = 0;
    int listenport = GetParam();

    dolphindb::Signal notify;
    dolphindb::Mutex mutex;
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer(*pConn, st);

    dolphindb::AutoFitTableAppender appender1("", case_+"_res1_SDT", *pConn);
    dolphindb::AutoFitTableAppender appender2("", case_+"_res2_SDT", *pConn);

    auto batchhandler = [&](std::vector<dolphindb::Message> msgs)
    {
        dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
        for (auto &msg : msgs)
        {
            const std::string &symbol = msg.getSymbol();
            bool succeeded = false;
            dolphindb::TableSP tmp = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    if (symbol == "msg1")
                    {
                        appender1.append(tmp);
                        msg1_total += 1;
                    }
                    else if (symbol == "msg2")
                    {
                        appender2.append(tmp);
                        msg2_total += 1;
                    }
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    dolphindb::Util::sleep(100);
                }
            }
            if (msg.getOffset() == 1999)
            {
                notify.set();
            }
        }
    };
    dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
    auto thread2 = threadedClient.subscribe(HOST, PORT, batchhandler, st, "mutiSchemaBatch", 0, true, nullptr, true, 1, 1.0, false, USER, PASSWD, sdsp);
    notify.wait();

    threadedClient.unsubscribe(HOST, PORT, st, "mutiSchemaBatch");
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res1_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex1_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res2_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex2_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_EQ(msg1_total, 1000);
    ASSERT_EQ(msg2_total, 1000);
}

TEST_F(StreamingDeserilizerTester_basic, test_UDPThreadclient_onehandler_subscribeWithstreamDeserilizer_should_serial)
{
    #ifdef _WIN32
        GTEST_SKIP() << "not support udp on Windows yet.";
    #endif
    std::string case_=getCaseName();
    int msg1_total = 0, msg2_total = 0;

    dolphindb::Signal notify;
    dolphindb::Mutex mutex;
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer(*pConn, st);
    dolphindb::AutoFitTableAppender appender1("", case_+"_res1_SDT", *pConn);
    dolphindb::AutoFitTableAppender appender2("", case_+"_res2_SDT", *pConn);

    auto onehandler = [&](dolphindb::Message msg)
    {
        const std::string &symbol = msg.getSymbol();
        dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
        bool succeeded = false;
        dolphindb::TableSP tmp = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                if (symbol == "msg1")
                {
                    appender1.append(tmp);
                    msg1_total += 1;
                }
                else if (symbol == "msg2")
                {
                    appender2.append(tmp);
                    msg2_total += 1;
                }
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                dolphindb::Util::sleep(100);
            }
        }

        if (msg.getOffset() == 1999)
        {
            notify.set();
        }
    };
    dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(scfg);
    auto thread1 = threadedClient.subscribe(HOST, PORT, onehandler, st, "test_SD", 0, true, nullptr, false, false, USER, PASSWD, sdsp);
    notify.wait();
    threadedClient.unsubscribe(HOST, PORT, st, "test_SD");
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res1_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex1_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res2_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex2_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_EQ(msg1_total, 1000);
    ASSERT_EQ(msg2_total, 1000);
}

TEST_P(StreamingDeserilizerTester_basic, test_Pollingclient_subscribeWithstreamDeserilizer)
{
    std::string case_=getCaseName();
    int msg1_total = 0, msg2_total = 0;
    int listenport = GetParam();

    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer(*pConn, st);

    dolphindb::AutoFitTableAppender appender1("", case_+"_res1_SDT", *pConn);
    dolphindb::AutoFitTableAppender appender2("", case_+"_res2_SDT", *pConn);

    dolphindb::PollingClient client = dolphindb::PollingClient(listenport);
    auto queue = client.subscribe(HOST, PORT, st, "actionTest", 0, true, nullptr, false, false, USER, PASSWD, sdsp);

    dolphindb::Message msg;
    dolphindb::Signal notify;
    dolphindb::Mutex mutex;

    std::thread th1 = std::thread([&]{
    dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
    while (true)
    {
        queue->pop(msg);
        if(msg.isNull()) {break;}
        else{
            const std::string &symbol = msg.getSymbol();
            bool succeeded = false;
            dolphindb::TableSP tmp = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    if (symbol == "msg1")
                    {
                        appender1.append(tmp);
                        msg1_total += 1;
                    }
                    else if (symbol == "msg2")
                    {
                        appender2.append(tmp);
                        msg2_total += 1;
                    }
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    dolphindb::Util::sleep(100);
                }
            }
            if (msg.getOffset() == 1999)
            {
                notify.set();
            }
        }
    } });

    notify.wait();
    client.unsubscribe(HOST, PORT, st, "actionTest");
    th1.join();
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res1_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex1_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res2_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex2_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_EQ(msg1_total, 1000);
    ASSERT_EQ(msg2_total, 1000);
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadpooledclient_threadCount_1_subscribeWithstreamDeserilizer)
{
    std::string case_=getCaseName();
    int msg1_total = 0, msg2_total = 0;
    int listenport = GetParam();
    dolphindb::Signal notify;
    dolphindb::Mutex mutex;
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer(*pConn, st);

    dolphindb::AutoFitTableAppender appender1("", case_+"_res1_SDT", *pConn);
    dolphindb::AutoFitTableAppender appender2("", case_+"_res2_SDT", *pConn);

    auto onehandler = [&](dolphindb::Message msg)
    {
        const std::string &symbol = msg.getSymbol();
        dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
        bool succeeded = false;
        dolphindb::TableSP tmp = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                if (symbol == "msg1")
                {
                    appender1.append(tmp);
                    msg1_total += 1;
                }
                else if (symbol == "msg2")
                {
                    appender2.append(tmp);
                    msg2_total += 1;
                }
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                dolphindb::Util::sleep(100);
            }
        }

        if (msg.getOffset() == 1999)
        {
            notify.set();
        }
    };

    dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, 1);
    auto threadVec = client.subscribe(HOST, PORT, onehandler, st, "test_SD", 0, true, nullptr, false, false, USER, PASSWD, sdsp);
    ASSERT_EQ(threadVec.size(), 1);
    notify.wait();

    client.unsubscribe(HOST, PORT, st, "test_SD");
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res1_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex1_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res2_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex2_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_EQ(msg1_total, 1000);
    ASSERT_EQ(msg2_total, 1000);
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadpooledclient_threadCount_2_subscribeWithstreamDeserilizer)
{
    std::string case_=getCaseName();
    int msg1_total = 0;
    int msg2_total = 0;
    int listenport = GetParam();
    std::cout << "current listenport is " << listenport << std::endl;

    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer(*pConn, st);

    dolphindb::AutoFitTableAppender appender1("", case_+"_res1_SDT", *pConn);
    dolphindb::AutoFitTableAppender appender2("", case_+"_res2_SDT", *pConn);

    dolphindb::Signal notify;
    dolphindb::Mutex mutex;
    auto onehandler = [&](dolphindb::Message msg)
    {
        const std::string &symbol = msg.getSymbol();
        dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
        bool succeeded = false;
        dolphindb::TableSP tmp = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                if (symbol == "msg1")
                {
                    appender1.append(tmp);
                    msg1_total += 1;
                }
                else if (symbol == "msg2")
                {
                    appender2.append(tmp);
                    msg2_total += 1;
                }
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                dolphindb::Util::sleep(100);
            }
        }

        if (msg.getOffset() == 1999)
        {
            notify.set();
        }
    };

    dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, 2);
    auto threadVec = client.subscribe(HOST, PORT, onehandler, st, "test_SD", 0, true, nullptr, false, false, USER, PASSWD, sdsp);
    ASSERT_EQ(threadVec.size(), 2);
    notify.wait();

    client.unsubscribe(HOST, PORT, st, "test_SD");
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res1_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex1_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res2_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex2_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_EQ(msg1_total, 1000);
    ASSERT_EQ(msg2_total, 1000);
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadclient_with_msgAsTable_True)
{
    std::string case_=getCaseName();
    int listenport = GetParam();
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer(*pConn, st);

    auto onehandler = [&](dolphindb::Message msg)
    {
        const std::string &symbol = msg.getSymbol();
        // std::cout << symbol << std::endl;
    };

    dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
    ASSERT_ANY_THROW(auto thread1 = threadedClient.subscribe(HOST, PORT, onehandler, st, "test_SD", 0, true, nullptr, true, false, USER, PASSWD, sdsp));
}


TEST_F(StreamingDeserilizerTester_basic, test_UDPThreadclient_with_msgAsTable_True_should_serial)
{
    #ifdef _WIN32
        GTEST_SKIP() << "not support udp on Windows yet.";
    #endif
    std::string case_=getCaseName();
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer(*pConn, st);

    auto onehandler = [&](dolphindb::Message msg)
    {
        const std::string &symbol = msg.getSymbol();
        // std::cout << symbol << std::endl;
    };
    dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(scfg);
    ASSERT_ANY_THROW(auto thread1 = threadedClient.subscribe(HOST, PORT, onehandler, st, "test_SD", 0, true, nullptr, true, false, USER, PASSWD, sdsp));
}


TEST_P(StreamingDeserilizerTester_basic, test_Pollingclient_with_msgAsTable_True)
{
    std::string case_=getCaseName();
    int listenport = GetParam();
    std::cout << "current listenport is " << listenport << std::endl;

    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer(*pConn, st);

    auto onehandler = [&](dolphindb::Message msg)
    {
        const std::string &symbol = msg.getSymbol();
    };
    dolphindb::PollingClient client = dolphindb::PollingClient(listenport);
    ASSERT_ANY_THROW(client.subscribe(HOST, PORT, st, "actionTest", 0, true, nullptr, true, false, USER, PASSWD, sdsp));
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadpooledclient_with_msgAsTable_True)
{
    std::string case_=getCaseName();
    int listenport = GetParam();
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer(*pConn, st);
    auto onehandler = [&](dolphindb::Message msg)
    {
        const std::string &symbol = msg.getSymbol();
    };
    dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, 1);
    ASSERT_ANY_THROW(auto threadVec = client.subscribe(HOST, PORT, onehandler, st, "test_SD", 0, true, nullptr, true, false, USER, PASSWD, sdsp));
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadclient_onehandler_subscribeWithstreamDeserilizer_2)
{
    std::string case_=getCaseName();
    int msg1_total = 0, msg2_total = 0;
    int listenport = GetParam();
    dolphindb::Signal notify;
    dolphindb::Mutex mutex;
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer_2(*pConn, st);
    dolphindb::AutoFitTableAppender appender1("", case_+"_res1_SDT", *pConn);
    dolphindb::AutoFitTableAppender appender2("", case_+"_res2_SDT", *pConn);
    auto onehandler = [&](dolphindb::Message msg)
    {
        const std::string &symbol = msg.getSymbol();
        dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
        bool succeeded = false;
        dolphindb::TableSP tmp = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                if (symbol == "msg1")
                {
                    appender1.append(tmp);
                    msg1_total += 1;
                }
                else if (symbol == "msg2")
                {
                    appender2.append(tmp);
                    msg2_total += 1;
                }
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                dolphindb::Util::sleep(100);
            }
        }

        if (msg.getOffset() == 1999)
        {
            notify.set();
        }
    };
    dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
    auto thread1 = threadedClient.subscribe(HOST, PORT, onehandler, st, "test_SD", 0, true, nullptr, false, false, USER, PASSWD, sdsp);
    notify.wait();
    threadedClient.unsubscribe(HOST, PORT, st, "test_SD");
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res1_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex1_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res2_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex2_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_EQ(msg1_total, 1000);
    ASSERT_EQ(msg2_total, 1000);
}

TEST_F(StreamingDeserilizerTester_basic, test_UDPThreadclient_onehandler_subscribeWithstreamDeserilizer_2_should_serial)
{
    #ifdef _WIN32
        GTEST_SKIP() << "not support udp on Windows yet.";
    #endif
    std::string case_=getCaseName();
    int msg1_total = 0, msg2_total = 0;
    dolphindb::Signal notify;
    dolphindb::Mutex mutex;
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer_2(*pConn, st);
    dolphindb::AutoFitTableAppender appender1("", case_+"_res1_SDT", *pConn);
    dolphindb::AutoFitTableAppender appender2("", case_+"_res2_SDT", *pConn);
    auto onehandler = [&](dolphindb::Message msg)
    {
        const std::string &symbol = msg.getSymbol();
        dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
        bool succeeded = false;
        dolphindb::TableSP tmp = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                if (symbol == "msg1")
                {
                    appender1.append(tmp);
                    msg1_total += 1;
                }
                else if (symbol == "msg2")
                {
                    appender2.append(tmp);
                    msg2_total += 1;
                }
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                dolphindb::Util::sleep(100);
            }
        }
        if (msg.getOffset() == 1999)
        {
            notify.set();
        }
    };
    dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(scfg);
    auto thread1 = threadedClient.subscribe(HOST, PORT, onehandler, st, "test_SD", 0, true, nullptr, false, false, USER, PASSWD, sdsp);
    notify.wait();

    threadedClient.unsubscribe(HOST, PORT, st, "test_SD");
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res1_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex1_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res2_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex2_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_EQ(msg1_total, 1000);
    ASSERT_EQ(msg2_total, 1000);
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadclient_batchhandler_subscribeWithstreamDeserilizer_2)
{
    std::string case_=getCaseName();
    int msg1_total = 0, msg2_total = 0;
    int listenport = GetParam();
    dolphindb::Signal notify;
    dolphindb::Mutex mutex;
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer_2(*pConn, st);
    dolphindb::AutoFitTableAppender appender1("", case_+"_res1_SDT", *pConn);
    dolphindb::AutoFitTableAppender appender2("", case_+"_res2_SDT", *pConn);
    auto batchhandler = [&](std::vector<dolphindb::Message> msgs)
    {
        dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
        for (auto &msg : msgs)
        {
            const std::string &symbol = msg.getSymbol();
            bool succeeded = false;
            dolphindb::TableSP tmp = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    if (symbol == "msg1")
                    {
                        appender1.append(tmp);
                        msg1_total += 1;
                    }
                    else if (symbol == "msg2")
                    {
                        appender2.append(tmp);
                        msg2_total += 1;
                    }
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    dolphindb::Util::sleep(100);
                }
            }

            if (msg.getOffset() == 1999)
            {
                notify.set();
            }
        }
    };
    dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
    auto thread2 = threadedClient.subscribe(HOST, PORT, batchhandler, st, "mutiSchemaBatch", 0, true, nullptr, true, 1, 1.0, false, USER, PASSWD, sdsp);
    notify.wait();
    threadedClient.unsubscribe(HOST, PORT, st, "mutiSchemaBatch");
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res1_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex1_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res2_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex2_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_EQ(msg1_total, 1000);
    ASSERT_EQ(msg2_total, 1000);
}

TEST_P(StreamingDeserilizerTester_basic, test_Pollingclient_subscribeWithstreamDeserilizer_2)
{
    std::string case_=getCaseName();
    int msg1_total = 0, msg2_total = 0;
    int listenport = GetParam();
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer_2(*pConn, st);
    dolphindb::AutoFitTableAppender appender1("", case_+"_res1_SDT", *pConn);
    dolphindb::AutoFitTableAppender appender2("", case_+"_res2_SDT", *pConn);
    dolphindb::PollingClient client = dolphindb::PollingClient(listenport);
    auto queue = client.subscribe(HOST, PORT, st, "actionTest", 0, true, nullptr, false, false, USER, PASSWD, sdsp);
    dolphindb::Message msg;
    dolphindb::Signal notify;
    dolphindb::Mutex mutex;
    std::thread th1 = std::thread([&]{
    dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
    while (true)
    {
        queue->pop(msg);
        if(msg.isNull()) {break;}
        else{
            const std::string &symbol = msg.getSymbol();
            bool succeeded = false;
            dolphindb::TableSP tmp = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    if (symbol == "msg1")
                    {
                        appender1.append(tmp);
                        msg1_total += 1;
                    }
                    else if (symbol == "msg2")
                    {
                        appender2.append(tmp);
                        msg2_total += 1;
                    }
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    dolphindb::Util::sleep(100);
                }
            }
            if (msg.getOffset() == 1999)
                notify.set();
        }
    } });
    notify.wait();
    client.unsubscribe(HOST, PORT, st, "actionTest");
    th1.join();
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res1_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex1_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res2_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex2_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_EQ(msg1_total, 1000);
    ASSERT_EQ(msg2_total, 1000);
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadpooledclient_threadCount_1_subscribeWithstreamDeserilizer_2)
{
    std::string case_=getCaseName();
    int msg1_total = 0, msg2_total = 0;
    int listenport = GetParam();
    dolphindb::Signal notify;
    dolphindb::Mutex mutex;
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer_2(*pConn, st);
    dolphindb::AutoFitTableAppender appender1("", case_+"_res1_SDT", *pConn);
    dolphindb::AutoFitTableAppender appender2("", case_+"_res2_SDT", *pConn);
    auto onehandler = [&](dolphindb::Message msg)
    {
        const std::string &symbol = msg.getSymbol();
        dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
        bool succeeded = false;
        dolphindb::TableSP tmp = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                if (symbol == "msg1")
                {
                    appender1.append(tmp);
                    msg1_total += 1;
                }
                else if (symbol == "msg2")
                {
                    appender2.append(tmp);
                    msg2_total += 1;
                }
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                dolphindb::Util::sleep(100);
            }
        }

        if (msg.getOffset() == 1999)
        {
            notify.set();
        }
    };
    dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, 1);
    auto threadVec = client.subscribe(HOST, PORT, onehandler, st, "test_SD", 0, true, nullptr, false, false, USER, PASSWD, sdsp);
    ASSERT_EQ(threadVec.size(), 1);
    notify.wait();
    client.unsubscribe(HOST, PORT, st, "test_SD");
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res1_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex1_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res2_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex2_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_EQ(msg1_total, 1000);
    ASSERT_EQ(msg2_total, 1000);
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadpooledclient_threadCount_2_subscribeWithstreamDeserilizer_2)
{
    std::string case_=getCaseName();
    int msg1_total = 0;
    int msg2_total = 0;
    int listenport = GetParam();
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer_2(*pConn, st);
    dolphindb::Signal notify;
    dolphindb::Mutex mutex;
    dolphindb::AutoFitTableAppender appender1("", case_+"_res1_SDT", *pConn);
    dolphindb::AutoFitTableAppender appender2("", case_+"_res2_SDT", *pConn);
    auto onehandler = [&](dolphindb::Message msg)
    {
        const std::string &symbol = msg.getSymbol();
        dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
        bool succeeded = false;
        dolphindb::TableSP tmp = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                if (symbol == "msg1")
                {
                    appender1.append(tmp);
                    msg1_total += 1;
                }
                else if (symbol == "msg2")
                {
                    appender2.append(tmp);
                    msg2_total += 1;
                }
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                dolphindb::Util::sleep(100);
            }
        }

        if (msg.getOffset() == 1999)
        {
            notify.set();
        }
    };
    dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, 2);
    auto threadVec = client.subscribe(HOST, PORT, onehandler, st, "test_SD", 0, true, nullptr, false, false, USER, PASSWD, sdsp);
    ASSERT_EQ(threadVec.size(), 2);
    notify.wait();
    client.unsubscribe(HOST, PORT, st, "test_SD");
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res1_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex1_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res2_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex2_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_EQ(msg1_total, 1000);
    ASSERT_EQ(msg2_total, 1000);
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadclient_with_msgAsTable_True_2)
{
    std::string case_=getCaseName();
    int listenport = GetParam();
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer_2(*pConn, st);
    auto onehandler = [&](dolphindb::Message msg)
    {
        const std::string &symbol = msg.getSymbol();
        std::cout << symbol << std::endl;
    };
    dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
    ASSERT_ANY_THROW(auto thread1 = threadedClient.subscribe(HOST, PORT, onehandler, st, "test_SD", 0, true, nullptr, true, false, USER, PASSWD, sdsp));
}

TEST_P(StreamingDeserilizerTester_basic, test_Pollingclient_with_msgAsTable_True_2)
{
    std::string case_=getCaseName();
    int listenport = GetParam();
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer_2(*pConn, st);
    auto onehandler = [&](dolphindb::Message msg)
    {
        const std::string &symbol = msg.getSymbol();
        std::cout << symbol << std::endl;
    };
    dolphindb::PollingClient client = dolphindb::PollingClient(listenport);
    ASSERT_ANY_THROW(client.subscribe(HOST, PORT, st, "actionTest", 0, true, nullptr, true, false, USER, PASSWD, sdsp));
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadpooledclient_with_msgAsTable_True_2)
{
    std::string case_=getCaseName();
    int listenport = GetParam();
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer_2(*pConn, st);
    auto onehandler = [&](dolphindb::Message msg)
    {
        const std::string &symbol = msg.getSymbol();
    };

    dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, 1);

    ASSERT_ANY_THROW(auto threadVec = client.subscribe(HOST, PORT, onehandler, st, "test_SD", 0, true, nullptr, true, false, USER, PASSWD, sdsp));
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadclient_onehandler_subscribeWithstreamDeserilizer_3)
{
    std::string case_=getCaseName();
    int msg1_total = 0, msg2_total = 0;
    int listenport = GetParam();
    dolphindb::Signal notify;
    dolphindb::Mutex mutex;
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer_3(*pConn, st);
    dolphindb::AutoFitTableAppender appender1("", case_+"_res1_SDT", *pConn);
    dolphindb::AutoFitTableAppender appender2("", case_+"_res2_SDT", *pConn);
    auto onehandler = [&](dolphindb::Message msg)
    {
        const std::string &symbol = msg.getSymbol();
        dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
        bool succeeded = false;
        dolphindb::TableSP tmp = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                if (symbol == "msg1")
                {
                    appender1.append(tmp);
                    msg1_total += 1;
                }
                else if (symbol == "msg2")
                {
                    appender2.append(tmp);
                    msg2_total += 1;
                }
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                dolphindb::Util::sleep(100);
            }
        }
        if (msg.getOffset() == 1999)
        {
            notify.set();
        }
    };
    dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
    auto thread1 = threadedClient.subscribe(HOST, PORT, onehandler, st, "test_SD", 0, true, nullptr, false, false, USER, PASSWD, sdsp);
    notify.wait();
    threadedClient.unsubscribe(HOST, PORT, st, "test_SD");
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res1_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex1_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res2_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex2_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_EQ(msg1_total, 1000);
    ASSERT_EQ(msg2_total, 1000);
}

TEST_F(StreamingDeserilizerTester_basic, test_UDPThreadclient_onehandler_subscribeWithstreamDeserilizer_3_should_serial)
{
    #ifdef _WIN32
        GTEST_SKIP() << "not support udp on Windows yet.";
    #endif
    std::string case_=getCaseName();
    int msg1_total = 0, msg2_total = 0;
    dolphindb::Signal notify;
    dolphindb::Mutex mutex;
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer_3(*pConn, st);
    dolphindb::AutoFitTableAppender appender1("", case_+"_res1_SDT", *pConn);
    dolphindb::AutoFitTableAppender appender2("", case_+"_res2_SDT", *pConn);
    auto onehandler = [&](dolphindb::Message msg)
    {
        const std::string &symbol = msg.getSymbol();
        dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
        bool succeeded = false;
        dolphindb::TableSP tmp = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                if (symbol == "msg1")
                {
                    appender1.append(tmp);
                    msg1_total += 1;
                }
                else if (symbol == "msg2")
                {
                    appender2.append(tmp);
                    msg2_total += 1;
                }
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                dolphindb::Util::sleep(100);
            }
        }

        if (msg.getOffset() == 1999)
        {
            notify.set();
        }
    };
    dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(scfg);
    auto thread1 = threadedClient.subscribe(HOST, PORT, onehandler, st, "test_SD", 0, true, nullptr, false, false, USER, PASSWD, sdsp);
    notify.wait();
    threadedClient.unsubscribe(HOST, PORT, st, "test_SD");
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res1_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex1_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res2_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex2_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_EQ(msg1_total, 1000);
    ASSERT_EQ(msg2_total, 1000);
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadclient_batchhandler_subscribeWithstreamDeserilizer_3)
{
    std::string case_=getCaseName();
    int msg1_total = 0, msg2_total = 0;
    int listenport = GetParam();
    dolphindb::Signal notify;
    dolphindb::Mutex mutex;
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer_3(*pConn, st);
    dolphindb::AutoFitTableAppender appender1("", case_+"_res1_SDT", *pConn);
    dolphindb::AutoFitTableAppender appender2("", case_+"_res2_SDT", *pConn);
    auto batchhandler = [&](std::vector<dolphindb::Message> msgs)
    {
        dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
        for (auto &msg : msgs)
        {
            const std::string &symbol = msg.getSymbol();
            bool succeeded = false;
            dolphindb::TableSP tmp = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    if (symbol == "msg1")
                    {
                        appender1.append(tmp);
                        msg1_total += 1;
                    }
                    else if (symbol == "msg2")
                    {
                        appender2.append(tmp);
                        msg2_total += 1;
                    }
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    dolphindb::Util::sleep(100);
                }
            }
            if (msg.getOffset() == 1999)
            {
                notify.set();
            }
        }
    };
    dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
    auto thread2 = threadedClient.subscribe(HOST, PORT, batchhandler, st, "mutiSchemaBatch", 0, true, nullptr, true, 1, 1.0, false, USER, PASSWD, sdsp);
    notify.wait();
    threadedClient.unsubscribe(HOST, PORT, st, "mutiSchemaBatch");
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res1_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex1_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res2_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex2_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_EQ(msg1_total, 1000);
    ASSERT_EQ(msg2_total, 1000);
}

TEST_P(StreamingDeserilizerTester_basic, test_Pollingclient_subscribeWithstreamDeserilizer_3)
{
    std::string case_=getCaseName();
    int msg1_total = 0, msg2_total = 0;
    int listenport = GetParam();
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer_3(*pConn, st);
    dolphindb::AutoFitTableAppender appender1("", case_+"_res1_SDT", *pConn);
    dolphindb::AutoFitTableAppender appender2("", case_+"_res2_SDT", *pConn);
    dolphindb::PollingClient client = dolphindb::PollingClient(listenport);
    auto queue = client.subscribe(HOST, PORT, st, "actionTest", 0, true, nullptr, false, false, USER, PASSWD, sdsp);
    dolphindb::Message msg;
    dolphindb::Signal notify;
    dolphindb::Mutex mutex;
    std::thread th1 = std::thread([&]{
    dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
    while (true)
    {
        queue->pop(msg);
        if(msg.isNull()) {break;}
        else{
            const std::string &symbol = msg.getSymbol();
            bool succeeded = false;
            dolphindb::TableSP tmp = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    if (symbol == "msg1")
                    {
                        appender1.append(tmp);
                        msg1_total += 1;
                    }
                    else if (symbol == "msg2")
                    {
                        appender2.append(tmp);
                        msg2_total += 1;
                    }
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    dolphindb::Util::sleep(100);
                }
            }
            if (msg.getOffset() == 1999)
                notify.set();
        }
    } });
    notify.wait();
    client.unsubscribe(HOST, PORT, st, "actionTest");
    th1.join();
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res1_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex1_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res2_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex2_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_EQ(msg1_total, 1000);
    ASSERT_EQ(msg2_total, 1000);
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadpooledclient_threadCount_1_subscribeWithstreamDeserilizer_3)
{
    std::string case_=getCaseName();
    int msg1_total = 0, msg2_total = 0;
    int listenport = GetParam();
    dolphindb::Signal notify;
    dolphindb::Mutex mutex;
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer_3(*pConn, st);
    dolphindb::AutoFitTableAppender appender1("", case_+"_res1_SDT", *pConn);
    dolphindb::AutoFitTableAppender appender2("", case_+"_res2_SDT", *pConn);
    auto onehandler = [&](dolphindb::Message msg)
    {
        const std::string &symbol = msg.getSymbol();
        dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
        bool succeeded = false;
        dolphindb::TableSP tmp = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                if (symbol == "msg1")
                {
                    appender1.append(tmp);
                    msg1_total += 1;
                }
                else if (symbol == "msg2")
                {
                    appender2.append(tmp);
                    msg2_total += 1;
                }
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                dolphindb::Util::sleep(100);
            }
        }
        if (msg.getOffset() == 1999)
        {
            notify.set();
        }
    };

    dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, 1);
    auto threadVec = client.subscribe(HOST, PORT, onehandler, st, "test_SD", 0, true, nullptr, false, false, USER, PASSWD, sdsp);
    ASSERT_EQ(threadVec.size(), 1);
    notify.wait();
    client.unsubscribe(HOST, PORT, st, "test_SD");
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res1_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex1_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res2_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex2_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_EQ(msg1_total, 1000);
    ASSERT_EQ(msg2_total, 1000);
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadpooledclient_threadCount_3_subscribeWithstreamDeserilizer_3)
{
    std::string case_=getCaseName();
    int msg1_total = 0;
    int msg2_total = 0;
    int listenport = GetParam();
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer_3(*pConn, st);
    dolphindb::Signal notify;
    dolphindb::Mutex mutex;
    dolphindb::AutoFitTableAppender appender1("", case_+"_res1_SDT", *pConn);
    dolphindb::AutoFitTableAppender appender2("", case_+"_res2_SDT", *pConn);
    auto onehandler = [&](dolphindb::Message msg)
    {
        const std::string &symbol = msg.getSymbol();
        dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
        bool succeeded = false;
        dolphindb::TableSP tmp = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                if (symbol == "msg1")
                {
                    appender1.append(tmp);
                    msg1_total += 1;
                }
                else if (symbol == "msg2")
                {
                    appender2.append(tmp);
                    msg2_total += 1;
                }
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                dolphindb::Util::sleep(100);
            }
        }

        if (msg.getOffset() == 1999)
        {
            notify.set();
        }
    };
    dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport);
    auto threadVec = client.subscribe(HOST, PORT, onehandler, st, "test_SD", 0, true, nullptr, false, false, USER, PASSWD, sdsp);
    ASSERT_EQ(threadVec.size(), 3);
    notify.wait();
    client.unsubscribe(HOST, PORT, st, "test_SD");
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res1_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex1_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res2_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex2_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_EQ(msg1_total, 1000);
    ASSERT_EQ(msg2_total, 1000);
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadclient_with_msgAsTable_True_3)
{
    std::string case_=getCaseName();
    int listenport = GetParam();
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer_3(*pConn, st);
    auto onehandler = [&](dolphindb::Message msg)
    {
        const std::string &symbol = msg.getSymbol();
        std::cout << symbol << std::endl;
    };
    dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
    ASSERT_ANY_THROW(auto thread1 = threadedClient.subscribe(HOST, PORT, onehandler, st, "test_SD", 0, true, nullptr, true, false, USER, PASSWD, sdsp));
}

TEST_P(StreamingDeserilizerTester_basic, test_Pollingclient_with_msgAsTable_True_3)
{
    std::string case_=getCaseName();
    int listenport = GetParam();
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer_3(*pConn, st);
    auto onehandler = [&](dolphindb::Message msg)
    {
        const std::string &symbol = msg.getSymbol();
        std::cout << symbol << std::endl;
    };
    dolphindb::PollingClient client = dolphindb::PollingClient(listenport);
    ASSERT_ANY_THROW(client.subscribe(HOST, PORT, st, "actionTest", 0, true, nullptr, true, false, USER, PASSWD, sdsp));
}

TEST_P(StreamingDeserilizerTester_basic, test_Threadpooledclient_with_msgAsTable_True_3)
{
    std::string case_=getCaseName();
    int listenport = GetParam();
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer_3(*pConn, st);
    auto onehandler = [&](dolphindb::Message msg)
    {
        const std::string &symbol = msg.getSymbol();
        std::cout << symbol << std::endl;
    };
    dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, 1);

    ASSERT_ANY_THROW(auto threadVec = client.subscribe(HOST, PORT, onehandler, st, "test_SD", 0, true, nullptr, true, false, USER, PASSWD, sdsp));
}

class StreamingDeserilizerTester_allTypes : public StreamingDeserilizerTester, public ::testing::WithParamInterface<std::tuple<int, std::pair<dolphindb::DATA_TYPE, std::string>>>
{
public:
    static std::vector<std::pair<dolphindb::DATA_TYPE, std::string>> getData()
    {
        return {{dolphindb::DT_BOOL, "rand(0 1, 1)[0]"},{dolphindb::DT_BOOL, "bool()"},
                {dolphindb::DT_CHAR, "rand(127c, 1)[0]"},{dolphindb::DT_CHAR, "char()"},
                {dolphindb::DT_SHORT, "rand(32767h, 1)[0]"},{dolphindb::DT_SHORT, "short()"},
                {dolphindb::DT_INT, "rand(2147483647, 1)[0]"},{dolphindb::DT_INT, "int()"},
                {dolphindb::DT_LONG, "rand(1000l, 1)[0]"},{dolphindb::DT_LONG, "long()"},
                {dolphindb::DT_DATE, "rand(2019.01.01, 1)[0]"},{dolphindb::DT_DATE, "date()"},
                {dolphindb::DT_MONTH, "rand(2019.01M, 1)[0]"},{dolphindb::DT_MONTH, "month()"},
                {dolphindb::DT_TIME, "rand(12:00:00.123, 1)[0]"},{dolphindb::DT_TIME, "time()"},
                {dolphindb::DT_MINUTE, "rand(12:00m, 1)[0]"},{dolphindb::DT_MINUTE, "minute()"},
                {dolphindb::DT_SECOND, "rand(12:00:00, 1)[0]"},{dolphindb::DT_SECOND, "second()"},
                {dolphindb::DT_DATETIME, "rand(2019.01.01 12:00:00, 1)[0]"},{dolphindb::DT_DATETIME, "datetime()"},
                {dolphindb::DT_TIMESTAMP, "rand(2019.01.01 12:00:00.123, 1)[0]"},{dolphindb::DT_TIMESTAMP, "timestamp()"},
                {dolphindb::DT_NANOTIME, "rand(12:00:00.123456789, 1)[0]"},{dolphindb::DT_NANOTIME, "nanotime()"},
                {dolphindb::DT_NANOTIMESTAMP, "rand(2019.01.01 12:00:00.123456789, 1)[0]"},{dolphindb::DT_NANOTIMESTAMP, "nanotimestamp()"},
                {dolphindb::DT_DATEHOUR, "rand(datehour(100), 1)[0]"},{dolphindb::DT_DATEHOUR, "datehour()"},
                {dolphindb::DT_FLOAT, "rand(10.00f, 1)[0]"},{dolphindb::DT_FLOAT, "float()"},
                {dolphindb::DT_DOUBLE, "rand(10.00, 1)[0]"},{dolphindb::DT_DOUBLE, "double()"},
                {dolphindb::DT_IP, "take(ipaddr('192.168.1.1'), 1)[0]"},{dolphindb::DT_IP, "ipaddr()"},
                {dolphindb::DT_UUID, "take(uuid('12345678-1234-1234-1234-123456789012'), 1)[0]"},{dolphindb::DT_UUID, "uuid()"},
                {dolphindb::DT_INT128, "take(int128(`e1671797c52e15f763380b45e841ec32), 1)[0]"},{dolphindb::DT_INT128, "int128()"},
                {dolphindb::DT_DECIMAL32, "decimal32(rand('-1.123''''2.23468965412', 1)[0], 8)"},{dolphindb::DT_DECIMAL32, "decimal32(,2)"},
                {dolphindb::DT_DECIMAL64, "decimal64(rand('-1.123''''2.123123123123123123', 1)[0], 15)"},{dolphindb::DT_DECIMAL64, "decimal64(,16)"},
                {dolphindb::DT_DECIMAL128, "decimal128(rand('-1.123''''2.123123123123123123123123123', 1)[0], 25)"},{dolphindb::DT_DECIMAL128, "decimal128(,26)"},
                {dolphindb::DT_BLOB, "blob('abc')"},{dolphindb::DT_BLOB, "blob(`)"}};
    };
};
INSTANTIATE_TEST_SUITE_P(, StreamingDeserilizerTester_allTypes, testing::Combine(
    testing::Values(0, rand() % 1000 + 13000),
    testing::ValuesIn(StreamingDeserilizerTester_allTypes::getData())
));


TEST_P(StreamingDeserilizerTester_allTypes, test_Threadclient_onehandler_subscribeWithstreamDeserilizer_allTypes)
{
    std::string case_=getCaseName();
    int msg1_total = 0, msg2_total = 0;
    int listenport = std::get<0>(GetParam());
    dolphindb::DATA_TYPE ttp = std::get<1>(GetParam()).first;
    std::string scr = std::get<1>(GetParam()).second;
    dolphindb::Signal notify;
    dolphindb::Mutex mutex;
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer_withallTypes(*pConn, st, ttp, scr);
    dolphindb::AutoFitTableAppender appender1("", case_+"_res1_SDT", *pConn);
    dolphindb::AutoFitTableAppender appender2("", case_+"_res2_SDT", *pConn);
    auto onehandler = [&](dolphindb::Message msg)
    {
        const std::string &symbol = msg.getSymbol();
        dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
        bool succeeded = false;
        dolphindb::TableSP tmp = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                if (symbol == "msg1")
                {
                    appender1.append(tmp);
                    msg1_total += 1;
                }
                else if (symbol == "msg2")
                {
                    appender2.append(tmp);
                    msg2_total += 1;
                }
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                dolphindb::Util::sleep(100);
            }
        }
        if (msg.getOffset() == 1999)
        {
            notify.set();
        }
    };
    dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
    auto thread1 = threadedClient.subscribe(HOST, PORT, onehandler, st, "test_SD", 0, false, nullptr, false, false, USER, PASSWD, sdsp);
    notify.wait();
    threadedClient.unsubscribe(HOST, PORT, st, "test_SD");
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res1_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex1_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res2_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex2_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_EQ(msg1_total, 1000);
    ASSERT_EQ(msg2_total, 1000);
}

TEST_P(StreamingDeserilizerTester_allTypes, test_UDPThreadclient_onehandler_subscribeWithstreamDeserilizer_allTypes_should_serial)
{
    #ifdef _WIN32
        GTEST_SKIP() << "not support udp on Windows yet.";
    #endif
    std::string case_=getCaseName();
    int msg1_total = 0, msg2_total = 0;
    dolphindb::DATA_TYPE ttp = std::get<1>(GetParam()).first;
    std::string scr = std::get<1>(GetParam()).second;
    dolphindb::Signal notify;
    dolphindb::Mutex mutex;
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer_withallTypes(*pConn, st, ttp, scr);
    dolphindb::AutoFitTableAppender appender1("", case_+"_res1_SDT", *pConn);
    dolphindb::AutoFitTableAppender appender2("", case_+"_res2_SDT", *pConn);
    auto onehandler = [&](dolphindb::Message msg)
    {
        const std::string &symbol = msg.getSymbol();
        dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
        bool succeeded = false;
        dolphindb::TableSP tmp = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                if (symbol == "msg1")
                {
                    appender1.append(tmp);
                    msg1_total += 1;
                }
                else if (symbol == "msg2")
                {
                    appender2.append(tmp);
                    msg2_total += 1;
                }
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                dolphindb::Util::sleep(100);
            }
        }

        if (msg.getOffset() == 1999)
        {
            notify.set();
        }
    };
    dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(scfg);
    auto thread1 = threadedClient.subscribe(HOST, PORT, onehandler, st, "test_SD", 0, false, nullptr, false, false, USER, PASSWD, sdsp);
    notify.wait();
    threadedClient.unsubscribe(HOST, PORT, st, "test_SD");
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res1_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex1_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res2_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex2_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_EQ(msg1_total, 1000);
    ASSERT_EQ(msg2_total, 1000);
}

TEST_P(StreamingDeserilizerTester_allTypes, test_Threadclient_batchhandler_subscribeWithstreamDeserilizer_allTypes)
{
    std::string case_=getCaseName();
    int msg1_total = 0, msg2_total = 0;
    int listenport = std::get<0>(GetParam());
    dolphindb::DATA_TYPE ttp = std::get<1>(GetParam()).first;
    std::string scr = std::get<1>(GetParam()).second;
    dolphindb::Signal notify;
    dolphindb::Mutex mutex;
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer_withallTypes(*pConn, st, ttp, scr);
    dolphindb::AutoFitTableAppender appender1("", case_+"_res1_SDT", *pConn);
    dolphindb::AutoFitTableAppender appender2("", case_+"_res2_SDT", *pConn);
    auto batchhandler = [&](std::vector<dolphindb::Message> msgs)
    {
        dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
        for (auto &msg : msgs)
        {
            const std::string &symbol = msg.getSymbol();
            bool succeeded = false;
            dolphindb::TableSP tmp = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    if (symbol == "msg1")
                    {
                        appender1.append(tmp);
                        msg1_total += 1;
                    }
                    else if (symbol == "msg2")
                    {
                        appender2.append(tmp);
                        msg2_total += 1;
                    }
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    dolphindb::Util::sleep(100);
                }
            }
            if (msg.getOffset() == 1999)
            {
                notify.set();
            }
        }
    };
    dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
    auto thread2 = threadedClient.subscribe(HOST, PORT, batchhandler, st, "mutiSchemaBatch", 0, false, nullptr, true, 1, 1.0, false, USER, PASSWD, sdsp);
    notify.wait();
    threadedClient.unsubscribe(HOST, PORT, st, "mutiSchemaBatch");
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res1_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex1_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res2_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex2_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_EQ(msg1_total, 1000);
    ASSERT_EQ(msg2_total, 1000);
}

TEST_P(StreamingDeserilizerTester_allTypes, test_Pollingclient_subscribeWithstreamDeserilizer_allTypes)
{
    std::string case_=getCaseName();
    int msg1_total = 0, msg2_total = 0;
    int listenport = std::get<0>(GetParam());
    dolphindb::DATA_TYPE ttp = std::get<1>(GetParam()).first;
    std::string scr = std::get<1>(GetParam()).second;
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer_withallTypes(*pConn, st, ttp, scr);
    dolphindb::AutoFitTableAppender appender1("", case_+"_res1_SDT", *pConn);
    dolphindb::AutoFitTableAppender appender2("", case_+"_res2_SDT", *pConn);
    dolphindb::PollingClient client = dolphindb::PollingClient(listenport);
    auto queue = client.subscribe(HOST, PORT, st, "actionTest", 0, true, nullptr, false, false, USER, PASSWD, sdsp);
    dolphindb::Message msg;
    dolphindb::Signal notify;
    dolphindb::Mutex mutex;
    std::thread th1 = std::thread([&]{
    dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
    while (true)
    {
        queue->pop(msg);
        if(msg.isNull()) {break;}
        else{
            const std::string &symbol = msg.getSymbol();
            bool succeeded = false;
            dolphindb::TableSP tmp = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    if (symbol == "msg1")
                    {
                        appender1.append(tmp);
                        msg1_total += 1;
                    }
                    else if (symbol == "msg2")
                    {
                        appender2.append(tmp);
                        msg2_total += 1;
                    }
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    dolphindb::Util::sleep(100);
                }
            }
            if (msg.getOffset() == 1999)
                notify.set();
        }
    } });
    notify.wait();
    client.unsubscribe(HOST, PORT, st, "actionTest");
    th1.join();
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res1_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex1_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res2_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex2_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_EQ(msg1_total, 1000);
    ASSERT_EQ(msg2_total, 1000);
}

TEST_P(StreamingDeserilizerTester_allTypes, test_Threadpooledclient_subscribeWithstreamDeserilizer_allTypes)
{
    std::string case_=getCaseName();
    int msg1_total = 0, msg2_total = 0;
    int listenport = std::get<0>(GetParam());
    dolphindb::DATA_TYPE ttp = std::get<1>(GetParam()).first;
    std::string scr = std::get<1>(GetParam()).second;
    dolphindb::Signal notify;
    dolphindb::Mutex mutex;
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer_withallTypes(*pConn, st, ttp, scr);
    dolphindb::AutoFitTableAppender appender1("", case_+"_res1_SDT", *pConn);
    dolphindb::AutoFitTableAppender appender2("", case_+"_res2_SDT", *pConn);
    auto onehandler = [&](dolphindb::Message msg)
    {
        const std::string &symbol = msg.getSymbol();
        dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
        bool succeeded = false;
        dolphindb::TableSP tmp = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                if (symbol == "msg1")
                {
                    appender1.append(tmp);
                    msg1_total += 1;
                }
                else if (symbol == "msg2")
                {
                    appender2.append(tmp);
                    msg2_total += 1;
                }
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                dolphindb::Util::sleep(100);
            }
        }
        if (msg.getOffset() == 1999)
        {
            notify.set();
        }
    };
    int threadCount = rand() % 10 + 1;
    dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, threadCount);
    auto threadVec = client.subscribe(HOST, PORT, onehandler, st, "test_SD", 0, true, nullptr, false, false, USER, PASSWD, sdsp);
    ASSERT_EQ(threadVec.size(), threadCount);
    notify.wait();
    client.unsubscribe(HOST, PORT, st, "test_SD");
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res1_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex1_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res2_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex2_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_EQ(msg1_total, 1000);
    ASSERT_EQ(msg2_total, 1000);
}

class StreamingDeserilizerTester_arrayVector : public StreamingDeserilizerTester, public ::testing::WithParamInterface<std::tuple<int, std::pair<dolphindb::DATA_TYPE, std::string>>>
{
public:
    static std::vector<std::pair<dolphindb::DATA_TYPE, std::string>> getData()
    {
        return {{dolphindb::DT_BOOL_ARRAY, "rand(0 1, 2)"},{dolphindb::DT_BOOL_ARRAY, "array(BOOL, 2,2,NULL)"},
                {dolphindb::DT_CHAR_ARRAY, "rand(127c, 2)"},{dolphindb::DT_CHAR_ARRAY, "array(CHAR, 2,2,NULL)"},
                {dolphindb::DT_SHORT_ARRAY, "rand(32767h, 2)"}, {dolphindb::DT_SHORT_ARRAY, "array(SHORT, 2,2,NULL)"},
                {dolphindb::DT_INT_ARRAY, "rand(2147483647, 2)"}, {dolphindb::DT_INT_ARRAY, "array(INT, 2,2,NULL)"},
                {dolphindb::DT_LONG_ARRAY, "rand(1000l, 2)"}, {dolphindb::DT_LONG_ARRAY, "array(LONG, 2,2,NULL)"},
                {dolphindb::DT_DATE_ARRAY, "rand(2019.01.01, 2)"}, {dolphindb::DT_DATE_ARRAY, "array(DATE, 2,2,NULL)"},
                {dolphindb::DT_MONTH_ARRAY, "rand(2019.01M, 2)"}, {dolphindb::DT_MONTH_ARRAY, "array(MONTH, 2,2,NULL)"},
                {dolphindb::DT_TIME_ARRAY, "rand(12:00:00.123, 2)"}, {dolphindb::DT_TIME_ARRAY, "array(TIME, 2,2,NULL)"},
                {dolphindb::DT_MINUTE_ARRAY, "rand(12:00m, 2)"}, {dolphindb::DT_MINUTE_ARRAY, "array(MINUTE, 2,2,NULL)"},
                {dolphindb::DT_SECOND_ARRAY, "rand(12:00:00, 2)"}, {dolphindb::DT_SECOND_ARRAY, "array(SECOND, 2,2,NULL)"},
                {dolphindb::DT_DATETIME_ARRAY, "rand(2019.01.01 12:00:00, 2)"}, {dolphindb::DT_DATETIME_ARRAY, "array(DATETIME, 2,2,NULL)"},
                {dolphindb::DT_TIMESTAMP_ARRAY, "rand(2019.01.01 12:00:00.123, 2)"}, {dolphindb::DT_TIMESTAMP_ARRAY, "array(TIMESTAMP, 2,2,NULL)"},
                {dolphindb::DT_NANOTIME_ARRAY, "rand(12:00:00.123456789, 2)"}, {dolphindb::DT_NANOTIME_ARRAY, "array(NANOTIME, 2,2,NULL)"},
                {dolphindb::DT_NANOTIMESTAMP_ARRAY, "rand(2019.01.01 12:00:00.123456789, 2)"}, {dolphindb::DT_NANOTIMESTAMP_ARRAY, "array(NANOTIMESTAMP, 2,2,NULL)"},
                {dolphindb::DT_DATEHOUR_ARRAY, "rand(datehour(100), 2)"}, {dolphindb::DT_DATEHOUR_ARRAY, "array(DATEHOUR, 2,2,NULL)"},
                {dolphindb::DT_FLOAT_ARRAY, "rand(10.00f, 2)"}, {dolphindb::DT_FLOAT_ARRAY, "array(FLOAT, 2,2,NULL)"},
                {dolphindb::DT_DOUBLE_ARRAY, "rand(10.00, 2)"}, {dolphindb::DT_DOUBLE_ARRAY, "array(DOUBLE, 2,2,NULL)"},
                {dolphindb::DT_IP_ARRAY, "take(ipaddr('192.168.1.1'), 2)"}, {dolphindb::DT_IP_ARRAY, "array(IPADDR, 2,2,NULL)"},
                {dolphindb::DT_UUID_ARRAY, "take(uuid('12345678-1234-1234-1234-123456789012'), 2)"}, {dolphindb::DT_UUID_ARRAY, "array(UUID, 2,2,NULL)"},
                {dolphindb::DT_INT128_ARRAY, "take(int128(`e1671797c52e15f763380b45e841ec32), 2)"}, {dolphindb::DT_INT128_ARRAY, "array(INT128, 2,2,NULL)"},
                {dolphindb::DT_DECIMAL32_ARRAY, "decimal32(rand('-1.123''''2.23468965412', 2), 8)"}, {dolphindb::DT_DECIMAL32_ARRAY, "array(DECIMAL32(2), 2,2,NULL)"},
                {dolphindb::DT_DECIMAL64_ARRAY, "decimal64(rand('-1.123''''2.123123123123123123', 2), 15)"}, {dolphindb::DT_DECIMAL64_ARRAY, "array(DECIMAL64(15), 2,2,NULL)"},
                {dolphindb::DT_DECIMAL128_ARRAY, "decimal128(rand('-1.123''''2.123123123123123123123123123', 2), 25)"}, {dolphindb::DT_DECIMAL128_ARRAY, "array(DECIMAL128(25), 2,2,NULL)"},};
    };
};
INSTANTIATE_TEST_SUITE_P(, StreamingDeserilizerTester_arrayVector, testing::Combine(
    testing::Values(0, rand() % 1000 + 13000),
    testing::ValuesIn(StreamingDeserilizerTester_arrayVector::getData())
));

TEST_P(StreamingDeserilizerTester_arrayVector, test_Threadclient_onehandler_subscribeWithstreamDeserilizer_arrayVector)
{
    std::string case_=getCaseName();
    int msg1_total = 0, msg2_total = 0;
    int listenport = std::get<0>(GetParam());
    dolphindb::DATA_TYPE ttp = std::get<1>(GetParam()).first;
    std::string scr = std::get<1>(GetParam()).second;
    dolphindb::Signal notify;
    dolphindb::Mutex mutex;
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer_witharrayVector(*pConn, st, ttp, scr);
    dolphindb::AutoFitTableAppender appender1("", case_+"_res1_SDT", *pConn);
    dolphindb::AutoFitTableAppender appender2("", case_+"_res2_SDT", *pConn);
    auto onehandler = [&](dolphindb::Message msg)
    {
        const std::string &symbol = msg.getSymbol();
        dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
        bool succeeded = false;
        dolphindb::TableSP tmp = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                if (symbol == "msg1")
                {
                    appender1.append(tmp);
                    msg1_total += 1;
                }
                else if (symbol == "msg2")
                {
                    appender2.append(tmp);
                    msg2_total += 1;
                }
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                dolphindb::Util::sleep(100);
            }
        }

        if (msg.getOffset() == 1999)
        {
            notify.set();
        }
    };
    dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
    auto thread1 = threadedClient.subscribe(HOST, PORT, onehandler, st, "test_SD", 0, false, nullptr, false, false, USER, PASSWD, sdsp);
    notify.wait();
    threadedClient.unsubscribe(HOST, PORT, st, "test_SD");
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res1_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex1_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res2_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex2_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_EQ(msg1_total, 1000);
    ASSERT_EQ(msg2_total, 1000);
}

TEST_P(StreamingDeserilizerTester_arrayVector, test_UDPThreadclient_onehandler_subscribeWithstreamDeserilizer_arrayVector_should_serial)
{
    #ifdef _WIN32
        GTEST_SKIP() << "not support udp on Windows yet.";
    #endif
    std::string case_=getCaseName();
    int msg1_total = 0, msg2_total = 0;
    dolphindb::DATA_TYPE ttp = std::get<1>(GetParam()).first;
    std::string scr = std::get<1>(GetParam()).second;
    dolphindb::Signal notify;
    dolphindb::Mutex mutex;
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer_witharrayVector(*pConn, st, ttp, scr);
    dolphindb::AutoFitTableAppender appender1("", case_+"_res1_SDT", *pConn);
    dolphindb::AutoFitTableAppender appender2("", case_+"_res2_SDT", *pConn);
    auto onehandler = [&](dolphindb::Message msg)
    {
        const std::string &symbol = msg.getSymbol();
        dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
        bool succeeded = false;
        dolphindb::TableSP tmp = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                if (symbol == "msg1")
                {
                    appender1.append(tmp);
                    msg1_total += 1;
                }
                else if (symbol == "msg2")
                {
                    appender2.append(tmp);
                    msg2_total += 1;
                }
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                dolphindb::Util::sleep(100);
            }
        }

        if (msg.getOffset() == 1999)
        {
            notify.set();
        }
    };
    dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(scfg);
    auto thread1 = threadedClient.subscribe(HOST, PORT, onehandler, st, "test_SD", 0, false, nullptr, false, false, USER, PASSWD, sdsp);
    notify.wait();
    threadedClient.unsubscribe(HOST, PORT, st, "test_SD");
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res1_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex1_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res2_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex2_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_EQ(msg1_total, 1000);
    ASSERT_EQ(msg2_total, 1000);
}

TEST_P(StreamingDeserilizerTester_arrayVector, test_Threadclient_batchhandler_subscribeWithstreamDeserilizer_arrayVector)
{
    std::string case_=getCaseName();
    int msg1_total = 0, msg2_total = 0;
    int listenport = std::get<0>(GetParam());
    dolphindb::DATA_TYPE ttp = std::get<1>(GetParam()).first;
    std::string scr = std::get<1>(GetParam()).second;
    dolphindb::Signal notify;
    dolphindb::Mutex mutex;
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer_witharrayVector(*pConn, st, ttp, scr);
    dolphindb::AutoFitTableAppender appender1("", case_+"_res1_SDT", *pConn);
    dolphindb::AutoFitTableAppender appender2("", case_+"_res2_SDT", *pConn);
    auto batchhandler = [&](std::vector<dolphindb::Message> msgs)
    {
        dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
        for (auto &msg : msgs)
        {
            const std::string &symbol = msg.getSymbol();
            bool succeeded = false;
            dolphindb::TableSP tmp = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    if (symbol == "msg1")
                    {
                        appender1.append(tmp);
                        msg1_total += 1;
                    }
                    else if (symbol == "msg2")
                    {
                        appender2.append(tmp);
                        msg2_total += 1;
                    }
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    dolphindb::Util::sleep(100);
                }
            }
            if (msg.getOffset() == 1999)
            {
                notify.set();
            }
        }
    };
    dolphindb::ThreadedClient threadedClient = dolphindb::ThreadedClient(listenport);
    auto thread2 = threadedClient.subscribe(HOST, PORT, batchhandler, st, "mutiSchemaBatch", 0, false, nullptr, true, 1, 1.0, false, USER, PASSWD, sdsp);
    notify.wait();
    threadedClient.unsubscribe(HOST, PORT, st, "mutiSchemaBatch");
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res1_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex1_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res2_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex2_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_EQ(msg1_total, 1000);
    ASSERT_EQ(msg2_total, 1000);
}

TEST_P(StreamingDeserilizerTester_arrayVector, test_Pollingclient_subscribeWithstreamDeserilizer_arrayVector)
{
    std::string case_=getCaseName();
    int msg1_total = 0, msg2_total = 0;
    int listenport = std::get<0>(GetParam());
    dolphindb::DATA_TYPE ttp = std::get<1>(GetParam()).first;
    std::string scr = std::get<1>(GetParam()).second;
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer_witharrayVector(*pConn, st, ttp, scr);
    dolphindb::AutoFitTableAppender appender1("", case_+"_res1_SDT", *pConn);
    dolphindb::AutoFitTableAppender appender2("", case_+"_res2_SDT", *pConn);
    dolphindb::PollingClient client = dolphindb::PollingClient(listenport);
    auto queue = client.subscribe(HOST, PORT, st, "actionTest", 0, true, nullptr, false, false, USER, PASSWD, sdsp);
    dolphindb::Message msg;
    dolphindb::Signal notify;
    dolphindb::Mutex mutex;
    std::thread th1 = std::thread([&]{
    dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
    while (true)
    {
        queue->pop(msg);
        // std::cout<<msg->getString()<<std::endl;
        if(msg.isNull()) {break;}
        else{
            const std::string &symbol = msg.getSymbol();
            bool succeeded = false;
            dolphindb::TableSP tmp = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    if (symbol == "msg1")
                    {
                        appender1.append(tmp);
                        msg1_total += 1;
                    }
                    else if (symbol == "msg2")
                    {
                        appender2.append(tmp);
                        msg2_total += 1;
                    }
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    dolphindb::Util::sleep(100);
                }
            }
            if (msg.getOffset() == 1999)
                notify.set();
        }
    } });

    notify.wait();
    client.unsubscribe(HOST, PORT, st, "actionTest");
    th1.join();
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res1_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex1_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res2_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex2_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_EQ(msg1_total, 1000);
    ASSERT_EQ(msg2_total, 1000);
}

TEST_P(StreamingDeserilizerTester_arrayVector, test_Threadpooledclient_subscribeWithstreamDeserilizer_arrayVector)
{
    std::string case_=getCaseName();
    int msg1_total = 0, msg2_total = 0;
    int listenport = std::get<0>(GetParam());
    dolphindb::DATA_TYPE ttp = std::get<1>(GetParam()).first;
    std::string scr = std::get<1>(GetParam()).second;
    dolphindb::Signal notify;
    dolphindb::Mutex mutex;
    const std::string st = case_;
    dolphindb::StreamDeserializerSP sdsp = createStreamDeserializer_witharrayVector(*pConn, st, ttp, scr);
    dolphindb::AutoFitTableAppender appender1("", case_+"_res1_SDT", *pConn);
    dolphindb::AutoFitTableAppender appender2("", case_+"_res2_SDT", *pConn);
    auto onehandler = [&](dolphindb::Message msg)
    {
        const std::string &symbol = msg.getSymbol();
        dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
        bool succeeded = false;
        dolphindb::TableSP tmp = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                if (symbol == "msg1")
                {
                    appender1.append(tmp);
                    msg1_total += 1;
                }
                else if (symbol == "msg2")
                {
                    appender2.append(tmp);
                    msg2_total += 1;
                }
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                dolphindb::Util::sleep(100);
            }
        }
        if (msg.getOffset() == 1999)
        {
            notify.set();
        }
    };
    int threadCount = rand() % 10 + 1;
    dolphindb::ThreadPooledClient client = dolphindb::ThreadPooledClient(listenport, threadCount);
    auto threadVec = client.subscribe(HOST, PORT, onehandler, st, "test_SD", 0, true, nullptr, false, false, USER, PASSWD, sdsp);
    ASSERT_EQ(threadVec.size(), threadCount);
    notify.wait();
    client.unsubscribe(HOST, PORT, st, "test_SD");
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res1_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex1_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_TRUE(pConn->run("re = select * from "+case_+"_res2_SDT order by datetimev;\
                        ex = select * from "+case_+"_ex2_SDT order by datetimev;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    ASSERT_EQ(msg1_total, 1000);
    ASSERT_EQ(msg2_total, 1000);
}