#include "config.h"

#define CLEAR_ENV2(_sessionsp) \
    _sessionsp->run("\
        def clear_env(){\
            for (name in (exec name from objs(true) where shared=true))\
            {\
                try{dropStreamTable(name, true)}catch(ex){};go;\
                try{undef(name, SHARED)}catch(ex){};go;\
            };\
        };\
        pnodeRun(clear_env);\
        for(db in getClusterDFSDatabases())\
        {\
            try{dropDatabase(db)}catch(ex){};go;\
        };\
        undef all;go");

namespace UDPCT
{

class StreamingUDPThreadedClientTester : public testing::Test
{
public:
    // Suite
    static void SetUpTestCase()
    {
        // DBConnection conn;

        conn300->initialize();
        conn.initialize();
        bool ret = conn300->connect(hostName, port300, "admin", "123456") && conn.connect(hostName, port, "admin", "123456");
        if (!ret)
        {
            cout << "Failed to connect to the server" << endl;
        }
        else
        {
            cout << "connect to " + hostName + ":" + std::to_string(port300) << endl;
            cout << "connect to " + hostName + ":" + std::to_string(port) << endl;
        }
        scfg.protocol = TransportationProtocol::UDP;
    }
    static void TearDownTestCase()
    {
        usedPorts.clear();
        conn300->close();
    }

    // Case
    virtual void SetUp()
    {
        cout << "check connect...";
        try
        {
            ConstantSP res = conn300->run("1+1");
        }
        catch (const std::exception &e)
        {
            conn300->connect(hostName, port300, "admin", "123456");
        }

        cout << "ok" << endl;
        CLEAR_ENV2(conn300);
    }
    virtual void TearDown()
    {
        CLEAR_ENV2(conn300);
    }
public:
    static StreamingClientConfig scfg;
};
StreamingClientConfig StreamingUDPThreadedClientTester::scfg;



static void createSharedTableAndReplay(const string &st, int rows)
{
    string script = "login(\"admin\",\"123456\")\n\
            st1 = streamTable(100:0, `datetimev`timestampv`sym`price1`price2,[DATETIME,TIMESTAMP,SYMBOL,DOUBLE,DOUBLE])\n\
            enableTableShareAndPersistence(table=st1, tableName=`" +
                    st + ")\n\
            go\n\
            setStreamTableFilterColumn(" +
                    st + ", `sym)";
    conn300->run(script);

    string replayScript = "n = " + to_string(rows) + ";table1_UDPCT = table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);\
            share table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]) as res_UDPCT;assert res_UDPCT.rows() == 0;go;\
            tableInsert(table1_UDPCT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));share table1_UDPCT as ex_UDPCT;\
            replay(inputTables=table1_UDPCT, outputTables=`" +
                            st + ", dateColumn=`timestampv, timeColumn=`timestampv)";
    conn300->run(replayScript);
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
                                                                                                                "enableTableShareAndPersistence(table=st1, tableName=`" +
        st + ", asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0);go;"
                "setStreamTableFilterColumn(" +
        st + ", `csymbol);go;"
                "table1_UDPCT = table(1:0, colName, colType);"
                "row_num = 1000;"
                "share table(1:0, colName, colType) as res_UDPCT;assert res_UDPCT.rows() == 0;go;"
                "col1 = rand(2 ,row_num);col2 = rand(256 ,row_num);col3 = rand(-row_num..row_num ,row_num);col4 = rand(-row_num..row_num ,row_num);col5 = rand(-row_num..row_num ,row_num);col6 = rand(0..row_num ,row_num);col7 = rand(0..row_num ,row_num);"
                "col8 = rand(0..row_num ,row_num);col9 = rand(0..row_num ,row_num);col10 = rand(0..row_num ,row_num);col11 = rand(0..row_num ,row_num);col12 = rand(0..row_num ,row_num);col13 = rand(0..row_num ,row_num);col14= rand(0..row_num ,row_num);"
                "col15 = rand(round(row_num,2) ,row_num);col16 = rand(round(row_num,2) ,row_num);col17 =rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);col18 =rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);col19 =rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);col20 = take(ipaddr(\"192.168.1.13\"),row_num);"
                "col21 = take(uuid(\"5d212a78-cc48-e3b1-4235-b4d91473ee87\"),row_num);col22 = take(int128(\"e1671797c52e15f763380b45e841ec32\"),row_num);col23=rand((-1000..1000)/1000.0000$DECIMAL32(" +
        to_string(scale32) + "),row_num);col24=rand((-1000..1000)/1000.0000$DECIMAL64(" + to_string(scale64) + "),row_num);col25=rand((-1000..1000)/1000.0000$DECIMAL128(" + to_string(scale128) + "),row_num);go;"
                                                                                                                                                                                                    "for (i in 0..(row_num-1)){tableInsert(table1_UDPCT,i,col1[i],col2[i],col3[i],col4[i],col5[i],col6[i],col7[i],col8[i],col9[i],col10[i],col11[i],col12[i],col13[i],col14[i],col15[i],col16[i],col17[i],col18[i],col19[i],col20[i],col21[i],col22[i],col23[i],col24[i],col25[i])};"
                                                                                                                                                                                                    "share table1_UDPCT as ex_UDPCT;go;"
                                                                                                                                                                                                    "replay(inputTables=table1_UDPCT, outputTables=`" +
        st + ", dateColumn=`ctimestamp, timeColumn=`ctimestamp);";
    conn300->run(replayScript);
}

static void createSharedTableAndReplay_withArrayVector(const string &st)
{
    string replayScript =
        "colName = `ts`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cdatehour`cfloat`cdouble`cipaddr`cuuid`cint128`cdecimal32`cdecimal64`cdecimal128;"
        "colType = [TIMESTAMP, BOOL[], CHAR[], SHORT[], INT[],LONG[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], DATEHOUR[], FLOAT[], DOUBLE[], IPADDR[], UUID[], INT128[],DECIMAL32(6)[],DECIMAL64(16)[],DECIMAL128(26)[]];"
        "st1 = streamTable(1:0,colName, colType);"
        "share table(1:0, colName, colType) as res_UDPCT;assert res_UDPCT.rows() == 0;go;"
        "enableTableShareAndPersistence(table=st1, tableName=`" +
        st + ");go;"
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
                "table1_UDPCT=table(ts,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cdatehour,cfloat,cdouble,cipaddr,cuuid,cint128,cdecimal32,cdecimal64,cdecimal128);"
                "share table1_UDPCT as ex_UDPCT;go;"
                "replay(inputTables=table1_UDPCT, outputTables=`" +
        st + ", dateColumn=`ts, timeColumn=`ts);";
    conn300->run(replayScript);
}


TableSP UDPmsgToTable(VectorSP tupleVal)
{
    vector<string> colNames;
    vector<ConstantSP> columnVecs;
    auto col_count = tupleVal->size();
    for(auto i=0;i<col_count;i++){
        colNames.emplace_back("col"+ to_string(i));
    }

    columnVecs.reserve(col_count);
    for (auto i=0;i<col_count;i++)
    {
        DATA_FORM form = tupleVal->get(i)->getForm();
        DATA_TYPE _t = tupleVal->get(i)->getType();

        DATA_TYPE type = form == DF_VECTOR && _t < ARRAY_TYPE_BASE ? static_cast<DATA_TYPE>(_t+ARRAY_TYPE_BASE):_t;
        int extraParam = tupleVal->get(i)->getExtraParamForType();

        VectorSP col;
        if (form == DF_VECTOR){
            col = Util::createArrayVector(type, 0, 0, true, extraParam);
        }else{
            col = Util::createVector(type, 0, 0, true, extraParam);
        }
        col->append(tupleVal->get(i));
        columnVecs.emplace_back(col);
    }

    return Util::createTable(colNames, columnVecs);
}

TEST_F(StreamingUDPThreadedClientTester, DISABLED_DDB_version_200)
{
    string st = "outTables_" + getRandString(10);
    UDPCT::createSharedTableAndReplay(st, 1000);
    ThreadedClient client(scfg);
    EXPECT_ANY_THROW(client.subscribe(hostName, port, [](Message msg){}, st));
}


TEST_F(StreamingUDPThreadedClientTester, tableNotExist)
{
    ThreadedClient client(scfg);
    EXPECT_ANY_THROW(client.subscribe(hostName, port300, [](Message msg){}, "ajwdhwjkae", DEFAULT_ACTION_NAME, 0));
}

TEST_F(StreamingUDPThreadedClientTester, subscribe_host_error)
{
    string st = "outTables_" + getRandString(10);
    UDPCT::createSharedTableAndReplay(st, 1000);
    ThreadedClient client(scfg);
    EXPECT_ANY_THROW(client.subscribe("abcd", port300, [](Message msg){}, st, DEFAULT_ACTION_NAME, 0));
}

TEST_F(StreamingUDPThreadedClientTester, subscribe_port_error)
{
    string st = "outTables_" + getRandString(10);
    UDPCT::createSharedTableAndReplay(st, 1000);
    ThreadedClient client(scfg);

    EXPECT_ANY_THROW(client.subscribe(hostName, -1, [](Message msg){},  st, DEFAULT_ACTION_NAME, 0));
}

TEST_F(StreamingUDPThreadedClientTester, subscribe_username_error)
{
    string st = "outTables_" + getRandString(10);
    UDPCT::createSharedTableAndReplay(st, 1000);
    ThreadedClient client(scfg);

    EXPECT_ANY_THROW(client.subscribe(hostName, port300, [](Message msg){}, st, DEFAULT_ACTION_NAME, 0, true, nullptr, false, false, "sjkdwae", "123456"));
}

TEST_F(StreamingUDPThreadedClientTester, subscribe_password_error)
{
    string st = "outTables_" + getRandString(10);
    UDPCT::createSharedTableAndReplay(st, 1000);
    ThreadedClient client(scfg);

    EXPECT_ANY_THROW(client.subscribe(hostName, port300, [](Message msg){}, st, DEFAULT_ACTION_NAME, 0, true, nullptr, false, false, "admin", "111111"));
}

TEST_F(StreamingUDPThreadedClientTester, subscribe_offset_negative)
{
    string st = "outTables_" + getRandString(10);
    UDPCT::createSharedTableAndReplay(st, 100);
    ConstantSP extra_data = conn300->run("select top 10 * from " + st);
    ThreadedClient client(scfg);
    Signal notify;
    Mutex mtx;
    string res_tab = "res_UDPCT";
    int msg_total = 0;
    AutoFitTableAppender appender("", res_tab, conn);

    auto handler = [&](Message msg){
        LockGuard<Mutex> lock(&mtx);
        msg_total += msg->get(0)->size();
        TableSP tmp = UDPmsgToTable(msg);
        int rows = appender.append(tmp);
        // EXPECT_EQ(rows, msg->get(0)->size());
        // EXPECT_EQ(msg.getOffset(), 100);
        
        if (msg_total == 10)
        {
            notify.set();
        }
    };

    auto thread = client.subscribe(hostName, port300, handler, st, DEFAULT_ACTION_NAME, -1);
    Util::sleep(1000);
    vector<ConstantSP> args = {extra_data};
    conn300->run("append!{" + st + "}", args);
    cout << "insert extra data finished!\n";

    notify.wait();
    client.unsubscribe(hostName, port300, st, DEFAULT_ACTION_NAME);
    Util::sleep(2000);
    conn300->upload("extraData", args[0]);
    EXPECT_TRUE(conn300->run("ex = exec * from extraData order by timestampv;"
                        "res = exec * from res_UDPCT order by timestampv;"
                        "all(each(eqObj, ex.values(), res.values()))")->getBool());
    EXPECT_FALSE(conn300->run("`"+st+" in (exec tableName from getStreamingStat().udpPubTables)")->getBool());

}

TEST_F(StreamingUDPThreadedClientTester, subscribe_offset_gt0)
{
    string st = "outTables_" + getRandString(10);
    UDPCT::createSharedTableAndReplay(st, 100);
    ThreadedClient client(scfg);
    Signal notify;
    Mutex mtx;
    string res_tab = "res_UDPCT";
    int msg_total = 0;
    AutoFitTableAppender appender("", res_tab, conn);

    auto handler = [&](Message msg){
        LockGuard<Mutex> lock(&mtx);
        // cout << msg->getString() << endl;
        msg_total += msg->get(0)->size();
        TableSP tmp = UDPmsgToTable(msg);
        int rows = appender.append(tmp);
        EXPECT_EQ(rows, msg->get(0)->size());
        
        if (msg_total == 100-10)
        {
            notify.set();
        }
    };

    auto thread = client.subscribe(hostName, port300, handler, st, DEFAULT_ACTION_NAME, 10);
    notify.wait();
    client.unsubscribe(hostName, port300, st, DEFAULT_ACTION_NAME);
    Util::sleep(2000);
    EXPECT_TRUE(conn300->run("ex = (exec * from ex_UDPCT order by datetimev)[10:];"
                        "res = exec * from res_UDPCT order by timestampv;"
                        "all(each(eqObj, ex.values(), res.values()))")->getBool());
    EXPECT_FALSE(conn300->run("`"+st+" in (exec tableName from getStreamingStat().udpPubTables)")->getBool());
}



TEST_F(StreamingUDPThreadedClientTester, subscribe_filter)
{
    GTEST_SKIP() << "filter not support yet.";
    string st = "outTables_" + getRandString(10);
    UDPCT::createSharedTableAndReplay(st, 100);
    ThreadedClient client(scfg);
    Signal notify;
    Mutex mtx;
    string res_tab = "res_UDPCT";
    int msg_total = 0;
    AutoFitTableAppender appender("", res_tab, conn);

    auto handler = [&](Message msg){
        LockGuard<Mutex> lock(&mtx);
        // cout << msg->getString() << endl;
        msg_total += msg->get(0)->size();
        TableSP tmp = UDPmsgToTable(msg);
        int rows = appender.append(tmp);
        EXPECT_EQ(rows, msg->get(0)->size());
        
        if (msg_total == 34)
        {
            notify.set();
        }
    };

    VectorSP filters = Util::createVector(DT_STRING, 1);
    filters->setString(0, "a");
    // cout << "filters: " << filters->getString() << endl;
    auto thread = client.subscribe(hostName, port300, handler, st, DEFAULT_ACTION_NAME, 0, true, filters);
    notify.wait();
    client.unsubscribe(hostName, port300, st, DEFAULT_ACTION_NAME);
    Util::sleep(2000);
    EXPECT_TRUE(conn300->run("ex = exec * from ex_UDPCT where sym = `a order by timestampv;"
                        "res = exec * from res_UDPCT order by timestampv;"
                        "all(each(eqObj, ex.values(), res.values()))")->getBool());
    EXPECT_FALSE(conn300->run("`"+st+" in (exec tableName from getStreamingStat().udpPubTables)")->getBool());
}


TEST_F(StreamingUDPThreadedClientTester, subscribe_twice_with_same_action)
{
    string st = "outTables_" + getRandString(10);
    UDPCT::createSharedTableAndReplay(st, 2000);
    ThreadedClient client(scfg);
    Signal notify;
    Mutex mtx;
    string res_tab = "res_UDPCT";
    int msg_total = 0;
    AutoFitTableAppender appender("", res_tab, conn);

    auto handler = [&](Message msg){
        LockGuard<Mutex> lock(&mtx);
        // cout << msg->getString() << endl;
        msg_total += msg->get(0)->size();
        TableSP tmp = UDPmsgToTable(msg);
        int rows = appender.append(tmp);
        EXPECT_EQ(rows, msg->get(0)->size());
        
        if (msg_total == 2000)
        {
            notify.set();
        }
    };

    auto thread = client.subscribe(hostName, port300, handler, st, DEFAULT_ACTION_NAME, 0);
    EXPECT_ANY_THROW(client.subscribe(hostName, port300, handler, st, DEFAULT_ACTION_NAME, 0));
    notify.wait();
    client.unsubscribe(hostName, port300, st, DEFAULT_ACTION_NAME);
    Util::sleep(2000);
    EXPECT_TRUE(conn300->run("ex = exec * from ex_UDPCT order by timestampv;"
                        "res = exec * from res_UDPCT order by timestampv;"
                        "all(each(eqObj, ex.values(), res.values()))")->getBool());
    EXPECT_FALSE(conn300->run("`"+st+" in (exec tableName from getStreamingStat().udpPubTables)")->getBool());
}


TEST_F(StreamingUDPThreadedClientTester, unsubscribe_twice)
{
    string st = "outTables_" + getRandString(10);
    UDPCT::createSharedTableAndReplay(st, 2000);
    ThreadedClient client(scfg);
    Signal notify;
    Mutex mtx;
    string res_tab = "res_UDPCT";
    int msg_total = 0;
    AutoFitTableAppender appender("", res_tab, conn);

    auto handler = [&](Message msg){
        LockGuard<Mutex> lock(&mtx);
        // cout << msg->getString() << endl;
        msg_total += msg->get(0)->size();
        TableSP tmp = UDPmsgToTable(msg);
        int rows = appender.append(tmp);
        EXPECT_EQ(rows, msg->get(0)->size());
        
        if (msg_total == 2000)
        {
            notify.set();
        }
    };

    auto thread = client.subscribe(hostName, port300, handler, st, DEFAULT_ACTION_NAME, 0);
    notify.wait();
    client.unsubscribe(hostName, port300, st, DEFAULT_ACTION_NAME);
    EXPECT_ANY_THROW(client.unsubscribe(hostName, port300, st, DEFAULT_ACTION_NAME));
    Util::sleep(2000);
    EXPECT_TRUE(conn300->run("ex = exec * from ex_UDPCT order by timestampv;"
                        "res = exec * from res_UDPCT order by timestampv;"
                        "all(each(eqObj, ex.values(), res.values()))")->getBool());
    EXPECT_FALSE(conn300->run("`"+st+" in (exec tableName from getStreamingStat().udpPubTables)")->getBool());
}


TEST_F(StreamingUDPThreadedClientTester, not_unsubscribe_after_subscribe)
{
    string st = "outTables_" + getRandString(10);
    UDPCT::createSharedTableAndReplay(st, 2000);
    ThreadedClient client(scfg);
    Signal notify;
    Mutex mtx;
    string res_tab = "res_UDPCT";
    int msg_total = 0;
    AutoFitTableAppender appender("", res_tab, conn);

    auto handler = [&](Message msg){
        LockGuard<Mutex> lock(&mtx);
        // cout << msg->getString() << endl;
        msg_total += msg->get(0)->size();
        TableSP tmp = UDPmsgToTable(msg);
        int rows = appender.append(tmp);
        EXPECT_EQ(rows, msg->get(0)->size());
        
        if (msg_total == 2000)
        {
            notify.set();
        }
    };

    auto thread = client.subscribe(hostName, port300, handler, st, DEFAULT_ACTION_NAME, 0);
    notify.wait();
    EXPECT_TRUE(conn300->run("ex = exec * from ex_UDPCT order by timestampv;"
                        "res = exec * from res_UDPCT order by timestampv;"
                        "all(each(eqObj, ex.values(), res.values()))")->getBool());
    EXPECT_TRUE(conn300->run("`"+st+" in (exec tableName from getStreamingStat().udpPubTables)")->getBool());
    conn300->run("\
        info = exec * from getStreamingStat().udpPubTables where tableName=`"+st+";\
        ip1 = (exec channel.split(':')[0] from info)[0];\
        port1 = (exec channel.split(':')[1] from info)[0];\
        stopPublishTable(ip1, port1, `"+st+",`"+DEFAULT_ACTION_NAME+",,true)");
    Util::sleep(1000);
    EXPECT_FALSE(conn300->run("`"+st+" in (exec tableName from getStreamingStat().udpPubTables)")->getBool());
    EXPECT_ANY_THROW(client.unsubscribe(hostName, port300, st, DEFAULT_ACTION_NAME));
}



TEST_F(StreamingUDPThreadedClientTester, subscribe_actionName_null)
{
    string st = "outTables_" + getRandString(10);
    UDPCT::createSharedTableAndReplay(st, 2000);
    ThreadedClient client(scfg);
    Signal notify;
    Mutex mtx;
    string res_tab = "res_UDPCT";
    int msg_total = 0;
    AutoFitTableAppender appender("", res_tab, conn);

    auto handler = [&](Message msg){
        LockGuard<Mutex> lock(&mtx);
        // cout << msg->getString() << endl;
        msg_total += msg->get(0)->size();
        TableSP tmp = UDPmsgToTable(msg);
        int rows = appender.append(tmp);
        EXPECT_EQ(rows, msg->get(0)->size());
        
        if (msg_total == 2000)
        {
            notify.set();
        }
    };

    auto thread = client.subscribe(hostName, port300, handler, st, "", 0);
    notify.wait();
    client.unsubscribe(hostName, port300, st, "");
    Util::sleep(2000);
    EXPECT_TRUE(conn300->run("ex = exec * from ex_UDPCT order by timestampv;"
                        "res = exec * from res_UDPCT order by timestampv;"
                        "all(each(eqObj, ex.values(), res.values()))")->getBool());
    EXPECT_FALSE(conn300->run("`"+st+" in (exec tableName from getStreamingStat().udpPubTables)")->getBool());
}


TEST_F(StreamingUDPThreadedClientTester, subscribe_multithread)
{
    string st = "outTables_" + getRandString(10);
    UDPCT::createSharedTableAndReplay(st, 2000);
    int num_clients = 10;

    vector<ThreadedClient> clients(num_clients);
    for (int i = 0; i < 10; i++)
    {
        clients[i] = ThreadedClient(scfg);
    }

    vector<string> res_tabs = {"res_UDPCT0", "res_UDPCT1", "res_UDPCT2", "res_UDPCT3", "res_UDPCT4", "res_UDPCT5", "res_UDPCT6", "res_UDPCT7", "res_UDPCT8", "res_UDPCT9"};
    for (int i = 0; i < num_clients; i++)
    {
        string res_tab = res_tabs[i];
        auto handler = [=](Message msg){
            AutoFitTableAppender appender("", res_tab, conn);
            TableSP tmp = UDPmsgToTable(msg);
            int rows = appender.append(tmp);
            EXPECT_EQ(rows, msg->get(0)->size());
        };
        string action = "action"+to_string(i);
        conn300->run("t= select * from res_UDPCT;share t as " + res_tabs[i]);
        clients[i].subscribe(hostName, port300, handler, st, action, 0);
    }

    function<void()> wait_all = [&]() {
        int cost_second = 0;
        while (cost_second < 10)
        {
            conn300->run("st = array(ANY)");
            for (auto i = 0; i < res_tabs.size(); i++){
                conn300->run("st.append!("+res_tabs[i]+")");
            }
            if (conn300->run("all(each(rows, st) == 2000)")->getBool())
            {
                break;
            }
            Util::sleep(1000);
            cost_second++;
        }
    };
    wait_all();
    // notify.wait();
    for (int i = 0; i < num_clients; i++)
    {
        clients[i].unsubscribe(hostName, port300, st, "action"+to_string(i));
        EXPECT_TRUE(conn300->run("ex = exec * from ex_UDPCT order by timestampv;"
                            "res = exec * from res_UDPCT"+to_string(i)+" order by timestampv;"
                            "all(each(eqObj, ex.values(), res.values()))")->getBool());
    }
    EXPECT_FALSE(conn300->run("`"+st+" in (exec tableName from getStreamingStat().udpPubTables)")->getBool());

}

TEST_F(StreamingUDPThreadedClientTester, subscribe_with_alldataTypes)
{
    string st = "outTables_" + getRandString(10);
    UDPCT::createSharedTableAndReplay_withAllDataType(st);
    ThreadedClient client(scfg);
    Signal notify;
    Mutex mtx;
    string res_tab = "res_UDPCT";
    int msg_total = 0;
    AutoFitTableAppender appender("", res_tab, conn);

    auto handler = [&](Message msg){
        LockGuard<Mutex> lock(&mtx);
        // cout << msg->getString() << endl;
        msg_total += msg->get(0)->size();
        TableSP tmp = UDPmsgToTable(msg);
        int rows = appender.append(tmp);
        EXPECT_EQ(rows, msg->get(0)->size());
        
        if (msg_total == 1000)
        {
            notify.set();
        }
    };

    auto thread = client.subscribe(hostName, port300, handler, st, DEFAULT_ACTION_NAME, 0);
    notify.wait();
    client.unsubscribe(hostName, port300, st, DEFAULT_ACTION_NAME);
    Util::sleep(1000);
    EXPECT_TRUE(conn300->run("ex = exec * from ex_UDPCT order by ind;"
                        "res = exec * from res_UDPCT order by ind;"
                        "all(each(eqObj, ex.values(), res.values()))")->getBool());
    EXPECT_FALSE(conn300->run("`"+st+" in (exec tableName from getStreamingStat().udpPubTables)")->getBool());
}



/*
TEST_F(StreamingUDPThreadedClientTester, subscribe_with_arrayVector)
{
    string st = "outTables_" + getRandString(10);
    UDPCT::createSharedTableAndReplay_withArrayVector(st);
    ThreadedClient client(scfg);
    Signal notify;
    Mutex mtx;
    string res_tab = "res_UDPCT";
    int msg_total = 0;
    AutoFitTableAppender appender("", res_tab, conn);

    auto handler = [&](Message msg){
        LockGuard<Mutex> lock(&mtx);
        // cout << msg->getString() << endl;
        msg_total += msg->get(0)->size();
        TableSP tmp = UDPmsgToTable(msg);
        int rows = appender.append(tmp);
        EXPECT_EQ(rows, msg->get(0)->size());
        
        if (msg_total == 1000)
        {
            notify.set();
        }
    };

    auto thread = client.subscribe(hostName, port300, handler, st, DEFAULT_ACTION_NAME, 0);
    notify.wait();
    client.unsubscribe(hostName, port300, st, DEFAULT_ACTION_NAME);
    Util::sleep(1000);
    EXPECT_TRUE(conn300->run("ex = exec * from ex_UDPCT order by ts;"
                        "res = exec * from res_UDPCT order by ts;"
                        "all(each(eqObj, ex.values(), res.values()))")->getBool());
    EXPECT_FALSE(conn300->run("`"+st+" in (exec tableName from getStreamingStat().udpPubTables)")->getBool());
}
*/

class StreamingUDPThreadedClientTester_realtime : public testing::Test, public ::testing::WithParamInterface<pair<DATA_TYPE, string>>
{
public:
    // Suite
    static void SetUpTestCase()
    {
        // DBConnection conn;

        conn300->initialize();
        bool ret = conn300->connect(hostName, port300, "admin", "123456");
        if (!ret)
        {
            cout << "Failed to connect to the server" << endl;
        }
        else
        {
            cout << "connect to " + hostName + ":" + std::to_string(port300) << endl;
        }
    }
    static void TearDownTestCase()
    {
        usedPorts.clear();
        conn300->close();
    }

    // Case
    virtual void SetUp()
    {
        cout << "check connect...";
        try
        {
            ConstantSP res = conn300->run("1+1");
        }
        catch(const std::exception& e)
        {
            conn300->connect(hostName, port300, "admin", "123456");
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
            "share table(1:0, colName, colType) as res_UDPCT;go;";
        conn300->run(s);
    };
    void insertData(DBConnection& conn, const string& name, const string& colScript){
        string s = 
            "row_num = 1000;"
            "col0 = now()..(now()+row_num-1);"
            "col1 = "+colScript+";"
            "for (i in 0..(row_num-1)){insert into "+name+" values([col0[i]], [col1[i]]);};";
        conn300->run(s);
    };
public:
    static StreamingClientConfig scfg_realtime;
};
StreamingClientConfig StreamingUDPThreadedClientTester_realtime::scfg_realtime;


INSTANTIATE_TEST_SUITE_P(basicTypes, StreamingUDPThreadedClientTester_realtime, testing::ValuesIn(StreamingUDPThreadedClientTester_realtime::getData()));
INSTANTIATE_TEST_SUITE_P(arrayVectorTypes, StreamingUDPThreadedClientTester_realtime, testing::ValuesIn(StreamingUDPThreadedClientTester_realtime::getAVData()));
TEST_P(StreamingUDPThreadedClientTester_realtime, test_realtime)
{
    DATA_TYPE ttp = GetParam().first;
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

    const string st = "test_realtime_" + getRandString(10);
    createST(conn, st, typeString);
    ThreadedClient client(scfg_realtime);
    Signal notify;
    Mutex mtx;
    string res_tab = "res_UDPCT";
    int msg_total = 0;
    AutoFitTableAppender appender("", res_tab, conn);

    auto handler = [&](Message msg){
        LockGuard<Mutex> lock(&mtx);
        // cout << msg->getString() << endl;
        msg_total += msg->get(0)->size();
        // cout << "msg_toal: " << msg_total << endl;
        TableSP tmp = UDPmsgToTable(msg);
        int rows = appender.append(tmp);
        EXPECT_EQ(rows, msg->get(0)->size());

        if (msg_total == 1000)
        {
            notify.set();
        }
    };

    string dataScript = GetParam().second;
    std::thread th = std::thread([&]() {
        insertData(conn, st, dataScript);
    });

    auto thread = client.subscribe(hostName, port300, handler, st, DEFAULT_ACTION_NAME, 0);

    th.join();
    notify.wait();
    Util::sleep(1000);
    cout << "msg_toal: " << msg_total << endl;
    EXPECT_TRUE(conn300->run("re = select * from res_UDPCT order by ts;\
                        ex = select * from "+st+" order by ts;\
                        all(each(eqObj, re.values(), ex.values()))")->getBool());
    EXPECT_FALSE(conn300->run("`"+st+" in (exec tableName from getStreamingStat().udpPubTables)")->getBool());
}


    TEST_F(StreamingUDPThreadedClientTester, test_subscribe_onehandler)
    {
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_UDPCT", conn);

        
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

        ThreadedClient threadedClient = ThreadedClient(scfg);
        auto thread = threadedClient.subscribe(hostName, port300, onehandler, st, "mutiSchemaOne", 0);
        notify.wait();

        cout << "total size: " << msg_total << endl;
        threadedClient.unsubscribe(hostName, port300, st, "mutiSchemaOne");
        Util::sleep(1000);
        EXPECT_TRUE(conn300->run("re = select * from res_UDPCT order by datetimev;\
                            ex = select * from ex_UDPCT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn300->run("(exec count(*) from getStreamingStat().udpPubTables where tableName =`"+st+") ==0")->getBool());
        EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
        EXPECT_EQ(msg_total, 1000);
    }

    TEST_F(StreamingUDPThreadedClientTester, test_subscribe_batchhandler)
    {
        GTEST_SKIP() << "not support batch handler yet";
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_UDPCT", conn);

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


        ThreadedClient threadedClient = ThreadedClient(scfg);
        auto thread = threadedClient.subscribe(hostName, port300, batchhandler, st, "mutiSchemaBatch", 0);
        notify.wait();

        cout << "total size: " << msg_total << endl;
        threadedClient.unsubscribe(hostName, port300, st, "mutiSchemaBatch");

        EXPECT_TRUE(conn300->run("re = select * from res_UDPCT order by datetimev;\
                            ex = select * from ex_UDPCT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn300->run("(exec count(*) from getStreamingStat().udpPubTables where tableName =`"+st+") ==0")->getBool());
        EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
        EXPECT_EQ(msg_total, 1000);

    }

    TEST_F(StreamingUDPThreadedClientTester, test_subscribeUnsubscribe_notExistTable)
    {

        ThreadedClient threadedClient = ThreadedClient(scfg);
        EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port300, [](Message msg){}, "st_notExist", "actionTest"));
    }

    TEST_F(StreamingUDPThreadedClientTester, test_subscribe_onehandler_tableNameNull)
    {
        auto onehandler = [](Message msg){};


        ThreadedClient threadedClient = ThreadedClient(scfg);

        EXPECT_ANY_THROW(auto thread = threadedClient.subscribe(hostName, port300, onehandler, "", "cppStreamingAPI", 0, false));
    }

    TEST_F(StreamingUDPThreadedClientTester, test_subscribe_batchhandler_tableNameNull)
    {
        auto batchhandler = [](vector<Message> msgs){};


        ThreadedClient threadedClient = ThreadedClient(scfg);
        EXPECT_ANY_THROW(auto thread = threadedClient.subscribe(hostName, port300, batchhandler, "", "cppStreamingAPI", 0, false));
    }

    TEST_F(StreamingUDPThreadedClientTester, test_subscribe_onehandler_offsetNegative)
    {
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        auto onehandler = [&](Message msg)
        {
            msg_total += 1;
            // cout << msg->getString() << endl;
        };


        ThreadedClient threadedClient = ThreadedClient(scfg);
        auto thread = threadedClient.subscribe(hostName, port300, onehandler, st, "actionTest", -1);

        cout << "total size: " << msg_total << endl;
        threadedClient.unsubscribe(hostName, port300, st, "actionTest");
        threadedClient.exit();
        EXPECT_TRUE(threadedClient.isExit());
        Util::sleep(1000);
        EXPECT_TRUE(conn300->run("(exec count(*) from getStreamingStat().udpPubTables where tableName =`"+st+") ==0")->getBool());
        EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
        EXPECT_EQ(msg_total, 0);
    }

    TEST_F(StreamingUDPThreadedClientTester, test_subscribe_batchhandler_offsetNegative)
    {
        GTEST_SKIP() << "not support batch handler yet";
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        auto batchhandler = [&](vector<Message> msgs)
        {
            for (auto &msg : msgs)
            {
                msg_total += 1;
                // cout  << msg->getString() << endl;
            }
        };

        ThreadedClient threadedClient = ThreadedClient(scfg);
        auto thread = threadedClient.subscribe(hostName, port300, batchhandler, st, "actionTest", -1);

        cout << "total size: " << msg_total << endl;
        threadedClient.unsubscribe(hostName, port300, st, "actionTest");
        threadedClient.exit();
        EXPECT_TRUE(threadedClient.isExit());
        Util::sleep(1000);
        EXPECT_TRUE(conn300->run("(exec count(*) from getStreamingStat().udpPubTables where tableName =`"+st+") ==0")->getBool());
        EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
        EXPECT_EQ(msg_total, 0);
    }

    TEST_F(StreamingUDPThreadedClientTester,test_subscribe_onehandler_offsetInMiddle)
    {
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay(st, 10);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;

        AutoFitTableAppender appender("", "res_UDPCT", conn);

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


        ThreadedClient threadedClient = ThreadedClient(scfg);
        auto thread = threadedClient.subscribe(hostName, port300, onehandler, st, "actionTest", 5);
        notify.wait();
        cout << "total size: " << msg_total << endl;
        threadedClient.unsubscribe(hostName, port300, st, "actionTest");
        threadedClient.exit();
        EXPECT_TRUE(threadedClient.isExit());
        EXPECT_TRUE(conn300->run(("eqObj(res_UDPCT.rows(), 5)"))->getBool());
        EXPECT_TRUE(conn300->run("re = select * from res_UDPCT order by datetimev;\
                            ex = select * from ex_UDPCT order by datetimev limit 5, 5;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn300->run("(exec count(*) from getStreamingStat().udpPubTables where tableName =`"+st+") ==0")->getBool());
        EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
        EXPECT_EQ(msg_total, 5);
    }

    TEST_F(StreamingUDPThreadedClientTester, test_subscribe_batchhandler_offsetInMiddle)
    {
        GTEST_SKIP() << "not support batch handler yet";
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay(st, 10);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;

        AutoFitTableAppender appender("", "res_UDPCT", conn);

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


        ThreadedClient threadedClient = ThreadedClient(scfg);
        auto thread = threadedClient.subscribe(hostName, port300, batchhandler, st, "actionTest", 5);
        notify.wait();
        cout << "total size: " << msg_total << endl;
        threadedClient.unsubscribe(hostName, port300, st, "actionTest");
        threadedClient.exit();
        EXPECT_TRUE(threadedClient.isExit());
        EXPECT_TRUE(conn300->run(("eqObj(res_UDPCT.rows(), 5)"))->getBool());
        EXPECT_TRUE(conn300->run("re = select * from res_UDPCT order by datetimev;\
                            ex = select * from ex_UDPCT order by datetimev limit 5, 5;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn300->run("(exec count(*) from getStreamingStat().udpPubTables where tableName =`"+st+") ==0")->getBool());
        EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
        EXPECT_EQ(msg_total, 5);
    }


    TEST_F(StreamingUDPThreadedClientTester, test_subscribe_onehandler_filter)
    {
        GTEST_SKIP() << "filter not supported yet";
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_UDPCT", conn);
        int target_rows = conn300->run("(exec count(*) from ex_UDPCT where sym='b')[0]")->getInt();

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


        ThreadedClient threadedClient = ThreadedClient(scfg);
        VectorSP filter = Util::createVector(DT_SYMBOL, 1, 1);
        filter->setString(0, "b");
        auto thread = threadedClient.subscribe(hostName, port300, onehandler, st, "actionTest", 0, true, filter);
        notify.wait();

        cout << "total size: " << msg_total << endl;
        threadedClient.unsubscribe(hostName, port300, st, "actionTest");
        threadedClient.exit();
        EXPECT_TRUE(threadedClient.isExit());
        EXPECT_TRUE(conn300->run("re = exec sym from res_UDPCT; all(re == `b)")->getBool());
        EXPECT_TRUE(conn300->run("(exec count(*) from getStreamingStat().udpPubTables where tableName =`"+st+") ==0")->getBool());
        EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
        EXPECT_TRUE(msg_total > 0);
    }

    TEST_F(StreamingUDPThreadedClientTester, test_subscribe_batchhandler_filter)
    {
        GTEST_SKIP() << "filter not supported yet";
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_UDPCT", conn);

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

        ThreadedClient threadedClient = ThreadedClient(scfg);
        VectorSP filter = Util::createVector(DT_SYMBOL, 1, 1);
        filter->setString(0, "b");

        auto thread = threadedClient.subscribe(hostName, port300, batchhandler, st, "actionTest", 0, true, filter);
        Util::sleep(2000);

        cout << "total size: " << msg_total << endl;
        threadedClient.unsubscribe(hostName, port300, st, "actionTest");
        threadedClient.exit();
        EXPECT_TRUE(threadedClient.isExit());
        EXPECT_TRUE(conn300->run("re = exec sym from res_UDPCT; all(re == `b)")->getBool());
        EXPECT_TRUE(conn300->run("(exec count(*) from getStreamingStat().udpPubTables where tableName =`"+st+") ==0")->getBool());
        EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
        EXPECT_TRUE(msg_total > 0);
    }

    TEST_F(StreamingUDPThreadedClientTester, test_subscribe_onehandler_msgAsTable)
    {
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_UDPCT", conn);

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


        ThreadedClient threadedClient = ThreadedClient(scfg);
        auto thread = threadedClient.subscribe(hostName, port300, onehandler, st, "actionTest", 0, false, nullptr, true);
        notify.wait();

        cout << "total size: " << msg_total << endl;
        threadedClient.unsubscribe(hostName, port300, st, "actionTest");
        threadedClient.exit();
        EXPECT_TRUE(threadedClient.isExit());
        EXPECT_TRUE(conn300->run("re = select * from res_UDPCT order by datetimev;\
                            ex = select * from ex_UDPCT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn300->run("(exec count(*) from getStreamingStat().udpPubTables where tableName =`"+st+") ==0")->getBool());
        EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
        EXPECT_EQ(msg_total, 1000);
    }

    TEST_F(StreamingUDPThreadedClientTester, test_subscribe_batchhandler_msgAsTable)
    {
        GTEST_SKIP() << "batchhandler not support yet.";
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay(st, 10000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_UDPCT", conn);

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


        ThreadedClient threadedClient = ThreadedClient(scfg);
        auto thread = threadedClient.subscribe(hostName, port300, batchhandler, st, "actionTest", 0, false, nullptr, false, 2000, 0.1, true);
        notify.wait();

        cout << "total size: " << msg_total << endl;
        threadedClient.unsubscribe(hostName, port300, st, "actionTest");
        threadedClient.exit();
        EXPECT_TRUE(threadedClient.isExit());
        EXPECT_TRUE(conn300->run("re = select * from res_UDPCT order by datetimev;\
                            ex = select * from ex_UDPCT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn300->run("(exec count(*) from getStreamingStat().udpPubTables where tableName =`"+st+") ==0")->getBool());
        EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
        EXPECT_EQ(msg_total, 10000);
    }

    TEST_F(StreamingUDPThreadedClientTester, test_subscribe_onehandler_allowExists)
    {
        GTEST_SKIP() << "server还没有修复allowExists的问题, jira: https://dolphindb1.atlassian.net/browse/D20-14283";
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay(st, 1000);
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


        ThreadedClient threadedClient = ThreadedClient(scfg);
        ThreadedClient threadedClient2(scfg);
        auto thread = threadedClient.subscribe(hostName, port300, onehandler, st, "actionTest", 0, false, nullptr, false, true);
        auto thread2 = threadedClient2.subscribe(hostName, port300, onehandler2, st, "actionTest", 0, false, nullptr, false, true);
        notify.wait();

        cout << "total size: " << msg_total << endl;
        threadedClient.unsubscribe(hostName, port300, st, "actionTest");
        threadedClient2.unsubscribe(hostName, port300, st, "actionTest");
        threadedClient.exit();
        EXPECT_TRUE(threadedClient.isExit());
        threadedClient2.exit();
        EXPECT_TRUE(threadedClient2.isExit());

        Util::sleep(1000);
        cout << conn300->run("exec * from getStreamingStat()[`pubConns]")->getString() << endl;
        EXPECT_TRUE(conn300->run("(exec count(*) from getStreamingStat().udpPubTables where tableName =`"+st+") ==0")->getBool());
        EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
        EXPECT_EQ(threadedClient2.getQueueDepth(thread2), 0);
        EXPECT_EQ(msg_total, 1000);
        EXPECT_EQ(msg_total2, 1000);
    }

    TEST_F(StreamingUDPThreadedClientTester, test_subscribe_batchhandler_allowExists)
    {
        GTEST_SKIP() << "server还没有修复allowExists的问题, jira: https://dolphindb1.atlassian.net/browse/D20-14283";
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay(st, 1000);
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


        ThreadedClient threadedClient = ThreadedClient(scfg);
        ThreadedClient threadedClient2(scfg);
        auto thread = threadedClient.subscribe(hostName, port300, batchhandler, st, "actionTest", 0, false, nullptr, false, true);
        auto thread2 = threadedClient2.subscribe(hostName, port300, batchhandler2, st, "actionTest", 0, false, nullptr, false, true);
        notify.wait();

        cout << "total size: " << msg_total << endl;
        threadedClient.unsubscribe(hostName, port300, st, "actionTest");
        threadedClient.exit();
        EXPECT_TRUE(threadedClient.isExit());
        threadedClient2.unsubscribe(hostName, port300, st, "actionTest");
        threadedClient2.exit();
        EXPECT_TRUE(threadedClient2.isExit());
        Util::sleep(1000);
        EXPECT_TRUE(conn300->run("(exec count(*) from getStreamingStat().udpPubTables where tableName =`"+st+") ==0")->getBool());
        EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
        EXPECT_EQ(msg_total, 1000);
        EXPECT_EQ(threadedClient2.getQueueDepth(thread2), 0);
        EXPECT_EQ(msg_total2, 1000);
    }

    TEST_F(StreamingUDPThreadedClientTester, test_subscribe_resub_false)
    {
        auto batchhandler = [&](vector<Message> msgs){};


        ThreadedClient threadedClient = ThreadedClient(scfg);
        EXPECT_ANY_THROW(auto thread = threadedClient.subscribe(hostName, port300, batchhandler, "st", "actionTest", 0, false));
    }

    TEST_F(StreamingUDPThreadedClientTester, test_subscribe_resub_true)
    {
        GTEST_SKIP() << "resub not support yet.";
        vector<Message> msgs;
        auto batchhandler = [&](vector<Message> ms)
        {
            for (auto &msg : ms)
            {
                msgs.push_back(msg);
            }
        };

        string st = "st_" + getRandString(10);
        conn300->run("share streamTable(1:0, `sym`val, [SYMBOL, INT]) as "+st+";tableInsert("+st+", `sym1, 1)");

        ThreadedClient threadedClient = ThreadedClient(scfg);
        auto thread = threadedClient.subscribe(hostName, port300, batchhandler, st, "resubTest", 0, true);
        Util::sleep(2000);
        conn300->run("\
            info = exec * from getStreamingStat().udpPubTables where tableName=`"+st+";\
            ip1 = (exec channel.split(':')[0] from info)[0];\
            port1 = (exec channel.split(':')[1] from info)[0];\
            stopPublishTable(ip1, port1, `"+st+",`"+DEFAULT_ACTION_NAME+",,true)");
        Util::sleep(1000);
        conn300->run("tableInsert("+st+", `sym2, 2)");
        Util::sleep(1000);
        threadedClient.unsubscribe(hostName, port300, st, "resubTest");
        threadedClient.exit();
        EXPECT_TRUE(threadedClient.isExit());
        EXPECT_EQ(msgs.size(), 2);
    }

    TEST_F(StreamingUDPThreadedClientTester, test_subscribe_batchhandler_batchSize)
    {
        GTEST_SKIP() << "batchhandler not support yet.";
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_UDPCT", conn);

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


        ThreadedClient threadedClient = ThreadedClient(scfg);
        auto thread = threadedClient.subscribe(hostName, port300, batchhandler, st, "actionTest", 0, true, nullptr, false, 200);
        notify.wait();

        cout << "total size: " << msg_total << endl;
        threadedClient.unsubscribe(hostName, port300, st, "actionTest");
        threadedClient.exit();
        EXPECT_TRUE(threadedClient.isExit());
        EXPECT_TRUE(conn300->run("re = select * from res_UDPCT order by datetimev;\
                            ex = select * from ex_UDPCT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn300->run("(exec count(*) from getStreamingStat().udpPubTables where tableName =`"+st+") ==0")->getBool());
        EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
        EXPECT_EQ(msg_total, 1000);
    }

    TEST_F(StreamingUDPThreadedClientTester, test_subscribe_batchhandler_throttle)
    {
        GTEST_SKIP() << "batchhandler not support yet.";
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay(st, 10);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_UDPCT", conn);
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


        ThreadedClient threadedClient = ThreadedClient(scfg);
        auto thread = threadedClient.subscribe(hostName, port300, batchhandler, st, "actionTest", 0, true, nullptr, false, 5000, 2.5);
        start = Util::getEpochTime();
        notify.wait();

        cout << "total size: " << msg_total << endl;
        threadedClient.unsubscribe(hostName, port300, st, "actionTest");
        threadedClient.exit();
        EXPECT_TRUE(threadedClient.isExit());
        EXPECT_TRUE(conn300->run("re = select * from res_UDPCT order by datetimev;\
                            ex = select * from ex_UDPCT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn300->run("(exec count(*) from getStreamingStat().udpPubTables where tableName =`"+st+") ==0")->getBool());
        EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
        EXPECT_EQ(msg_total, 10);

        auto duration = end - start;
        EXPECT_TRUE(duration >= 2500);
    }

    TEST_F(StreamingUDPThreadedClientTester, test_subscribe_onehandler_hostNull)
    {
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        auto onehandler = [&](Message msg)
        {
            msg_total += 1;
            // cout << msg->getString() << endl;
        };


        ThreadedClient threadedClient = ThreadedClient(scfg);
        EXPECT_ANY_THROW(threadedClient.subscribe("", port300, onehandler, st, "actionTest", 0));

    }

    TEST_F(StreamingUDPThreadedClientTester, test_subscribe_batchhandler_hostNull)
    {
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        auto batchhandler = [&](vector<Message> msgs)
        {
            for (auto &msg : msgs)
            {
                msg_total += 1;
                // cout  << msg->getString() << endl;
            }
        };


        ThreadedClient threadedClient = ThreadedClient(scfg);
        EXPECT_ANY_THROW(threadedClient.subscribe("", port300, batchhandler, st, "actionTest", 0));

    }

    TEST_F(StreamingUDPThreadedClientTester, test_subscribe_onehandler_portNull)
    {
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        auto onehandler = [&](Message msg)
        {
            msg_total += 1;
            // cout << msg->getString() << endl;
        };


        ThreadedClient threadedClient = ThreadedClient(scfg);
        EXPECT_ANY_THROW(threadedClient.subscribe(hostName, NULL, onehandler, st, "actionTest", 0));

    }

    TEST_F(StreamingUDPThreadedClientTester, test_subscribe_onehandler_actionNameNull)
    {
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay(st, 1000);
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


        ThreadedClient threadedClient = ThreadedClient(scfg);
        auto thread = threadedClient.subscribe(hostName, port300, onehandler, st, "", 0);
        notify.wait();

        TableSP res = conn300->run("exec msgOffset, actions from getStreamingStat().udpPubTables where tableName=`"+st);
        EXPECT_EQ(res->getColumn(0)->getRow(0)->getInt(), 1000);
        EXPECT_EQ(res->getColumn(1)->getRow(0)->getString(), "");


        cout << "total size: " << msg_total << endl;
        threadedClient.unsubscribe(hostName, port300, st, "");
        Util::sleep(1000);
        EXPECT_TRUE(conn300->run("(exec count(*) from getStreamingStat().udpPubTables where tableName =`"+st+") ==0")->getBool());
        EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
        EXPECT_EQ(msg_total, 1000);
    }

    TEST_F(StreamingUDPThreadedClientTester, test_subscribe_batchhandler_actionNameNull)
    {
        GTEST_SKIP() << "batchhandler not support yet.";
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay(st, 1000);
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


        ThreadedClient threadedClient = ThreadedClient(scfg);
        auto thread = threadedClient.subscribe(hostName, port300, batchhandler, st, "", 0);
        notify.wait();

        TableSP stat = conn300->run("getStreamingStat().udpPubTables");

        EXPECT_EQ(stat->getColumn(0)->getRow(0)->getString(), st);
        EXPECT_EQ(stat->getColumn(2)->getRow(0)->getInt(), 1000);
        EXPECT_EQ(stat->getColumn(3)->getRow(0)->getString(), "");

        cout << "total size: " << msg_total << endl;
        threadedClient.unsubscribe(hostName, port300, st, "");
        Util::sleep(1000);
        EXPECT_TRUE(conn300->run("(exec count(*) from getStreamingStat().udpPubTables where tableName =`"+st+") ==0")->getBool());
        EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
        EXPECT_EQ(msg_total, 1000);
    }

    TEST_F(StreamingUDPThreadedClientTester, test_unsubscribe_onehandler_hostNull)
    {
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        auto onehandler = [&](Message msg)
        {
            msg_total += 1;
            // cout << msg->getString() << endl;
        };


        ThreadedClient threadedClient = ThreadedClient(scfg);
        auto thread = threadedClient.subscribe(hostName, port300, onehandler, st, "actionTest", 0);

        cout << "total size: " << msg_total << endl;
        EXPECT_ANY_THROW(threadedClient.unsubscribe("", port300, st, "actionTest"));

        threadedClient.unsubscribe(hostName, port300, st, "actionTest");
        threadedClient.exit();
        EXPECT_TRUE(threadedClient.isExit());
        Util::sleep(1000);
        EXPECT_TRUE(conn300->run("(exec count(*) from getStreamingStat().udpPubTables where tableName =`"+st+") ==0")->getBool());
    }

    TEST_F(StreamingUDPThreadedClientTester, test_unsubscribe_portNull)
    {
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        auto onehandler = [&](Message msg)
        {
            msg_total += 1;
            // cout << msg->getString() << endl;
        };


        ThreadedClient threadedClient = ThreadedClient(scfg);
        auto thread = threadedClient.subscribe(hostName, port300, onehandler, st, "actionTest", 0);
        cout << "total size: " << msg_total << endl;

        EXPECT_ANY_THROW(threadedClient.unsubscribe(hostName, NULL, st, "actionTest"));
        threadedClient.unsubscribe(hostName, port300, st, "actionTest");
        threadedClient.exit();
        EXPECT_TRUE(threadedClient.isExit());
        Util::sleep(1000);
        EXPECT_TRUE(conn300->run("(exec count(*) from getStreamingStat().udpPubTables where tableName =`"+st+") ==0")->getBool());
    }

    TEST_F(StreamingUDPThreadedClientTester, test_unsubscribe_tableNameNull)
    {
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;

        auto onehandler = [&](Message msg)
        {
            msg_total += 1;
            // cout << msg->getString() << endl;
        };


        ThreadedClient threadedClient = ThreadedClient(scfg);
        auto thread = threadedClient.subscribe(hostName, port300, onehandler, st, "actionTest", 0);

        cout << "total size: " << msg_total << endl;
        EXPECT_ANY_THROW(threadedClient.unsubscribe(hostName, port300, "", "actionTest"));

        threadedClient.unsubscribe(hostName, port300, st, "actionTest");
        threadedClient.exit();
        EXPECT_TRUE(threadedClient.isExit());
        Util::sleep(1000);
        EXPECT_TRUE(conn300->run("(exec count(*) from getStreamingStat().udpPubTables where tableName =`"+st+") ==0")->getBool());
    }

    TEST_F(StreamingUDPThreadedClientTester, test_unsubscribe_actionNameNull)
    {
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay(st, 1000);
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


        ThreadedClient threadedClient = ThreadedClient(scfg);
        auto thread = threadedClient.subscribe(hostName, port300, onehandler, st, "actionTest", 0);
        notify.wait();

        cout << "total size: " << msg_total << endl;
        EXPECT_ANY_THROW(threadedClient.unsubscribe(hostName, port300, st, ""));
        TableSP res = conn300->run("exec msgOffset, actions from getStreamingStat().udpPubTables where tableName=`"+st);
        EXPECT_EQ(res->getColumn(0)->getRow(0)->getInt(), 1000);
        EXPECT_EQ(res->getColumn(1)->getRow(0)->getString(), "actionTest");


        threadedClient.unsubscribe(hostName, port300, st, "actionTest");
        threadedClient.exit();
        EXPECT_TRUE(threadedClient.isExit());

        Util::sleep(1000);
        EXPECT_TRUE(conn300->run("(exec count(*) from getStreamingStat().udpPubTables where tableName =`"+st+") ==0")->getBool());

    }

    TEST_F(StreamingUDPThreadedClientTester, tes_onehandler_subscribe_twice)
    {
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay(st, 1000);
        int msg_total = 0;
        Signal notify;
        Mutex mtx;

        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mtx);
            msg_total += 1;
            // handle msg
            if (msg_total == 1000) notify.set();
        };

        ThreadedClient threadedClient = ThreadedClient(scfg);
        auto thread1 = threadedClient.subscribe(hostName, port300, onehandler, st, "mutiSchemaOne", 0, false);
        EXPECT_ANY_THROW(auto thread2 = threadedClient.subscribe(hostName, port300, onehandler, st, "mutiSchemaOne", 0, false));
        notify.wait();
        cout << "total size: " << msg_total << endl;
        threadedClient.unsubscribe(hostName, port300, st, "mutiSchemaOne");
        Util::sleep(1000);

        EXPECT_TRUE(conn300->run("(exec count(*) from getStreamingStat().udpPubTables where tableName =`"+st+") ==0")->getBool());
        EXPECT_EQ(threadedClient.getQueueDepth(thread1), 0);
        EXPECT_EQ(msg_total, 1000);
    }

    TEST_F(StreamingUDPThreadedClientTester, test_batchhandler_subscribe_twice)
    {
        GTEST_SKIP() << "batchhandler not support yet.";
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay(st, 1000);
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


        ThreadedClient threadedClient = ThreadedClient(scfg);
        auto thread1 = threadedClient.subscribe(hostName, port300, batchhandler, st, "mutiSchemaBatch", 0, false);
        EXPECT_ANY_THROW(auto thread2 = threadedClient.subscribe(hostName, port300, batchhandler, st, "mutiSchemaBatch", 0, false));
        notify.wait();

        cout << "total size: " << msg_total << endl;
        threadedClient.unsubscribe(hostName, port300, st, "mutiSchemaBatch");
        Util::sleep(1000);
        EXPECT_TRUE(conn300->run("(exec count(*) from getStreamingStat().udpPubTables where tableName =`"+st+") ==0")->getBool());
        EXPECT_EQ(threadedClient.getQueueDepth(thread1), 0);
        EXPECT_EQ(msg_total, 1000);
    }

    TEST_F(StreamingUDPThreadedClientTester, test_subscribe_onehandler_withAllDataType)
    {
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay_withAllDataType(st);
        int msg_total = 0;
        TableSP ex_table = conn300->run("select * from "+st+"");
        int index = 0;
        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_UDPCT", conn);

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

        ThreadedClient threadedClient = ThreadedClient(scfg);
        auto thread = threadedClient.subscribe(hostName, port300, onehandler, st, "mutiSchemaOne", 0);
        notify.wait();
        cout << "total size: " << msg_total << endl;
        threadedClient.unsubscribe(hostName, port300, st, "mutiSchemaOne");

        EXPECT_TRUE(conn300->run("re = select * from res_UDPCT order by ind;\
                            ex = select * from ex_UDPCT order by ind;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn300->run("(exec count(*) from getStreamingStat().udpPubTables where tableName =`"+st+") ==0")->getBool());
        EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
        EXPECT_EQ(msg_total, 1000);
    }

    TEST_F(StreamingUDPThreadedClientTester, test_subscribe_batchhandler_withAllDataType)
    {
        GTEST_SKIP() << "batchhandler not support yet.";
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay_withAllDataType(st);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_UDPCT", conn);

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


        ThreadedClient threadedClient = ThreadedClient(scfg);
        auto thread = threadedClient.subscribe(hostName, port300, batchhandler, st, "mutiSchemaBatch", 0);
        notify.wait();

        cout << "total size: " << msg_total << endl;
        threadedClient.unsubscribe(hostName, port300, st, "mutiSchemaBatch");
        EXPECT_TRUE(conn300->run("re = select * from res_UDPCT order by ind;\
                            ex = select * from ex_UDPCT order by ind;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn300->run("(exec count(*) from getStreamingStat().udpPubTables where tableName =`"+st+") ==0")->getBool());
        EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
        EXPECT_EQ(msg_total, 1000);
    }

    TEST_F(StreamingUDPThreadedClientTester, test_subscribe_onehandler_arrayVector)
    {
        GTEST_SKIP() << "https://dolphindb1.atlassian.net/browse/D20-20340";
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay_withArrayVector(st);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_UDPCT", conn);

        auto onehandler = [&](Message msg)
        {
            cout << msg->getString() << endl;
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

        ThreadedClient threadedClient = ThreadedClient(scfg);
        auto thread = threadedClient.subscribe(hostName, port300, onehandler, st, "arrayVectorTableTest", 0);
        notify.wait();

        cout << "total size: " << msg_total << endl;
        threadedClient.unsubscribe(hostName, port300, st, "arrayVectorTableTest");
        EXPECT_TRUE(conn300->run("re = select * from res_UDPCT order by ts;\
                            ex = select * from ex_UDPCT order by ts;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn300->run("(exec count(*) from getStreamingStat().udpPubTables where tableName =`"+st+") ==0")->getBool());
        EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
        EXPECT_EQ(msg_total, 1000);
    }

    TEST_F(StreamingUDPThreadedClientTester, test_subscribe_batchhandler_arrayVector)
    {
        GTEST_SKIP() << "batchhandler not support yet.";
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay_withArrayVector(st);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_UDPCT", conn);

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


        ThreadedClient threadedClient = ThreadedClient(scfg);
        auto thread = threadedClient.subscribe(hostName, port300, batchhandler, st, "arrayVectorTableTest", 0);
        notify.wait();

        cout << "total size: " << msg_total << endl;
        threadedClient.unsubscribe(hostName, port300, st, "arrayVectorTableTest");
        EXPECT_TRUE(conn300->run("re = select * from res_UDPCT order by ts;\
                            ex = select * from ex_UDPCT order by ts;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn300->run("(exec count(*) from getStreamingStat().udpPubTables where tableName =`"+st+") ==0")->getBool());
        EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
        EXPECT_EQ(msg_total, 1000);
    }

    TEST_F(StreamingUDPThreadedClientTester, test_subscribe_hugetable_onehandler)
    {
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay(st, 1000000);
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

        ThreadedClient threadedClient = ThreadedClient(scfg);
        auto thread = threadedClient.subscribe(hostName, port300, onehandler, st, "mutiSchemaOne", 0);
        notify.wait();

        cout << "total size: " << msg_total << endl;
        threadedClient.unsubscribe(hostName, port300, st, "mutiSchemaOne");
        Util::sleep(1000);
        EXPECT_TRUE(conn300->run("(exec count(*) from getStreamingStat().udpPubTables where tableName =`"+st+") ==0")->getBool());

        EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
        EXPECT_EQ(msg_total, 1000000);
    }

    TEST_F(StreamingUDPThreadedClientTester, test_subscribe_hugetable_batchhandler)
    {
        GTEST_SKIP() << "batchhandler not support yet.";
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay(st, 1000000);
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


        ThreadedClient threadedClient = ThreadedClient(scfg);
        auto thread = threadedClient.subscribe(hostName, port300, batchhandler, st, "mutiSchemaBatch", 0);
        notify.wait();

        cout << "total size: " << msg_total << endl;
        threadedClient.unsubscribe(hostName, port300, st, "mutiSchemaBatch");
        Util::sleep(1000);
        EXPECT_TRUE(conn300->run("(exec count(*) from getStreamingStat().udpPubTables where tableName =`"+st+") ==0")->getBool());

        EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
        EXPECT_EQ(msg_total, 1000000);
    }

    TEST_F(StreamingUDPThreadedClientTester, test_subscribe_hugetable_onehandler_msgAsTable)
    {
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay(st, 1000000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_UDPCT", conn);

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

        ThreadedClient threadedClient = ThreadedClient(scfg);
        auto thread = threadedClient.subscribe(hostName, port300, onehandler, st, "mutiSchemaOne", 0, false, nullptr, true);
        notify.wait();

        cout << "total size: " << msg_total << endl;
        threadedClient.unsubscribe(hostName, port300, st, "mutiSchemaOne");
        EXPECT_TRUE(conn300->run("re = select * from res_UDPCT order by datetimev;\
                            ex = select * from ex_UDPCT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn300->run("(exec count(*) from getStreamingStat().udpPubTables where tableName =`"+st+") ==0")->getBool());

        EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
        EXPECT_EQ(msg_total, 1000000);
    }

    TEST_F(StreamingUDPThreadedClientTester, test_subscribe_hugetable_batchhandler_msgAsTable_batchSize_throttle)
    {
        GTEST_SKIP() << "batchhandler not support yet.";
        string st = "outTables_" + getRandString(10);
        UDPCT::createSharedTableAndReplay(st, 1000000);
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
        Signal notify;
        Mutex mutex;
        AutoFitTableAppender appender("", "res_UDPCT", conn);

        auto batchhandler = [&](vector<Message> msgs)
        {
            LockGuard<Mutex> lock(&mutex);
            for (auto &msg : msgs)
            {
                batch_count++;
                int rows = msg->rows();
                EXPECT_TRUE(rows == actual_batchSize || rows == remain_rows);
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
                        Util::sleep(100);
                    }
                }
                if (batch_count == max_batch){
                    start_time = Util::getEpochTime();
                }

                int first_offset = msg.getOffset();
                int last_offset = first_offset + rows - 1;
                if (last_offset == 999999)
                {
                    notify.set();
                }
            }
        };


        ThreadedClient threadedClient = ThreadedClient(scfg);
        auto thread = threadedClient.subscribe(hostName, port300, batchhandler, st, "mutiSchemaBatch", 0, false, nullptr, false, test_batchSize, test_throttle, true);
        notify.wait();
        end_time = Util::getEpochTime();

        cout << "total size: " << msg_total << endl;
        threadedClient.unsubscribe(hostName, port300, st, "mutiSchemaBatch");
        EXPECT_TRUE(conn300->run("re = select * from res_UDPCT order by datetimev;\
                            ex = select * from ex_UDPCT order by datetimev;\
                            all(each(eqObj, re.values(), ex.values()))")->getBool());
        EXPECT_TRUE(conn300->run("(exec count(*) from getStreamingStat().udpPubTables where tableName =`"+st+") ==0")->getBool());

        EXPECT_EQ(threadedClient.getQueueDepth(thread), 0);
        EXPECT_EQ(msg_total, 1000000);
        EXPECT_TRUE(end_time - start_time >= test_throttle * 1000);
    }



    TEST_F(StreamingUDPThreadedClientTester, test_unsubscribe_in_handler)
    {
        string st = "outTables_" + getRandString(10);
        string s = "share streamTable(1:0, `ts`val, [TIMESTAMP, INT]) as "+st+";go;";
        conn300->run(s);
        int msg_total = 0;
        // Signal notify;
        // Mutex mutex;
        ThreadedClient threadedClient = ThreadedClient(scfg);
        auto onehandler = [&](Message msg)
        {
            msg_total+=1;
            cout << "msg_total: " << msg_total << endl;
            cout << "unsubscribe while msg in queue" << endl;
            threadedClient.unsubscribe(hostName, port300, st, "mutiSchemaOne");
            // notify.set();
        };
        auto thread = threadedClient.subscribe(hostName, port300, onehandler, st, "mutiSchemaOne", 0);
        conn300->run("for (i in 0..9999) {tableInsert(`"+st+", now()+i, rand(1000, 1)[0]);};");
        // notify.wait();
        EXPECT_EQ(msg_total, 1); // only one message is in handler, because unsubscribe is called before the first message received
        EXPECT_TRUE(conn300->run("(exec count(*) from getStreamingStat().udpPubTables where tableName =`"+st+") ==0")->getBool());
    }



} // UDPCT namespace
