#include "config.h"

class DBConnectionTest : public testing::Test
{
protected:
    // Suite
    static void SetUpTestCase()
    {
        // DBConnection connReconn;
        connReconn.initialize();
        connReconn.connect(hostName, port, "admin", "123456", "", false, vector<string>(), 7200, true);
    }
    static void TearDownTestCase()
    {
        connReconn.close();
    }

    // Case
    virtual void SetUp()
    {
        cout << "check connect...";
        ConstantSP res = connReconn.run("1+1");
        if (!(res->getBool()))
        {
            cout << "Server not responed, please check." << endl;
        }
        else
        {
            cout << "ok" << endl;
            CLEAR_ENV(connReconn);
        }
    }
    virtual void TearDown()
    {
        CLEAR_ENV(connReconn);
    }
};

bool strToBool(string val)
{
    std::transform(val.begin(), val.end(), val.begin(), ::tolower);
    vector<string> falsevec = {"false", "f", "", "0"};
    for (auto &i : falsevec)
    {
        if (val == i)
            return false;
        else
            return true;
    }
    return NULL;
}

void StopCurNode(string cur_node)
{
    DBConnection conn1(false, false);
    conn1.connect(hostName, ctl_port, "admin", "123456");

    conn1.run("try{stopDataNode(\"" + cur_node + "\")}catch(ex){};");
    cout << cur_node + " has stopped..." << endl;
    std::this_thread::sleep_for(std::chrono::seconds(5));
    // std::this_thread::yield();
    conn1.run("try{startDataNode(\"" + cur_node + "\")}catch(ex){};");
    bool state = conn1.run("exec state from getClusterPerf() where name = `" + cur_node)->getBool();
    int wait_time = 0;
    while (!state && wait_time < 60){
        conn1.run("try{startDataNode(\"" + cur_node + "\")}catch(ex){};");
        state = conn1.run("exec state from getClusterPerf() where name = `" + cur_node)->getBool();
        cout << "waiting for " + cur_node + " to start..." << endl;
        Util::sleep(1000);
        wait_time += 1;
    }
}

bool assertUnConnect()
{
    Util::sleep(2000);
    DBConnection conn2(false, false);
    cout << "check if unconnected..." << endl;
    return conn2.connect(hostName, port, "admin", "123456");
}

TEST_F(DBConnectionTest, test_connect_withErrUserid)
{
    DBConnection conn_demo(false, false);
    EXPECT_FALSE(conn_demo.connect(hostName, port, "adminasdvvv", "123456"));
}

TEST_F(DBConnectionTest, test_connect_withErrPassword)
{
    DBConnection conn_demo(false, false);
    EXPECT_FALSE(conn_demo.connect(hostName, port, "admin", "123456789"));
}

TEST_F(DBConnectionTest, test_connect_withStartupScript)
{
    string script = "tab=table(`1`2`3 as col1,4 5 6 as col2);share tab as startup_tab";
    DBConnection conn_demo(false, false);
    conn_demo.connect(hostName, port, "admin", "123456", script, false, vector<string>(), 7200, false);

    ConstantSP resVec = conn_demo.run("tab1=table(`1`2`3 as col1,4 5 6 as col2);each(eqObj,tab1.values(),startup_tab.values())");
    for (auto i = 0; i < resVec->size(); i++)
        EXPECT_TRUE(resVec->get(i)->getBool());

    string res = conn_demo.getInitScript();
    EXPECT_EQ(script, res);
    conn_demo.close();

    string script2 = "tab=table(`1`2`3 as col1,4 5 6 as col2);share tab as startup_tab_2";
    conn_demo.setInitScript(script2);
    conn_demo.connect(hostName, port, "admin", "123456", script2, false, vector<string>(), 7200, false);
    EXPECT_EQ(conn_demo.getInitScript(), script2);

    conn_demo.run("undef(`startup_tab,SHARED);undef(`startup_tab_2,SHARED);");
    conn_demo.close();
}

TEST_F(DBConnectionTest, test_connection_enableSSL)
{
    DBConnection conn_demo(true);
    conn_demo.connect(hostName, port);
    conn_demo.login("admin", "123456", true);
    conn_demo.run("1+1");
    auto a = conn_demo.run("1 2 3 4");
    auto b = conn_demo.run("`APPL`MSFT`IBM`GOOG");
    auto c = conn_demo.run("2012.06.13 2012.06.14 2012.06.15 2012.06.16");
    vector<ConstantSP> cols = {a, b, c};
    vector<string> colNames = {"a", "b", "c"};
    conn_demo.upload(colNames, cols);
    TableSP t = conn_demo.run("t = table(a as col1, b as col2, c as col3);t");
    EXPECT_EQ(t->getString(), "col1 col2 col3      \n"
                              "---- ---- ----------\n"
                              "1    APPL 2012.06.13\n"
                              "2    MSFT 2012.06.14\n"
                              "3    IBM  2012.06.15\n"
                              "4    GOOG 2012.06.16\n");
    conn_demo.close();
}

#ifndef _WIN32
TEST_F(DBConnectionTest, test_connection_asyncTask)
{
    connReconn.run("try{undef(`tab, SHARED);}catch(ex){}");
    connReconn.run("t=table(`1`2`3 as col1, 1 2 3 as col2);"
                   "share t as tab;"
                   "records = [];");
    DBConnection conn_demo(false, true);
    conn_demo.connect(hostName, port, "admin", "123456");

    conn_demo.run("for(i in 1..5){tableInsert(tab, string(i), i);sleep(1000)};");
    cout << "async job is running";
    do
    {
        connReconn.run("records.append!(exec count(*) from tab)");
        cout << ".";
        Util::sleep(1000);
    } while (connReconn.run("exec count(*) from tab")->getInt() != 8);
    connReconn.run("records.append!(exec count(*) from tab)");

    EXPECT_TRUE(connReconn.run("a=records.pop!();eqObj(a,8)")->getBool());

    connReconn.run("undef(`tab, SHARED)");
    conn_demo.close();
}
#endif

TEST_F(DBConnectionTest, test_connection_function_login)
{
    DBConnection conn_demo(false, false);
    conn_demo.connect(hostName, port);
    EXPECT_ANY_THROW(conn_demo.login("admin123123", "123456", false));
    EXPECT_ANY_THROW(conn_demo.login("admin", "123456789", false));

    conn_demo.login("admin", "123456", false);
    cout << conn_demo.run("getRecentJobs()")->getString();

    conn_demo.close();
}

TEST_F(DBConnectionTest, test_connection_python_script)
{
    string script1 = "import dolphindb as ddb\n"
                        "def list_append(testtype):\n"
                        "\ta= [testtype(1),testtype(2),testtype(3)]\n"
                        "\ta.append(testtype(4))\n"
                        "\ta.append(None)\n"
                        "\tassert a ==[testtype(1),testtype(2),testtype(3),testtype(4),None],'1'\n"
                        "\ndef test_list_append():\n"
                        "\ttypes=[int,long,short,float,double,char,bool,date,minute,month,second,datetime,timestamp,nanotime,nanotimestamp,datehour]\n"
                        "\tfor testtype in types:\n"
                        "\t\tlist_append(testtype)\n"
                        "\t\treturn True;\n"
                        "\n"
                        "def test_list_append_ipaddr_str_uuid():\n"
                        "\ta1= []\n"
                        "\ta2= []\n"
                        "\ta3= ['1','2','3']\n"
                        "\ta1.append(ipaddr(\"192.168.1.13\"))\n"
                        "\ta2.append(uuid(\"9d457e79-1bed-d6c2-3612-b0d31c1881f6\"))\n"
                        "\ta3.append('4')\n"
                        "\tassert a1 == [ipaddr(\"192.168.1.13\")],'2'\n"
                        "\tassert a2==[uuid(\"9d457e79-1bed-d6c2-3612-b0d31c1881f6\")],'3'\n"
                        "\tassert a3==['1','2','3','4'],'4'\n"
                        "\treturn True;\n"
                        "test_list_append()\n"
                        "test_list_append_ipaddr_str_uuid()";
    // cout<<script1<<endl;

    DBConnectionSP conn300_demo = new DBConnection(false, false, 7200, false, true);
    conn300_demo->connect(hostName, port300, "admin", "123456");
    ConstantSP res = conn300_demo->run(script1);
    // cout<< script1;
    // cout<< res->getString();
    EXPECT_EQ(res->getBool(), true);

    conn300_demo->close();

}

TEST_F(DBConnectionTest, test_connection_python_dataform)
{
    string script1 = "import dolphindb as ddb\n"
                        "a=[1,2,3]\n"
                        "b={1,2,3}\n"
                        "c={1:1,2:2}\n"
                        "d=(12,3,4)";
    DBConnectionSP conn300_demo = new DBConnection(false, false, 7200, false, true);
    conn300_demo->connect(hostName, port300, "admin", "123456");
    conn300_demo->run(script1);

    EXPECT_EQ(conn300_demo->run("type(a)")->getString(), "list");
    EXPECT_EQ(conn300_demo->run("type(b)")->getString(), "set");
    EXPECT_EQ(conn300_demo->run("type(c)")->getString(), "dict");
    EXPECT_EQ(conn300_demo->run("type(d)")->getString(), "tuple");

    conn300_demo->close();
}

TEST_F(DBConnectionTest, test_connection_python_setInitscriptAndgetInitscript)
{
    string script1 = "import dolphindb as ddb";
    DBConnectionSP conn300_demo = new DBConnection(false, false, 7200, false, true);
    conn300_demo->connect(hostName, port300, "admin", "123456", script1);
    string res = conn300_demo->getInitScript();
    // cout<< res;
    EXPECT_EQ(res, script1);

    conn300_demo->close();
}

TEST_F(DBConnectionTest, test_connection_python_upload)
{
    vector<string> colName = {"col1", "col2", "col3", "col4", "col5"};
    vector<DATA_TYPE> colType = {DT_BOOL, DT_INT, DT_STRING, DT_DATE, DT_FLOAT};
    TableSP t = Util::createTable(colName, colType, 5, 5);

    t->set(0, 0, Util::createBool(1));
    t->set(1, 0, Util::createInt(1));
    t->set(2, 0, Util::createString("abc"));
    t->set(3, 0, Util::createDate(1));
    t->set(4, 0, Util::createFloat(1.123));

    DBConnectionSP conn300_demo = new DBConnection(false, false, 7200, false, true);
    conn300_demo->connect(hostName, port300, "admin", "123456");
    conn300_demo->upload("t", {t});
    TableSP t1 = conn300_demo->run("t");
    EXPECT_EQ(t1->getString(), t->getString());

    conn300_demo->close();
}

TEST_F(DBConnectionTest, test_connect_reconnect)
{
#ifdef _WIN32
    GTEST_SKIP();
#else
    bool res;
    string cur_node = connReconn.run("getNodeAlias()")->getString();

    std::thread t1 = std::thread(StopCurNode, cur_node);
    std::thread t2 = std::thread([&]
                                 { res = assertUnConnect(); });

    t1.join();
    t2.join();

    Util::sleep(1000);
    EXPECT_EQ(res, false);
    cout << "check passed..." << endl;
    cout << "check if reconnected..." << endl;
    EXPECT_EQ(connReconn.run("1+1")->getInt(), 2);
    cout << "check passed..." << endl;
    // std::thread t1= std::thread(job);
    // t1.join();
#endif
}

TEST_F(DBConnectionTest, test_connectionPool_withErrUserid)
{
    EXPECT_ANY_THROW(DBConnectionPool pool_demo(hostName, port, 10, "adminasdccc", "123456"));
}

TEST_F(DBConnectionTest, test_connectionPool_withErrPassword)
{
    EXPECT_ANY_THROW(DBConnectionPool pool_demo(hostName, port, 10, "admin", "123456789"));
}


TEST_F(DBConnectionTest, DISABLED_test_connectionPool_loadBalance)
{
    EXPECT_ANY_THROW(DBConnectionPool pool_demo(hostName, port, 10, "adminasdccc", "123456", true));
    EXPECT_ANY_THROW(DBConnectionPool pool_demo(hostName, port, 10, "admin", "123456789", true));
    DBConnectionPool pool_demo(hostName, port, 10, "admin", "123456", true);
    DBConnection controller(false, false);
    controller.connect(hostName, ctl_port, "admin", "123456");

    connReconn.run("login(`admin,`123456);"
                   "dbpath=\"dfs://test_dfs\";"
                   "if(existsDatabase(dbpath)){dropDatabase(dbpath)};"
                   "db=database(dbpath, VALUE, 2000.01.01..2000.12.20);"
                   "t=table(100:0, `col1`col2`col3, [SYMBOL,INT,DATE]);"
                   "db.createPartitionedTable(t,`dfs_tab,`col3);");

    pool_demo.run("t=table(100:0, `col1`col2`col3, [SYMBOL,INT,DATE]);"
                  "tableInsert(t,rand(`APPLE`YSL`BMW`HW`SAUM`TDB,1000), rand(1000,1000), take(2000.01.01..2000.12.20,1000));"
                  "loadTable('dfs://test_dfs',`dfs_tab).append!(t);",
                  1000);
    while (!pool_demo.isFinished(1000))
    {
        // cout<<controller.run(
        //         "dnload=take(double(0),7);"
        // 		"for (i in 0..6){"
        // 		"nodeload =exec double(memoryUsed*0.3+connectionNum*0.4+cpuUsage*0.3) from  getClusterPerf() where name = \"datanode\"+string(i+1);"
        // 		"dnload[i]=nodeload[0]};"
        // 		"max(dnload)-min(dnload);")->getDouble()<<endl;
        Util::sleep(100);
    };

    TableSP ex_tab = connReconn.run("select * from loadTable('dfs://test_dfs',`dfs_tab)");
    EXPECT_EQ(1000, ex_tab->rows());
    EXPECT_FALSE(pool_demo.isShutDown());

    pool_demo.shutDown();
    controller.close();
}

TEST_F(DBConnectionTest, test_connectionPool_compress)
{
    DBConnectionPool pool_demo(hostName, port, 10, "admin", "123456", false, false, true);
    const int count = 1000;
    const int scale32 = rand() % 9, scale64 = rand() % 18;

    vector<int> time(count);
    vector<long long> value(count);
    vector<float> cfloat(count);
    vector<string> name(count);
    vector<string> blob(count);
    vector<string> ipaddr(count);
    vector<double> decimal32(count);
    vector<double> decimal64(count);
    int basetime = Util::countDays(2012, 1, 1);
    for (int i = 0; i < count; i++)
    {
        time[i] = basetime + (i % 15);
        value[i] = (i % 500 == 0) ? i : (long long)nullptr;
        cfloat[i] = float(i) + 0.1;
        name[i] = to_string(i);
        blob[i] = "fdsafsdfasd" + to_string(i % 32);
        ipaddr[i] = "192.168.100." + to_string(i % 255);
        decimal32[i] = 0.13566;
        decimal64[i] = 1.2245667899;
    }

    VectorSP boolVector = Util::createVector(DT_BOOL, count, count);
    VectorSP charVector = Util::createVector(DT_CHAR, count, count);
    VectorSP shortVector = Util::createVector(DT_SHORT, count, count);
    VectorSP intVector = Util::createVector(DT_INT, count, count);
    VectorSP dateVector = Util::createVector(DT_DATE, count, count);
    VectorSP monthVector = Util::createVector(DT_MONTH, count, count);
    VectorSP timeVector = Util::createVector(DT_TIME, count, count);
    VectorSP minuteVector = Util::createVector(DT_MINUTE, count, count);
    VectorSP secondVector = Util::createVector(DT_SECOND, count, count);
    VectorSP datetimeVector = Util::createVector(DT_DATETIME, count, count);
    VectorSP timestampVector = Util::createVector(DT_TIMESTAMP, count, count);
    VectorSP nanotimeVector = Util::createVector(DT_NANOTIME, count, count);
    VectorSP nanotimestampVector = Util::createVector(DT_NANOTIMESTAMP, count, count);
    VectorSP floatVector = Util::createVector(DT_FLOAT, count, count);
    VectorSP doubleVector = Util::createVector(DT_DOUBLE, count, count);
    VectorSP symbolVector = Util::createVector(DT_SYMBOL, count, count);
    VectorSP stringVector = Util::createVector(DT_STRING, count, count);
    VectorSP ipaddrVector = Util::createVector(DT_IP, count, count);
    VectorSP blobVector = Util::createVector(DT_BLOB, count, count);
    VectorSP decimal32Vector = Util::createVector(DT_DECIMAL32, count, count, true, scale32);
    VectorSP decimal64Vector = Util::createVector(DT_DECIMAL64, count, count, true, scale64);

    boolVector->setInt(0, count, time.data());
    charVector->setInt(0, count, time.data());
    shortVector->setInt(0, count, time.data());
    intVector->setInt(0, count, time.data());
    dateVector->setInt(0, count, time.data());
    monthVector->setInt(0, count, time.data());
    timeVector->setInt(0, count, time.data());
    minuteVector->setInt(0, count, time.data());
    secondVector->setInt(0, count, time.data());
    datetimeVector->setLong(0, count, value.data());
    timestampVector->setLong(0, count, value.data());
    nanotimeVector->setLong(0, count, value.data());
    nanotimestampVector->setLong(0, count, value.data());
    floatVector->setFloat(0, count, cfloat.data());
    doubleVector->setFloat(0, count, cfloat.data());
    symbolVector->setString(0, count, name.data());
    stringVector->setString(0, count, name.data());
    ipaddrVector->setString(0, count, ipaddr.data());
    blobVector->setString(0, count, blob.data());
    decimal32Vector->setDouble(0, count, decimal32.data());
    decimal64Vector->setDouble(0, count, decimal64.data());

    vector<string> colName = {"cbool", "cchar", "cshort", "cint", "cdate", "cmonth", "ctime", "cminute", "csecond", "cdatetime", "ctimestamp", "cnanotime",
                              "cnanotimestamp", "cfloat", "cdouble", "csymbol", "cstring", "cipaddr", "cblob", "cdecimal32", "cdecimal64"};
    vector<ConstantSP> colVector{boolVector, charVector, shortVector, intVector, dateVector, monthVector, timeVector, minuteVector, secondVector,
                                 datetimeVector, timestampVector, nanotimeVector, nanotimestampVector, floatVector, doubleVector, symbolVector, stringVector,
                                 ipaddrVector, blobVector, decimal32Vector, decimal64Vector};
    TableSP table = Util::createTable(colName, colVector);
    vector<COMPRESS_METHOD> typeVec{COMPRESS_LZ4, COMPRESS_LZ4, COMPRESS_LZ4, COMPRESS_DELTA,
                                    COMPRESS_DELTA, COMPRESS_DELTA, COMPRESS_DELTA, COMPRESS_DELTA,
                                    COMPRESS_DELTA, COMPRESS_DELTA, COMPRESS_DELTA, COMPRESS_DELTA,
                                    COMPRESS_DELTA, COMPRESS_LZ4, COMPRESS_LZ4, COMPRESS_LZ4,
                                    COMPRESS_LZ4, COMPRESS_LZ4, COMPRESS_LZ4, COMPRESS_LZ4, COMPRESS_LZ4};
    table->setColumnCompressMethods(typeVec);
    connReconn.upload("table", table);

    vector<ConstantSP> args{table};
    string script = "colName =  `cbool`cchar`cshort`cint`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring`cipaddr`cblob`cdecimal32`cdecimal64;\n"
                    "colType = [BOOL, CHAR, SHORT, INT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING,IPADDR,BLOB,DECIMAL32(" +
                    to_string(scale32) + "),DECIMAL64(" + to_string(scale64) + ")];\n"
                                                                               "share streamTable(1:0,colName,colType) as table1;";
    connReconn.run(script);

    pool_demo.run("tableInsert{table1}", args, 1000);
    while (!pool_demo.isFinished(1000))
    {
        Util::sleep(1000);
    };

    EXPECT_EQ(pool_demo.getData(1000)->getInt(), count);
    ConstantSP res = connReconn.run("each(eqObj,table.values(),table1.values())");
    for (int i = 0; i < res->size(); i++)
        EXPECT_TRUE(res->get(i)->getBool());

    connReconn.run("undef(`table1, SHARED)");
    pool_demo.shutDown();
}

TEST_F(DBConnectionTest, test_DBconnectionPool)
{
    DBConnectionPool pool_demo(hostName, port, 10, "admin", "123456");
    EXPECT_EQ(pool_demo.getConnectionCount(), 10);
    vector<int> ids = {1, 10, 100, 1000};
    string srcipt1 = "tb = table(100:0,`col1`col2`col3,[INT,INT,INT]);share tb as tmp;";
    connReconn.run(srcipt1);
    EXPECT_ANY_THROW(pool_demo.run("for(i in 1..100){tableInsert(tmp,rand(100,1),rand(100,1),rand(100,1));};select * from tmp", -1));
    pool_demo.run("for(i in 1..100){tableInsert(tmp,rand(100,1),rand(100,1),rand(100,1));};select * from tmp", ids[0]);

    while (!pool_demo.isFinished(ids[0]))
    {
        Util::sleep(1000);
    };

    TableSP ex_tab = connReconn.run("tmp");
    EXPECT_EQ(pool_demo.getData(ids[0])->getString(), ex_tab->getString());
    EXPECT_FALSE(pool_demo.isShutDown());

    vector<ConstantSP> args;
    args.push_back(ex_tab);
    pool_demo.run("tableInsert{tmp}", args, ids[1]);
    while (!pool_demo.isFinished(ids[1]))
    {
        Util::sleep(1000);
    };

    pool_demo.run("exec * from tmp", ids[2]);
    while (!pool_demo.isFinished(ids[2]))
    {
        Util::sleep(1000);
    };
    ex_tab = connReconn.run("tmp");
    EXPECT_EQ(pool_demo.getData(ids[2])->getString(), ex_tab->getString());
    EXPECT_ANY_THROW(pool_demo.getData(ids[2])); // id can only be used once.
    EXPECT_FALSE(pool_demo.isShutDown());

    connReconn.run("undef(`tmp,SHARED);");
    pool_demo.shutDown();
}

TEST_F(DBConnectionTest, test_DBconnectionPoolwithFetchSize)
{
    DBConnectionPool pool_demo(hostName, port, 10, "admin", "123456");
    EXPECT_EQ(pool_demo.getConnectionCount(), 10);
    vector<int> ids = {1, 10, 100, 1000};
    string dbpath = "dfs://test_"+getRandString(10);
    string tab = "pt";
    connReconn.run("dbpath = '" + dbpath +
                   "';tab =`" + tab +
                   ";if(existsDatabase(dbpath))"
                   "    {dropDatabase(dbpath)};"
                   "t=table(1:0, `c1`c2`c3`c4`c5, [INT, SYMBOL, TIMESTAMP, DOUBLE, DOUBLE[]]);share t as res_t;"
                   "db=database(dbpath, VALUE, 1..10, engine='TSDB');"
                   "pt=db.createPartitionedTable(t,'pt','c1',,`c1);");

    string s = "rows = 20000;t=table(rand(1..100,20000) as c1, rand(`a`b`c`d`e`f`g, 20000) as c2, rand(timestamp(10001..50100), 20000) as c3, rand(100.0000, 20000) as c4, arrayVector(1..20000*3, rand(100.0000, 60000)) as c5);"
               "loadTable('" + dbpath + "', `" + tab + ").append!(t);"
               "go;select * from loadTable('" + dbpath + "', `" + tab + ")";
    pool_demo.run(s, ids[1], 4, 2, 8192);

    AutoFitTableAppender appender("", "res_t", connReconn);
    while (!pool_demo.isFinished(ids[1]))
    {
        Util::sleep(1000);
    };

    BlockReaderSP reader = pool_demo.getData(ids[1]);
    int rows{0};
    while (reader->hasNext()) {
        auto res = reader->read();
        int upserts = appender.append(res);
        EXPECT_EQ(upserts, res->rows());
        EXPECT_EQ(res->getForm(), DF_TABLE);
        rows += res->rows();
    }
    EXPECT_EQ(rows, 20000);
    EXPECT_TRUE(connReconn.run(R"(
        ex_t=select * from loadTable(dbpath,tab);
        all(each(eqObj,ex_t.values(),res_t.values()))
    )")->getBool());
    connReconn.run("undef(`res_t,SHARED);dropDatabase(dbpath);go");
    pool_demo.shutDown();
}

#ifndef _WIN32
TEST_F(DBConnectionTest, test_connection_concurrent_insert_datas)
{
    srand(time(NULL));
    string dbpath = "dfs://test_concurrent";
    string tab = "pt";

    connReconn.run("dbpath = '" + dbpath +
                   "';tab =`" + tab +
                   ";if(existsDatabase(dbpath))"
                   "    {dropDatabase(dbpath)};"
                   "t=table(1:0, `c1`c2`c3, [INT, SYMBOL, TIMESTAMP]);"
                   "db=database(dbpath, VALUE, 1..10, chunkGranularity='TABLE');"
                   "pt=db.createPartitionedTable(t,'pt','c1');");

    string s = "t=table(rand(1..100,10) as c1, rand(`a`b`c`d`e`f`g, 10) as c2, rand(timestamp(10001..10100), 10) as c3);"
               "loadTable('" +
               dbpath + "', `" + tab + ").append!(t);"
                                       "go;"
                                       "exec count(*) from loadTable('" +
               dbpath + "', `" + tab + ");";
    auto query = [&s]()
    {
        DBConnection conn0(false, false);
        conn0.connect(hostName, port, "admin", "123456");
        bool success = false;
        while (!success)
        {
            try
            {
                conn0.run(s);
                success = true;
            }
            catch (const std::exception &e)
            {
                string err = e.what();
                cout << "err is " << err << endl;
                ASSERT_TRUE(err.find("RefId:S00002") != std::string::npos || err.find("RefId:S01019") != std::string::npos);
            }
        }
        conn0.close();
    };

    vector<thread> threads;
    for (int i = 0; i < 50; i++)
        threads.emplace_back(query);

    for (auto &thread : threads)
        thread.join();

    EXPECT_EQ(connReconn.run("exec count(*) from loadTable('dfs://test_concurrent', `pt)")->getInt(), 500);
}

TEST_F(DBConnectionTest, test_connectionPool_concurrent_insert_datas)
{
    srand(time(NULL));
    string dbpath = "dfs://test_concurrent";
    string tab = "pt";
    int maxConnections = connReconn.run("int(getConfig('maxConnections'))")->getInt();
    connReconn.run("dbpath = '" + dbpath +
                   "';tab =`" + tab +
                   ";if(existsDatabase(dbpath))"
                   "    {dropDatabase(dbpath)};"
                   "t=table(1:0, `c1`c2`c3, [INT, SYMBOL, TIMESTAMP]);"
                   "db=database(dbpath, VALUE, 1..10, chunkGranularity='TABLE');"
                   "pt=db.createPartitionedTable(t,'pt','c1');");

    string s = "t=table(rand(1..100,100) as c1, rand(`a`b`c`d`e`f`g, 100) as c2, rand(timestamp(10001..10100), 100) as c3);"
               "loadTable('" +
               dbpath + "', `" + tab + ").append!(t);"
                                       "go;"
                                       "exec count(*) from loadTable('" +
               dbpath + "', `" + tab + ");";
    auto query = [&s]()
    {
        DBConnectionPool pool(hostName, port, 10, "admin", "123456");
        bool success = false;
        int id = rand() % 1000;
        while (!success)
        {
            try
            {
                pool.run(s, id);
                while (!pool.isFinished(id))
                {
                    Util::sleep(500);
                }

                success = true;
            }
            catch (const std::exception &e)
            {
                string err = e.what();
                ASSERT_TRUE(err.find("RefId:S00002") != std::string::npos || err.find("RefId:S01019") != std::string::npos);
            }
        }
        pool.shutDown();
    };

    vector<thread> threads;
    for (int i = 0; i < 5; i++)
        threads.emplace_back(query);

    for (auto &thread : threads)
        thread.join();

    EXPECT_EQ(connReconn.run("exec count(*) from loadTable('dfs://test_concurrent', `pt)")->getInt(), 500);
}
#endif

class connection_insert_null : public DBConnectionTest, public testing::WithParamInterface<tuple<string, DATA_TYPE>>
{
public:
    static vector<tuple<string, DATA_TYPE>> data_prepare()
    {
        vector<string> testTypes = {"BOOL", "CHAR", "SHORT", "INT", "LONG", "DATE", "MONTH", "TIME", "MINUTE", "SECOND", "DATETIME", "TIMESTAMP", "NANOTIME", "NANOTIMESTAMP", "DATEHOUR", "FLOAT", "DOUBLE", "STRING", "SYMBOL", "BLOB", "IPADDR", "UUID", "INT128", "DECIMAL32(8)", "DECIMAL64(15)", "DECIMAL128(28)",
                                    "BOOL[]", "CHAR[]", "SHORT[]", "INT[]", "LONG[]", "DATE[]", "MONTH[]", "TIME[]", "MINUTE[]", "SECOND[]", "DATETIME[]", "TIMESTAMP[]", "NANOTIME[]", "NANOTIMESTAMP[]", "DATEHOUR[]", "FLOAT[]", "DOUBLE[]", "IPADDR[]", "UUID[]", "INT128[]", "DECIMAL32(8)[]", "DECIMAL64(15)[]", "DECIMAL128(25)[]"};
        vector<DATA_TYPE> dataTypes = {DT_BOOL, DT_CHAR, DT_SHORT, DT_INT, DT_LONG, DT_DATE, DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_DATEHOUR, DT_FLOAT, DT_DOUBLE, DT_STRING, DT_SYMBOL, DT_BLOB, DT_IP, DT_UUID, DT_INT128, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128,
                                       DT_BOOL_ARRAY, DT_CHAR_ARRAY, DT_SHORT_ARRAY, DT_INT_ARRAY, DT_LONG_ARRAY, DT_DATE_ARRAY, DT_MONTH_ARRAY, DT_TIME_ARRAY, DT_MINUTE_ARRAY, DT_SECOND_ARRAY, DT_DATETIME_ARRAY, DT_TIMESTAMP_ARRAY, DT_NANOTIME_ARRAY, DT_NANOTIMESTAMP_ARRAY, DT_DATEHOUR_ARRAY, DT_FLOAT_ARRAY, DT_DOUBLE_ARRAY, DT_IP_ARRAY, DT_UUID_ARRAY, DT_INT128_ARRAY, DT_DECIMAL32_ARRAY, DT_DECIMAL64_ARRAY, DT_DECIMAL128_ARRAY};
        vector<tuple<string, DATA_TYPE>> data;
        for (auto i = 0; i < testTypes.size(); i++)
            data.push_back(make_tuple(testTypes[i], dataTypes[i]));
        return data;
    }
};
INSTANTIATE_TEST_SUITE_P(, connection_insert_null, testing::ValuesIn(connection_insert_null::data_prepare()));

TEST_P(connection_insert_null, test_append_empty_table)
{
    string type = std::get<0>(GetParam());
    DATA_TYPE dataType = std::get<1>(GetParam());
    cout << "test type: " << type << endl;
    string colName = "c1";
    string script1 =
        "colName = [`" + colName + "];"
                                   "colType = [" +
        type + "];"
               "share table(1:0, colName, colType) as att;";

    connReconn.run(script1);
    VectorSP col1 = Util::createVector(dataType, 0);
    vector<string> colNames = {"c1"};
    vector<ConstantSP> cols = {col1};
    TableSP empty2 = Util::createTable(colNames, cols);

    connReconn.upload("empty", empty2);
    auto res = connReconn.run("each(eqObj, att.values(), empty.values())");
    EXPECT_EQ(res->getString(), "[1]");

    vector<ConstantSP> args = {empty2};
    auto t = connReconn.run("append!{att}", args);
    EXPECT_EQ(connReconn.run("exec count(*) from att")->getInt(), 0);
    EXPECT_EQ(t->rows(), 0);
    connReconn.run("undef(`att, SHARED)");
    EXPECT_ANY_THROW(connReconn.run("append!{att}", args));
}


TEST_F(DBConnectionTest, test_connection_parallel)
{
    connReconn.run("login(`admin,`123456);try{createUser(`test1, `123456)}catch(ex){};go;setMaxJobParallelism(`test1, 10);");
    { // TestConnectionParallel_lt_MaxJobParallelism
        int priority = 3;
        int parallel = 1;
        DBConnectionSP _tmpC = new DBConnection(false, false);
        _tmpC->connect(hostName, port, "test1", "123456");
        TableSP res = _tmpC->run("getConsoleJobs()", priority, parallel);
        EXPECT_EQ(res->getColumn(5)->get(0)->getInt(), 3);
        EXPECT_EQ(res->getColumn(6)->get(0)->getInt(), 1);

    }
    { // TestConnectionParallel_gt_MaxJobParallelism
        int priority = 1;
        int parallel = 12;
        DBConnectionSP _tmpC = new DBConnection(false, false);
        _tmpC->connect(hostName, port, "test1", "123456");
        TableSP res = _tmpC->run("getConsoleJobs()", priority, parallel);
        EXPECT_EQ(res->getColumn(5)->get(0)->getInt(), 1);
        EXPECT_EQ(res->getColumn(6)->get(0)->getInt(), 10);
    }
    { // TestConnectionParallel_default
        DBConnectionSP _tmpC = new DBConnection(false, false);
        _tmpC->connect(hostName, port, "test1", "123456");
        TableSP res = _tmpC->run("getConsoleJobs()");
        EXPECT_EQ(res->getColumn(5)->get(0)->getInt(), 4);
        EXPECT_EQ(res->getColumn(6)->get(0)->getInt(), 10);
    }

}


TEST_F(DBConnectionTest, test_connectionPool_parallel)
{
    connReconn.run("login(`admin,`123456);try{createUser(`test1, `123456)}catch(ex){};go;setMaxJobParallelism(`test1, 10);");
    { // TestConnectionParallel_lt_MaxJobParallelism
        int priority = 3;
        int parallel = rand() % 10 + 1;
        DBConnectionPoolSP _tmpP = new DBConnectionPool(hostName, port, 10, "test1", "123456");
        int id = rand() % 1000;
        EXPECT_ANY_THROW(_tmpP->run("getConsoleJobs()", id, priority, 0));
        _tmpP->run("getConsoleJobs()", id, priority, parallel);
        while (!_tmpP->isFinished(id))
        {
            Util::sleep(500);
        }
        TableSP res = _tmpP->getData(id);
        EXPECT_EQ(res->getColumn(5)->get(0)->getInt(), priority);
        EXPECT_EQ(res->getColumn(6)->get(0)->getInt(), parallel);

    }
    { // TestConnectionParallel_gt_MaxJobParallelism
        int priority = 1;
        int parallel = rand() % 64 + 11;
        DBConnectionPoolSP _tmpP = new DBConnectionPool(hostName, port, 10, "test1", "123456");
        int id = rand() % 1000;
        _tmpP->run("getConsoleJobs()", id, priority, parallel);
        while (!_tmpP->isFinished(id))
        {
            Util::sleep(500);
        }
        TableSP res = _tmpP->getData(id);
        EXPECT_EQ(res->getColumn(5)->get(0)->getInt(), priority);
        EXPECT_EQ(res->getColumn(6)->get(0)->getInt(), 10);
    }
    { // TestConnectionParallel_default
        DBConnectionPoolSP _tmpP = new DBConnectionPool(hostName, port, 10, "test1", "123456");
        int id = rand() % 1000;
        _tmpP->run("getConsoleJobs()", id);
        while (!_tmpP->isFinished(id))
        {
            Util::sleep(500);
        }
        TableSP res = _tmpP->getData(id);
        EXPECT_EQ(res->getColumn(5)->get(0)->getInt(), 4);
        EXPECT_EQ(res->getColumn(6)->get(0)->getInt(), 10);
    }

}

TEST_F(DBConnectionTest, test_connection_login_encrypt){
    DBConnectionSP _c = new DBConnection(false, false);
    _c->connect(hostName, port);
    #ifdef TEST_OPENSSL
        _c->login("admin", "123456", true);
        ConstantSP res = _c->run("1+1");
        EXPECT_EQ(res->getInt(), 2);
    #else
        EXPECT_ANY_THROW(_c->login("admin", "123456", true));
    #endif
    _c->close();
}


TEST_F(DBConnectionTest, test_connection_login_ban_guest){
    {
        DBConnectionSP _c = new DBConnection(false, false);
        _c->connect(hostName, port);
        _c->login("admin", "123456", false);
        bool res = _c->run("bool(getConfig(`enableClientAuth))")->getBool();
        EXPECT_TRUE(res);
        _c->close();
    }
    {
        DBConnectionSP _c = new DBConnection(false, false);
        _c->connect(hostName, port);
        EXPECT_ANY_THROW(_c->run("bool(getConfig(`enableClientAuth))")); // guest user is banned
        _c->close();
    }
    {
        DBConnectionPoolSP _p = new DBConnectionPool(hostName, port, 10, "admin", "123456");
        int id = rand() % 1000;
        _p->run("bool(getConfig(`enableClientAuth))", id);
        while (!_p->isFinished(id))
        {
            Util::sleep(500);
        }
        bool res = _p->getData(id)->getBool();
        EXPECT_TRUE(res);
        _p->shutDown();
    }
    {
        DBConnectionPoolSP _p = new DBConnectionPool(hostName, port, 10);
        int id = rand() % 1000;
        try{
            _p->run("bool(getConfig(`enableClientAuth))", id);
            while (!_p->isFinished(id))
            {
                Util::sleep(500);
            }
        }catch(const std::exception& e){
            EXPECT_PRED_FORMAT2(testing::IsSubstring, "RefId: S04009", e.what());
        }
  
        _p->shutDown();
    }

}

TEST_F(DBConnectionTest, test_connection_SCRAM){
    try{
        connReconn.run("try{deleteUser('scramUser')}catch(ex){};go;createUser(`scramUser, `123456, authMode='scram')");
    }catch(const std::exception& e){
        GTEST_SKIP() << e.what();
    }
    {
        DBConnectionSP _c = new DBConnection(false, false, 7200, false, false, false, true);
        _c->connect(hostName, port);
        _c->login("scramUser", "123456", true);
        ConstantSP res = _c->run("getCurrentSessionAndUser()[1]");
        EXPECT_EQ(res->getString(), "scramUser");
        _c->close();
        cout << "test_connection_SCRAM1 passed" << endl;
    }
    {
        DBConnection _c = DBConnection(false, false, 7200, false, false, false, true);
        _c.connect(hostName, port, "scramUser", "123456");
        ConstantSP res = _c.run("getCurrentSessionAndUser()[1]");
        EXPECT_EQ(res->getString(), "scramUser");
        _c.close();
        cout << "test_connection_SCRAM2 passed" << endl;
    }
}

TEST_F(DBConnectionTest, test_connectionPool_SCRAM){
    try{
        connReconn.run("try{deleteUser('scramUser')}catch(ex){};go;createUser(`scramUser, `123456, authMode='scram')");
    }catch(const std::exception& e){
        GTEST_SKIP() << e.what();
    }
    DBConnectionPoolSP _p = new DBConnectionPool(hostName, port, 10, "scramUser", "123456");
    _p->run("getCurrentSessionAndUser()[1]", 0);
    while (!_p->isFinished(0)){
        Util::sleep(500);
    }
    EXPECT_EQ(_p->getData(0)->getString(), "scramUser");
    _p->shutDown();
    cout << "test_connectionPool_SCRAM passed" << endl;
}