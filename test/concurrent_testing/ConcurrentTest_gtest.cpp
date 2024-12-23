#include "../config.h"


#ifndef _WIN32
class ConcurrentTest : public testing::Test
{
public:
    static bool createTable(DBConnection &conn, string dbName, string tableName);
    static void insertTask(string dbName, string tableName);
    static int getTableRows(DBConnection &conn, string dbName, string tableName);
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

bool ConcurrentTest::createTable(DBConnection &session, string dbName = "", string tableName = ""){
	string s = "login(`admin,`123456);";
    s += "dbPath = \""+dbName+"\";";
    s += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
    s += "db = database(dbPath,VALUE,date(1..100));";
    s += "dummy = table(1:0, `id`sym`date`value, [INT, SYMBOL, DATE, DOUBLE]);";
    s += ""+tableName+" = db.createPartitionedTable(dummy,`"+tableName+",`date);";
    if (dbName == ""){
        s = "colName =  `cint`csymbol`cdate`cdouble;"
            "colType = [INT,SYMBOL,DATE,DOUBLE];"
            "tab1=table(1:0,colName,colType);"
            "share tab1 as "+tableName+"";
    }
    session.run(s);
    return true;
}

void ConcurrentTest::insertTask(string dbName = "", string tableName = ""){
    DBConnection _conn(false,false);
    _conn.connect(hostName, port, "admin", "123456");
    string str = "dummy = table(1:0, `id`sym`date`value, [INT, SYMBOL, DATE, DOUBLE]);"
                "tableInsert(dummy,rand(10000,1),rand(`a`b`c`d`e`f`g`h`i`j`k`l`m`n,1),date(rand(100000, 1)),rand(10000.00,1));"
                "loadTable(\""+dbName+"\",`"+tableName+").append!(dummy)";
    if (dbName == ""){
        str = "tableInsert("+tableName+",rand(10000,1),rand(`a`b`c`d`e`f`g`h`i`j`k`l`m`n,1),rand(2000.01.01..2021.12.20,1),rand(10000.00,1));";
    }
    _conn.run(str);
    Util::sleep(1000);
    _conn.close();
    // cout<<"insert finished! "<<endl;
}

int ConcurrentTest::getTableRows(DBConnection &session, string dbName = "", string tableName = ""){
    if (dbName == ""){
        return session.run("exec count(*) from "+tableName)->getInt();
    }
    return session.run("exec count(*) from loadTable(\""+dbName+"\",`"+tableName+")")->getInt();
}

TEST_F(ConcurrentTest, test_HightlyConcurrentConn_insertToPartitionedTable){
    int bf_num = 500;
    string db = "dfs://test_dfsDemo";
    string tab = "dfs_table";

    if(createTable(conn, db, tab))
        cout<<"create database and partitionedtable successfully"<<endl;
	vector<thread> th(bf_num);
    int sum=0;
	for (int i = 0; i < bf_num; i++){
		th[i] = std::thread(insertTask, db, tab);
	}
	for (int i = 0; i < bf_num; i++){
		th[i].join();
        sum+=1;
	}
    EXPECT_EQ(sum, bf_num);
    EXPECT_EQ(getTableRows(conn, db, tab), bf_num);
}


TEST_F(ConcurrentTest, test_HightlyConcurrentConn_insertToinMemoryTable){
    int bf_num = 500;
    string tab = "tmp";

    if(createTable(conn, "", tab))
        cout<<"create in-memory table successfully"<<endl;
	vector<thread> th(bf_num);
    int sum=0;
	for (int i = 0; i < bf_num; i++){
		th[i] = std::thread(insertTask, "", tab);
	}
	for (int i = 0; i < bf_num; i++){
		th[i].join();
        sum+=1;
	}
    EXPECT_EQ(sum, bf_num);
    EXPECT_EQ(getTableRows(conn, "", tab), bf_num);

}

TEST_F(ConcurrentTest, test_threadedClient_multi_client_subscribe_concurrent){
    string tab = "cc_"+getRandString(5);
    int subNum = 10;
    int test_rows = 100000;
    string script = "login(\"admin\",\"123456\")\n\
            st1 = streamTable(100:0, `datetimev`timestampv`sym`price1`price2,[DATETIME,TIMESTAMP,SYMBOL,DOUBLE,DOUBLE])\n\
            enableTableShareAndPersistence(table=st1, tableName=`"+tab+")\n\
            go\n\
            setStreamTableFilterColumn("+tab+", `sym)";
    conn.run(script);

    string replayScript = "n = "+to_string(test_rows)+";table1_STCT = table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);\
            tableInsert(table1_STCT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));\
            replay(inputTables=table1_STCT, outputTables=`"+tab+", dateColumn=`timestampv, timeColumn=`timestampv)";
    conn.run(replayScript);

    vector<string> subActions(subNum);
    vector<ThreadedClient*> clients(subNum);
    for (int i=0; i< subNum;i++){
        subActions[i] = "cctest_"+to_string(i);
        conn.run("share table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]) as res_"+to_string(i));
        clients[i] = new ThreadedClient();
    }

    for (int i=0; i< subNum;i++){
        auto hd = [=](Message msg){
            // cout << msg->getString();
            TableSP tmp = (TableSP)msg;
            conn.run("tableInsert{res_"+to_string(i)+"}", tmp);
        };
        auto tsp = clients[i]->subscribe(hostName, port, hd, tab, subActions[i], 0, true, nullptr, true, true, "admin", "123456");
        cout << "thread id: " << tsp->getID() << endl;
    }

    bool flag = false;
    int timeout = 0;
    do{
        flag = conn.run("res_rows = array(INT);\
            for (i in 0:"+to_string(subNum)+"){res_rows.append!(objByName('res_'+string(i)).rows())};\
            all(res_rows == "+to_string(test_rows)+");")->getBool();
        Util::sleep(1000);
        timeout++;
    }while(!flag && timeout < 60);

    for (int i=0; i< subNum;i++){
        clients[i]->unsubscribe(hostName, port, tab, subActions[i]);
        delete clients[i];
    }
    EXPECT_TRUE(conn.run("tableRes = array(BOOL);\
            for (i in 0:"+to_string(subNum)+"){tableRes.append!(each(eqObj, objByName('res_'+string(i)).values(), "+tab+".values()))};\
            all(tableRes);")->getBool());

}



TEST_F(ConcurrentTest, test_threadedClient_single_client_multi_subscribe_concurrent){
    string tab = "cc_"+getRandString(5);
    int subNum = 10;
    int test_rows = 100000;
    string script = "login(\"admin\",\"123456\")\n\
            st1 = streamTable(100:0, `datetimev`timestampv`sym`price1`price2,[DATETIME,TIMESTAMP,SYMBOL,DOUBLE,DOUBLE])\n\
            enableTableShareAndPersistence(table=st1, tableName=`"+tab+")\n\
            go\n\
            setStreamTableFilterColumn("+tab+", `sym)";
    conn.run(script);

    string replayScript = "n = "+to_string(test_rows)+";table1_STCT = table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);\
            tableInsert(table1_STCT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));\
            replay(inputTables=table1_STCT, outputTables=`"+tab+", dateColumn=`timestampv, timeColumn=`timestampv)";
    conn.run(replayScript);

    vector<string> subActions(subNum);
    ThreadedClient client = ThreadedClient();
    for (int i=0; i< subNum;i++){
        subActions[i] = "cctest_"+to_string(i);
        conn.run("share table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]) as res_"+to_string(i));

    }

    for (int i=0; i< subNum;i++){
        auto hd = [=](Message msg){
            // cout << msg->getString();
            TableSP tmp = (TableSP)msg;
            conn.run("tableInsert{res_"+to_string(i)+"}", tmp);
        };
        auto tsp = client.subscribe(hostName, port, hd, tab, subActions[i], 0, true, nullptr, true, true, "admin", "123456");
    }

    bool flag = false;
    int timeout = 0;
    do{
        flag = conn.run("res_rows = array(INT);\
            for (i in 0:"+to_string(subNum)+"){res_rows.append!(objByName('res_'+string(i)).rows())};\
            all(res_rows == "+to_string(test_rows)+");")->getBool();
        Util::sleep(1000);
        timeout++;
    }while(!flag && timeout < 60);

    for (int i=0; i< subNum;i++){
        client.unsubscribe(hostName, port, tab, subActions[i]);
    }
    EXPECT_TRUE(conn.run("tableRes = array(BOOL);\
            for (i in 0:"+to_string(subNum)+"){tableRes.append!(each(eqObj, objByName('res_'+string(i)).values(), "+tab+".values()))};\
            all(tableRes);")->getBool());

}


#endif // not windows