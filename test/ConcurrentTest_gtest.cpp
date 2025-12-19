#include <gtest/gtest.h>
#include "config.h"
#include "Streaming.h"

class ConcurrentTest : public testing::Test
{
public:
    static dolphindb::DBConnection conn;
    static bool createTable(dolphindb::DBConnection &conn, std::string dbName, std::string tableName);
    static void insertTask(std::string dbName, std::string tableName);
    static int getTableRows(dolphindb::DBConnection &conn, std::string dbName, std::string tableName);
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
dolphindb::DBConnection ConcurrentTest::conn(false, false);


bool ConcurrentTest::createTable(dolphindb::DBConnection &session, std::string dbName = "", std::string tableName = ""){
    std::string s = "";
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

void ConcurrentTest::insertTask(std::string dbName = "", std::string tableName = ""){
    dolphindb::DBConnection _conn(false,false);
    _conn.connect(HOST, PORT, USER, PASSWD);
    std::string str = "dummy = table(1:0, `id`sym`date`value, [INT, SYMBOL, DATE, DOUBLE]);"
                "tableInsert(dummy,rand(10000,1),rand(`a`b`c`d`e`f`g`h`i`j`k`l`m`n,1),date(rand(100000, 1)),rand(10000.00,1));"
                "loadTable(\""+dbName+"\",`"+tableName+").append!(dummy)";
    if (dbName == ""){
        str = "tableInsert("+tableName+",rand(10000,1),rand(`a`b`c`d`e`f`g`h`i`j`k`l`m`n,1),rand(2000.01.01..2021.12.20,1),rand(10000.00,1));";
    }
    _conn.run(str);
    dolphindb::Util::sleep(1000);
    _conn.close();
}

int ConcurrentTest::getTableRows(dolphindb::DBConnection &session, std::string dbName = "", std::string tableName = ""){
    if (dbName == ""){
        return session.run("exec count(*) from "+tableName)->getInt();
    }
    return session.run("exec count(*) from loadTable(\""+dbName+"\",`"+tableName+")")->getInt();
}

TEST_F(ConcurrentTest, test_HightlyConcurrentConn_insertToPartitionedTable){
    std::string case_=getCaseName();
    std::string db="dfs://" + case_;
    int bf_num = 500;
    std::string tab = "test_HightlyConcurrentConn_insertToPartitionedTable";

    if(createTable(conn, db, tab))
        std::cout<<"create database and partitionedtable successfully"<<std::endl;
    std::vector<std::thread> th(bf_num);
    int sum=0;
    for (int i = 0; i < bf_num; i++){
        th[i] = std::thread(insertTask, db, tab);
    }
    for (int i = 0; i < bf_num; i++){
        th[i].join();
        sum+=1;
    }
    ASSERT_EQ(sum, bf_num);
    ASSERT_EQ(getTableRows(conn, db, tab), bf_num);
}

TEST_F(ConcurrentTest, test_HightlyConcurrentConn_insertToinMemoryTable){
    std::string tab=getCaseName();
    int bf_num = 500;

    if(createTable(conn, "", tab))
        std::cout<<"create in-memory table successfully"<<std::endl;
    std::vector<std::thread> th(bf_num);
    int sum=0;
    for (int i = 0; i < bf_num; i++){
        th[i] = std::thread(insertTask, "", tab);
    }
    for (int i = 0; i < bf_num; i++){
        th[i].join();
        sum+=1;
    }
    ASSERT_EQ(sum, bf_num);
    ASSERT_EQ(getTableRows(conn, "", tab), bf_num);
}

TEST_F(ConcurrentTest, test_threadedClient_multi_client_subscribe_concurrent){
    std::string tab = getCaseName();
    int subNum = 10;
    int test_rows = 100000;
    std::string script = "\
            share streamTable(100:0, `datetimev`timestampv`sym`price1`price2,[DATETIME,TIMESTAMP,SYMBOL,DOUBLE,DOUBLE]) as `"+tab+"\n\
            go\n\
            setStreamTableFilterColumn("+tab+", `sym)";
    conn.run(script);

    std::string replayScript = "n = "+std::to_string(test_rows)+";table1_STCT = table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);\
            tableInsert(table1_STCT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));\
            replay(inputTables=table1_STCT, outputTables=`"+tab+", dateColumn=`timestampv, timeColumn=`timestampv)";
    conn.run(replayScript);

    std::vector<std::string> subActions(subNum);
    std::vector<dolphindb::ThreadedClient*> clients(subNum);
    for (int i=0; i< subNum;i++){
        subActions[i] = "cctest_"+std::to_string(i);
        conn.run("share table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]) as "+tab+"_"+std::to_string(i));
        clients[i] = new dolphindb::ThreadedClient();
    }

    for (int i=0; i< subNum;i++){
        auto hd = [=](dolphindb::Message msg){
            // std::cout << msg->getString();
            dolphindb::TableSP tmp = (dolphindb::TableSP)msg;
            std::vector<dolphindb::ConstantSP> args = {tmp};
            conn.run("tableInsert{"+tab+"_"+std::to_string(i)+"}", args);
        };
        auto tsp = clients[i]->subscribe(HOST, PORT, hd, tab, subActions[i], 0, true, nullptr, true, true, USER, PASSWD);
    }

    bool flag = false;
    int timeout = 0;
    do{
        flag = conn.run("res_rows = array(INT);\
            for (i in 0:"+std::to_string(subNum)+"){res_rows.append!(objByName('"+tab+"_'+string(i)).rows())};\
            all(res_rows == "+std::to_string(test_rows)+");")->getBool();
        dolphindb::Util::sleep(1000);
        timeout++;
    }while(!flag && timeout < 60);

    for (int i=0; i< subNum;i++){
        clients[i]->unsubscribe(HOST, PORT, tab, subActions[i]);
        delete clients[i];
    }
    ASSERT_TRUE(conn.run("tableRes = array(BOOL);\
            for (i in 0:"+std::to_string(subNum)+"){tableRes.append!(each(eqObj, objByName('"+tab+"_'+string(i)).values(), "+tab+".values()))};\
            all(tableRes);")->getBool());
}

TEST_F(ConcurrentTest, test_threadedClient_single_client_multi_subscribe_concurrent){
    std::string tab = getCaseName();
    int subNum = 10;
    int test_rows = 100000;
    std::string script = "\
            share streamTable(100:0, `datetimev`timestampv`sym`price1`price2,[DATETIME,TIMESTAMP,SYMBOL,DOUBLE,DOUBLE]) as `"+tab+"\n\
            go\n\
            setStreamTableFilterColumn("+tab+", `sym)";
    conn.run(script);

    std::string replayScript = "n = "+std::to_string(test_rows)+";table1_STCT = table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);\
            tableInsert(table1_STCT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));\
            replay(inputTables=table1_STCT, outputTables=`"+tab+", dateColumn=`timestampv, timeColumn=`timestampv)";
    conn.run(replayScript);

    std::vector<std::string> subActions(subNum);
    dolphindb::ThreadedClient client = dolphindb::ThreadedClient();
    for (int i=0; i< subNum;i++){
        subActions[i] = "cctest_"+std::to_string(i);
        conn.run("share table(1:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]) as "+tab+"_"+std::to_string(i));

    }

    for (int i=0; i< subNum;i++){
        auto hd = [=](dolphindb::Message msg){
            dolphindb::TableSP tmp = (dolphindb::TableSP)msg;
            std::vector<dolphindb::ConstantSP> args = {tmp};
            conn.run("tableInsert{"+tab+"_"+std::to_string(i)+"}", args);
        };
        auto tsp = client.subscribe(HOST, PORT, hd, tab, subActions[i], 0, true, nullptr, true, true, USER, PASSWD);
    }

    bool flag = false;
    int timeout = 0;
    do{
        flag = conn.run("res_rows = array(INT);\
            for (i in 0:"+std::to_string(subNum)+"){res_rows.append!(objByName('"+tab+"_'+string(i)).rows())};\
            all(res_rows == "+std::to_string(test_rows)+");")->getBool();
        dolphindb::Util::sleep(1000);
        timeout++;
    }while(!flag && timeout < 60);

    for (int i=0; i< subNum;i++){
        client.unsubscribe(HOST, PORT, tab, subActions[i]);
    }
    ASSERT_TRUE(conn.run("tableRes = array(BOOL);\
            for (i in 0:"+std::to_string(subNum)+"){tableRes.append!(each(eqObj, objByName('"+tab+"_'+string(i)).values(), "+tab+".values()))};\
            all(tableRes);")->getBool());
}