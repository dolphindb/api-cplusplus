#include "DolphinDB.h"
#include "MultithreadedTableWriter.h"
#include "Util.h"
#include "Streaming.h"
#include <vector>
#include <string>
#include <thread>

using namespace dolphindb;
using namespace std;

const string hostName = "192.168.0.16";
const string node_name = "datanode4";
const int sub_port = 9005;
const int publish_port = 9002;
const int ctl_port = 9000;

vector<string> sites = {"192.168.0.16:9003:datanode2", "192.168.0.16:9004:datanode3", "192.168.0.16:9005:datanode4"};
vector<string> nodeNames = {"datanode2", "datanode3", "datanode4"};

void stopNodeTask(string host, int controllerPort, string nodeName)
{
    cout << "Try to stop current Datanode: " + nodeName << endl;
    DBConnection conn2(false, false);
    conn2.connect(host, controllerPort, "admin", "123456");

    conn2.run("try{stopDataNode(\"" + nodeName + "\");}catch(ex){}");
    bool status = conn2.run("(exec state from getClusterPerf() where name = `" + nodeName + ")[0]")->getBool();
    while (status)
    {
        Util::sleep(500);
        status = conn2.run("(exec state from getClusterPerf() where name = `" + nodeName + ")[0]")->getBool();
    }

    cout << "Stop successfully." << endl;
    conn2.close();
    return;
}

void restartNodeTask(string host, int controllerPort, string nodeName, int sleep_second)
{
    DBConnection conn2(false, false);
    conn2.connect(host, controllerPort, "admin", "123456");
    Util::sleep(sleep_second * 1000);
    cout << "Restart the Datanode: " + nodeName << endl;
    conn2.run("startDataNode(\"" + nodeName + "\")");
    bool status = conn2.run("(exec state from getClusterPerf() where name = `" + nodeName + ")[0]")->getBool();
    while (!status)
    {
        Util::sleep(500);
        status = conn2.run("(exec state from getClusterPerf() where name = `" + nodeName + ")[0]")->getBool();
    }

    cout << "Start successfully." << endl;
    conn2.close();
    return;
}

void restartAllNodes(string host, int controllerPort, vector<string> nodeNames)
{
    DBConnection conn2(false, false);
    conn2.connect(host, controllerPort, "admin", "123456");

    for (auto &nodeName : nodeNames)
    {
        cout << "Restart the Datanode: " + nodeName << endl;
        conn2.run("startDataNode(\"" + nodeName + "\")");
        bool status = conn2.run("(exec state from getClusterPerf() where name = `" + nodeName + ")[0]")->getBool();
        while (!status)
        {
            Util::sleep(500);
            status = conn2.run("(exec state from getClusterPerf() where name = `" + nodeName + ")[0]")->getBool();
        }

        cout << "Start successfully." << endl;
    }

    conn2.close();
    return;
}

void restartAllDataNodes(vector<string> nodes, int ctl_port)
{
    DBConnection conn_restartnode(false, false);
    bool ret = conn_restartnode.connect(hostName, ctl_port, "admin", "123456");
    for (unsigned int j = 0; j < nodeNames.size(); j++)
    {
        conn_restartnode.run("try{startDataNode(\"" + nodeNames[j] + "\")}catch(ex){};");
    }
    conn_restartnode.close();
}

void test_basicHA()
{
    static DBConnection conn(false, false);
    string init_script = "undef all;go;temp=table(`1`2`3 as col1, 1 2 3 as col2);";
    conn.connect(hostName, sub_port, "admin", "123456", init_script, true, sites, 7200, false);

    string first_node = conn.run("getNodeAlias()")->getString();

    cout << "Current datanode is " + first_node << endl;
    Util::sleep(2000);
    stopNodeTask(hostName, ctl_port, first_node);

    string second_node = conn.run("getNodeAlias()")->getString();
    assert(first_node != second_node);
    assert(conn.run("`temp in objs(true).name")->getBool());
    cout << "Now datanode has changed to " + second_node << endl;
    Util::sleep(2000);
    stopNodeTask(hostName, ctl_port, second_node);

    string third_node = conn.run("getNodeAlias()")->getString();
    assert(third_node != second_node);
    assert(conn.run("`temp in objs(true).name")->getBool());
    cout << "Now datanode has changed to " + third_node << endl;
    Util::sleep(2000);

    restartAllDataNodes(nodeNames, ctl_port);
    cout << "test_basicHA passed! " << endl;
}

void test_uploadHA()
{
    static DBConnection conn(false, false);
    conn.connect(hostName, sub_port, "admin", "123456", "", true, sites, 7200, false);

    vector<string> colNames = {"sym", "value", "datetime"};
    vector<DATA_TYPE> colTypes = {DT_STRING, DT_INT, DT_DATETIME};
    int colNum = 3, rowNum = 1000;
    TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1000);
    vector<VectorSP> columnVecs;
    columnVecs.reserve(colNum);
    for (int i = 0; i < colNum; i++)
    {
        columnVecs.emplace_back(tab2->getColumn(i));
    }
    for (int i = 0; i < rowNum; i++)
    {
        columnVecs[0]->set(i, Util::createString("stu_" + to_string(rand() % rowNum)));
        columnVecs[1]->set(i, Util::createInt(rand() % rowNum));
        columnVecs[2]->set(i, Util::createDateTime(rand() % rowNum));
    }

    string first_node = conn.run("getNodeAlias()")->getString();
    cout << "Current datanode is " + first_node << endl;
    conn.upload("tab2", {tab2});
    TableSP res_1 = conn.run("tab2");
    assert(tab2->getString() == res_1->getString());
    cout << "upload table successed" << endl
         << endl;
    Util::sleep(2000);
    stopNodeTask(hostName, ctl_port, first_node);

    columnVecs.clear();
    for (int i = 0; i < rowNum; i++)
    {
        columnVecs[0]->set(i, Util::createString("stu_" + to_string(rand() % rowNum)));
        columnVecs[1]->set(i, Util::createInt(rand() % rowNum));
        columnVecs[2]->set(i, Util::createDateTime(rand() % rowNum));
    }

    string second_node = conn.run("getNodeAlias()")->getString();
    cout << "Now datanode has changed to " + second_node << endl;
    conn.upload("tab2", {tab2});
    TableSP res_2 = conn.run("tab2");
    // EXPECT_NE(res_1->getString(),res_2->getString());
    assert(tab2->getString() == res_2->getString());
    cout << "upload table successed" << endl
         << endl;
    Util::sleep(2000);
    stopNodeTask(hostName, ctl_port, second_node);

    columnVecs.clear();
    for (int i = 0; i < rowNum; i++)
    {
        columnVecs[0]->set(i, Util::createString("stu_" + to_string(rand() % rowNum)));
        columnVecs[1]->set(i, Util::createInt(rand() % rowNum));
        columnVecs[2]->set(i, Util::createDateTime(rand() % rowNum));
    }
    string third_node = conn.run("getNodeAlias()")->getString();
    cout << "Now datanode has changed to " + third_node << endl;
    conn.upload("tab2", {tab2});
    TableSP res_3 = conn.run("tab2");
    // EXPECT_NE(res_3->getString(),res_2->getString());
    assert(tab2->getString() == res_3->getString());
    cout << "upload table successed" << endl
         << endl;

    restartAllDataNodes(nodeNames, ctl_port);
    cout << "test_uploadHA passed! " << endl;
}

// pair<StreamDeserializerSP, TableSP> createStreamDeserializer(const int rows) {
//     DBConnection conn(false, false);
//     conn.connect(hostName, publish_port, "admin", "123456");

//     string del_streamtable="login(\"admin\",\"123456\");\
//                             try{ dropStreamTable(`SDoutTables);dropStreamTable(`st2) }catch(ex){};try{ undef(`SDoutTables,SHARED) }catch(ex){}";
//     conn.run(del_streamtable);
// 	string script = "login(\"admin\",\"123456\")\n\
//             st2 = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n\
//             enableTableShareAndPersistence(table=st2, tableName=`SDoutTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\n\
//             go\n\
//             setStreamTableFilterColumn(SDoutTables, `sym)";
// 	conn.run(script);

// 	string replayScript = "n = "+to_string(rows)+";table1_SDPT = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);\
//             table2_SDPT = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);\
//             tableInsert(table1_SDPT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));\
//             tableInsert(table2_SDPT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));\
//             d = dict(['msg1','msg2'], [table1_SDPT, table2_SDPT]);\
//             replay(inputTables=d, outputTables=`SDoutTables, dateColumn=`timestampv, timeColumn=`timestampv,replayRate=10,absoluteRate=true)";
// 	conn.run(replayScript);

// 	DictionarySP t1s = conn.run("schema(table1_SDPT)");
// 	DictionarySP t2s = conn.run("schema(table2_SDPT)");
// 	unordered_map<string, DictionarySP> sym2schema;
// 	sym2schema["msg1"] = t1s;
// 	sym2schema["msg2"] = t2s;
//     StreamDeserializerSP sdsp = new StreamDeserializer(sym2schema);
//     TableSP tab1 = conn.run("select * from table1_SDPT")->getInstance();
//     return {sdsp,tab1};
// }

// void test_streaming_threadedClient_HA(const int test_rows){
//     pair<StreamDeserializerSP, TableSP> pair1 = createStreamDeserializer(test_rows);
//     StreamDeserializerSP sdsp = pair1.first;
//     TableSP res = pair1.second;

//     srand(time(0));
//     int msg_total=0;
//     int listenport = 13000 + rand() % 2000;
// 	Signal notify;
//     Mutex mutex;

//     auto onehandler = [&](Message msg) {
//         // const string &symbol = msg.getSymbol();
//         LockGuard<Mutex> lock(&mutex);
//         string err;
//         vector<ConstantSP> rowVal;
//         VectorSP msgV = (VectorSP)msg;
//         // cout<<msgV->getString()<<endl;

//         if(msgV->size() != res->columns())
//             msgV->append(Util::createNullConstant(DT_DOUBLE));
//         for(auto i = 0;i<msgV->size();i++)
//             rowVal.emplace_back(msgV->get(i));

//         int row = 1;
//         res->append(rowVal, row, err);
//         // cout<<err<<endl;
//         msg_total += 1;

//         cout << "total subscribed rows: "+to_string(msg_total)<<endl;
//         Util::sleep(1000);
//         if (msg_total == test_rows*2) {
//             notify.set();
//         }
//     };

//     ThreadedClient threadedClient(listenport);
//     auto thread1 = threadedClient.subscribe(hostName, publish_port, onehandler, "SDoutTables", "test_HA", 0, true,nullptr, false, false, "admin", "123456",sdsp);
//     notify.wait();
//     threadedClient.unsubscribe(hostName, publish_port, "SDoutTables", "test_HA");

//     assert(res->rows() == test_rows*2);
// }

// void test_streaming_ThreadPooledClient_HA(const int test_rows){
//     pair<StreamDeserializerSP, TableSP> pair1 = createStreamDeserializer(test_rows);
//     StreamDeserializerSP sdsp = pair1.first;
//     TableSP res = pair1.second;

//     srand(time(0));
//     int msg_total=0;
//     int listenport = 13000 + rand() % 2000;
// 	Signal notify;
//     Mutex mutex;

//     auto onehandler = [&](Message msg) {
//         // const string &symbol = msg.getSymbol();
//         LockGuard<Mutex> lock(&mutex);
//         string err;
//         vector<ConstantSP> rowVal;
//         VectorSP msgV = (VectorSP)msg;
//         // cout<<msgV->getString()<<endl;

//         if(msgV->size() != res->columns())
//             msgV->append(Util::createNullConstant(DT_DOUBLE));
//         for(auto i = 0;i<msgV->size();i++)
//             rowVal.emplace_back(msgV->get(i));

//         int row = 1;
//         res->append(rowVal, row, err);
//         // cout<<err<<endl;
//         msg_total += 1;

//         cout << "total subscribed rows: "+to_string(msg_total)<<endl;
//         Util::sleep(1000);
//         if (msg_total == test_rows*2) {
//             notify.set();
//         }
//     };

//     ThreadPooledClient client(listenport, 10);
//     auto threadVec = client.subscribe(hostName, publish_port, onehandler, "SDoutTables", "test_HA", 0, true,nullptr, false,false, "admin","123456",sdsp);
//     assert(threadVec.size() == 10);
//     notify.wait();

//     client.unsubscribe(hostName, publish_port, "SDoutTables", "test_HA");

//     assert(res->rows() == test_rows*2);
// }

// void test_streaming_PollingClient_HA(const int test_rows){
//     pair<StreamDeserializerSP, TableSP> pair1 = createStreamDeserializer(test_rows);
//     StreamDeserializerSP sdsp = pair1.first;
//     TableSP res = pair1.second;

//     srand(time(0));
//     int msg_total=0;
//     int listenport = 13000 + rand() % 2000;

//     PollingClient client(listenport);
//     auto queue = client.subscribe(hostName, publish_port,"SDoutTables", "test_HA", 0, true,nullptr, false,false, "admin","123456",sdsp);

//     vector<Message> msgs;
//     while(queue->pop(msgs,1000)) {
//         for (auto &msg : msgs) {
//             // const string &symbol = msg.getSymbol();
//             string err;
//             vector<ConstantSP> rowVal;
//             VectorSP msgV = (VectorSP)msg;
//             // cout<<msgV->getString()<<endl;

//             if(msgV->size() != res->columns())
//                 msgV->append(Util::createNullConstant(DT_DOUBLE));
//             for(auto i = 0;i<msgV->size();i++)
//                 rowVal.emplace_back(msgV->get(i));

//             int row = 1;
//             res->append(rowVal, row, err);
//             // cout<<err<<endl;
//             msg_total += 1;

//             cout << "total subscribed rows: "+to_string(msg_total)<<endl;
//             Util::sleep(1000);
//         }
//     }

//     client.unsubscribe(hostName, publish_port, "SDoutTables", "test_HA");

//     assert(res->rows() == test_rows*2);
// }

void test_DBConnection_HA_datanodeNotRunning()
{
    string init_script = "try{undef(`share_t, SHARED)}catch(ex){};t=table(`1`2`3 as col1, 1 2 3 as col2);share t as share_t;go";
    stopNodeTask(hostName, ctl_port, node_name);

    thread th1 = thread([&]
                        {
        DBConnection conn(false, false);
        cout << "th1 msg: Try to connect to " + node_name<<endl;
        conn.connect(hostName, sub_port, "admin", "123456", init_script, true, sites, 7200, false);
        string nodeName = conn.run("getNodeAlias()")->getString();
        cout << "th1 msg: Actually connect to " + nodeName<<endl;
        assert(conn.run("`share_t in objs(true).name and (exec shared from objs(true) where name = `share_t)[0]")->getBool());
        cout<<"------------------------------->>> test_DBConnection_HA_datanodeisStopped pass"<<endl;
        conn.close();
        return; });
    th1.join();
    restartNodeTask(hostName, ctl_port, node_name, 2);
}

void test_DBConnection_HA_datanodeAllStopped()
{
    string init_script = "try{undef(`share_t, SHARED)}catch(ex){};t=table(`1`2`3 as col1, 1 2 3 as col2);share t as share_t;go";
    DBConnection ctl(false, false);
    ctl.connect(hostName, ctl_port, "admin", "123456");
    for (auto &node : nodeNames)
    {
        stopNodeTask(hostName, ctl_port, node);
    }

    thread th1 = thread([&]
                        {
        DBConnection conn(false, false);
        cout << "th1 msg: Try to connect to " + node_name<<endl;
        conn.connect(hostName, sub_port, "admin", "123456", init_script, true, sites, 7200, false);
        string nodeName = conn.run("getNodeAlias()")->getString();
        cout << "th1 msg: Actually connect to " + nodeName<<endl;
        assert(nodeName == "datanode3");
        assert(conn.run("`share_t in objs(true).name and (exec shared from objs(true) where name = `share_t)[0]")->getBool());
        cout<<"------------------------------->>> test_DBConnection_HA_datanodeAllStopped pass"<<endl;
        conn.close();
        return; });
    thread th2 = thread(restartNodeTask, hostName, ctl_port, "datanode3", 2);
    th1.join();
    th2.join();
    restartAllDataNodes(nodeNames, ctl_port);
}

void test_insertToDFS_HA(const int test_rows)
{
    DBConnection conn(false, false);
    conn.connect(hostName, sub_port, "admin", "123456", "", true, sites, 7200, false);

    string script;
    script += "login(`admin,`123456);";
    script += "dbPath = \"dfs://test_dfs_HA\";";
    script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
    script += "db = database(dbPath,HASH,[DATE,2]);";
    script += "dummy = table(1:0, `id`sym`date`value, [INT, SYMBOL, DATE, INT]);";
    script += "pt_dfs_HA = db.createPartitionedTable(dummy,`pt_dfs_HA,`date);";
    conn.run(script);

    int index = 0;
    while (index != test_rows)
    {
        index += 1;
        string script1 = "tmp=table(" + to_string(index) + " as id, rand(`A`B`C`D, 1) as sym, date(" + to_string(index) + ") as date, " + to_string(index) + " as value);\
                            tableInsert(loadTable(\"dfs://test_dfs_HA\",`pt_dfs_HA),tmp);";
        ConstantSP res = conn.run(script1);
        if (conn.run("exec count(*) from loadTable(\"dfs://test_dfs_HA\",`pt_dfs_HA)")->getInt() != index)
        {
            cout << "Insert failed" << endl;
            break;
        }

        cout << endl
             << "total insert rows are " + conn.run("exec count(*) from loadTable(\"dfs://test_dfs_HA\", `pt_dfs_HA)")->getString() << endl;
        string nodeName = conn.run("getNodeAlias()")->getString();
        cout << "Now datanode is " + nodeName << endl;
        stopNodeTask(hostName, ctl_port, nodeName);
        restartNodeTask(hostName, ctl_port, nodeName, 1);
    };

    assert(conn.run("exec count(*) from loadTable(\"dfs://test_dfs_HA\", `pt_dfs_HA)")->getInt() == index);
    conn.run("undef all;");
    conn.close();
}

void test_MTW_HA_1(const int test_rows)
{
    DBConnection conn(false, false);
    conn.connect(hostName, sub_port, "admin", "123456", "", true, sites, 7200, false);

    string dbName = "dfs://test_MTW_HA";
    string script = "dbName = \"dfs://test_MTW_HA\"\n"
                    "if(exists(dbName)){\n"
                    "\tdropDatabase(dbName)\t\n"
                    "}\n"
                    "db  = database(dbName, HASH,[TIME, 4]);\n";
    script += "t = table(1000:0, `symbol`date`value,[SYMBOL, TIME, INT]);"
              "pt = db.createPartitionedTable(t,`pt,`date);";
    conn.run(script);
    SmartPointer<MultithreadedTableWriter> mulwrite;
    ErrorCodeInfo pErrorInfo;
    mulwrite = new MultithreadedTableWriter(hostName, sub_port, "admin", "123456", dbName, "pt", false, true, &sites, 1000, 1, 4, "date");
    string sym[] = {"A", "B", "C", "D"};

    for (auto i = 0; i < test_rows; i++)
    {
        assert(mulwrite->insert(pErrorInfo, sym[rand() % 4], i + 200, i + 99));
        if (i == test_rows / 2)
        {
            stopNodeTask(hostName, ctl_port, "datanode4");
            restartNodeTask(hostName, ctl_port, "datanode4", 1);
        }
    }
    mulwrite->waitForThreadCompletion();

    if (pErrorInfo.errorCode != "")
    {
        cout << "err1: " << pErrorInfo.errorInfo << endl;
        cout << "------------------------------->>> test_MTW_HA_1 failed" << endl;
        conn.run("undef all;");
        conn.close();
        return;
    }

    ConstantSP t1 = conn.run("select * from loadTable(\"dfs://test_MTW_HA\",`pt) order by date;");

    assert(conn.run("exec count(*) from loadTable(\"dfs://test_MTW_HA\", `pt)")->getInt() == t1->rows());
    cout << "------------------------------->>> test_MTW_HA_1 passed" << endl;
    conn.run("undef all;");
    conn.close();
}

void test_MTW_HA_2(const int test_rows)
{
    DBConnection conn(false, false);
    conn.connect(hostName, sub_port, "admin", "123456", "", true, sites, 7200, false);

    string dbName = "dfs://test_MTW_HA_2";
    string script = "dbName = \"dfs://test_MTW_HA_2\"\n"
                    "if(exists(dbName)){\n"
                    "\tdropDatabase(dbName)\t\n"
                    "}\n"
                    "db  = database(dbName, HASH,[TIME, 4]);\n";
    script += "t = table(1000:0, `symbol`date`value,[SYMBOL, TIME, INT]);"
              "pt = db.createPartitionedTable(t,`pt,`date);";
    conn.run(script);
    SmartPointer<MultithreadedTableWriter> mulwrite;
    ErrorCodeInfo pErrorInfo;
    mulwrite = new MultithreadedTableWriter(hostName, sub_port, "admin", "123456", dbName, "pt", false, true, &sites, 1000, 1, 4, "date");
    string sym[] = {"A", "B", "C", "D"};
    vector<vector<ConstantSP> *> datas;
    srand((int)time(NULL));
    for (int i = 0; i < test_rows; i++)
    {
        vector<ConstantSP> *prow = new vector<ConstantSP>;
        vector<ConstantSP> &row = *prow;
        row.push_back(Util::createString(sym[rand() % 4]));
        row.push_back(Util::createTime(i));
        row.push_back(Util::createInt(i + 64));
        datas.push_back(prow);
    }

    std::thread th1 = thread([&]
                             { mulwrite->insertUnwrittenData(datas, pErrorInfo); });
    std::thread th2 = thread([=]
                             {
        MultithreadedTableWriter::Status status;
        do{
            mulwrite->getStatus(status);
            if(status.sentRows >= test_rows/2){
                stopNodeTask(hostName, ctl_port, "datanode4");
                restartNodeTask(hostName, ctl_port, "datanode4", 1);
                break;
            }
        }while (true); });

    th2.join();
    th1.join();

    if (pErrorInfo.errorCode != "")
    {
        cout << "err2: " << pErrorInfo.errorInfo << endl;
        cout << "------------------------------->>> test_MTW_HA_2 failed" << endl;
        conn.run("undef all;");
        conn.close();
        return;
    }

    assert(conn.run("exec count(*) from loadTable(\"dfs://test_MTW_HA_2\", `pt)")->getInt() == test_rows);
    cout << "------------------------------->>> test_MTW_HA_2 passed" << endl;
    conn.run("undef all;");
    conn.close();
}

void test_DBConnectionPool_HA(const int test_rows)
{
    DBConnectionPool pool(hostName, sub_port, 10, "admin", "123456", false, true);
    DBConnection conn(false, false);
    conn.connect(hostName, sub_port, "admin", "123456", "", true, sites, 7200, false);

    string script;
    script += "login(`admin,`123456);";
    script += "dbPath = \"dfs://test_dfs_HA\";";
    script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
    script += "db = database(dbPath,HASH,[DATE,2]);";
    script += "dummy = table(1:0, `id`sym`date`value, [INT, SYMBOL, DATE, INT]);";
    script += "pt_dfs_HA = db.createPartitionedTable(dummy,`pt_dfs_HA,`date);";
    conn.run(script);

    vector<string> colName = {"id", "sym", "date", "value"};
    vector<ConstantSP> cols = {};

    VectorSP col1 = Util::createVector(DT_INT, 0, test_rows);
    VectorSP col2 = Util::createVector(DT_SYMBOL, 0, test_rows);
    VectorSP col3 = Util::createVector(DT_DATE, 0, test_rows);
    VectorSP col4 = Util::createVector(DT_INT, 0, test_rows);

    for (auto i = 0; i < test_rows; i++)
    {
        col1->append(Util::createInt(i));
        col2->append(Util::createString("symbol1"));
        col3->append(Util::createDate(i));
        col4->append(Util::createInt(i + 100));
    }

    cols.push_back(col1);
    cols.push_back(col2);
    cols.push_back(col3);
    cols.push_back(col4);

    TableSP tab = Util::createTable(colName, cols);

    vector<ConstantSP> args{tab};
    pool.run("tableInsert{loadTable(\"dfs://test_dfs_HA\", `pt_dfs_HA)}", args, 1);
    int rows = 0;
    while (!pool.isFinished(1))
    {
        rows = conn.run("exec count(*) from loadTable(\"dfs://test_dfs_HA\", `pt_dfs_HA)")->getInt();
        if (rows >= test_rows / 2)
        {
            stopNodeTask(hostName, ctl_port, "datanode4");
            restartNodeTask(hostName, ctl_port, "datanode4", 1);
        }
    };

    assert(pool.getData(1)->getBool());

    conn.upload("tab", tab);
    conn.run("share tab as temp");
    ConstantSP res = conn.run("res = exec * from loadTable(\"dfs://test_dfs_HA\", `pt_dfs_HA) order by id;each(eqObj,res.values(),tab.values())");
    for (int i = 0; i < res->size(); i++)
    {
        assert(res->get(i)->getBool());
    }

    cout << "test_DBConnectionPool_HA passed..." << endl;
    conn.run("undef all;");
    conn.close();
    pool.shutDown();
}

int main(int argc, char **argv)
{
    string temp = argv[1];
    int testNum = atoi(temp.c_str());

    try
    {
        if (testNum == 1)
        {
            test_basicHA();
            test_uploadHA();
            test_DBConnection_HA_datanodeNotRunning();
            test_DBConnection_HA_datanodeAllStopped();
        }
        else if (testNum == 2)
        {
            // test_streaming_PollingClient_HA(10);
            // test_streaming_threadedClient_HA(10);
            // test_streaming_ThreadPooledClient_HA(10);
        }
        else if (testNum == 3)
        {
            test_insertToDFS_HA(10);
        }
        else if (testNum == 4)
        {
            test_MTW_HA_1(1000000);
            test_MTW_HA_2(1000000);
        }
        else if (testNum == 5)
        {
            test_DBConnectionPool_HA(10000000);
        }
    }
    catch (const exception &e)
    {
        cout << "Exception was throwed out: " << endl
             << '\t' << e.what() << endl;
    }

    return 0;
}