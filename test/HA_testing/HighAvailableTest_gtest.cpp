#include "../config.h"


#ifndef _WIN32
class HighAvailableTest : public testing::Test
{
public:
	// Case
	virtual void SetUp()
	{
        restartAllNodes(hostName, ctl_port);
	}
    virtual void TearDown()
    {
        restartAllNodes(hostName, ctl_port);
    }
    static void stopNode(const string& host, const int& controllerPort, const string& nodeName);
    static void restartNode(const string& host, const int& controllerPort, const string& nodeName);
    static void restartAllNodes(const string& host, const int& controllerPort);
};


void HighAvailableTest::stopNode(const string& host, const int& controllerPort, const string& nodeName)
{
    cout << "Try to stop current Datanode: " + nodeName << endl;
    DBConnection _conn(false, false);
    _conn.connect(host, controllerPort, "admin", "123456");

    int status = _conn.run("(exec state from getClusterPerf() where name = `" + nodeName + ")[0]")->getInt();
    int try_count = 0;
    while (status == 1 && try_count < 60) {
        _conn.run("try{stopDataNode(\"" + nodeName + "\");}catch(ex){}");
        status = _conn.run("(exec state from getClusterPerf() where name = `" + nodeName + ")[0]")->getInt();
        try_count++;
        Util::sleep(1000);
    }
    if (status == 1) {
        // cout << "Stop failed." << endl;
        throw runtime_error("Stop failed after 60 seconds.");
    }else{
        cout << "Stop successfully." << endl;
    }
    _conn.close();
}

void HighAvailableTest::restartNode(const string& host, const int& controllerPort, const string& nodeName)
{
    DBConnection _conn(false, false);
    _conn.connect(host, controllerPort, "admin", "123456");
    cout << "Restart the Datanode: " + nodeName << endl;

    int status = _conn.run("(exec state from getClusterPerf() where name = `" + nodeName + ")[0]")->getInt();
    int try_count = 0;
    while (status == 0 && try_count < 60) {
        _conn.run("startDataNode(\"" + nodeName + "\")");
        status = _conn.run("(exec state from getClusterPerf() where name = `" + nodeName + ")[0]")->getInt();
        Util::sleep(1000);
        try_count++;
    }
    if (status == 0) {
        // cout << "Restart failed." << endl;
        throw runtime_error("Restart failed after 60 seconds.");
    }else{
        cout << "Restart successfully." << endl;
    }
    _conn.close();
}

void HighAvailableTest::restartAllNodes(const string& host, const int& controllerPort)
{
    DBConnection _conn(false, false);
    _conn.connect(host, controllerPort, "admin", "123456");

    if (_conn.run("all((exec state from getClusterPerf() where mode in 0 4) == 1)")->getBool()){
        return;
    }

    VectorSP nodes = _conn.run("nodes = exec name from getClusterPerf() where mode in 0 4 and state = 0;nodes");
    bool state = _conn.run("all((exec state from getClusterPerf()) == 1)")->getBool();

    int try_count = 0;
    while (try_count < 60 && state == false) {
        cout << "Restart the Datanode: " + nodes->getString() << endl;
        _conn.run("startDataNode(nodes)");
        if (_conn.run("all((exec state from getClusterPerf()) == 1)")->getBool()){
            break;
        }
        try_count++;
        Util::sleep(1000);
    }
    if (try_count == 60) {
        // cout << "Restart failed." << endl;
        throw runtime_error("Restart failed after 60 seconds.");
    }else{
        cout << "Restart successfully." << endl;
    }
    _conn.close();
}

TEST_F(HighAvailableTest, test_connHA_basic){
    DBConnectionSP controller = new DBConnection(false, false);
    controller->connect(hostName, ctl_port, "admin", "123456");
    controller->run("tmp = select name,port from getClusterPerf() where mode in 0 4");
    VectorSP ports = controller->run("tmp.port");
    srand((int)time(NULL));
    int index = rand() % ports->size();
    int nodePort = ports->getInt(index);
    string nodeName = controller->run("exec name from getClusterPerf() where mode in 0 4 and port = " + to_string(nodePort))->get(0)->getString();
    controller->close();
    cout << "selected node to connect: " << nodeName << endl;

    string init_script = "temp=table(`1`2`3 as col1, 1 2 3 as col2);";
    conn.connect(hostName, nodePort, "admin", "123456", init_script, true, sites, 7200, false);

    string first_node = conn.run("getNodeAlias()")->getString();

    cout << "Current datanode is " + first_node << endl;
    Util::sleep(2000);
    stopNode(hostName, ctl_port, first_node);

    string second_node = conn.run("getNodeAlias()")->getString();
    EXPECT_TRUE(first_node != second_node);
    EXPECT_TRUE(conn.run("`temp in objs(true).name")->getBool());
    cout << "Now datanode has changed to " + second_node << endl;
    Util::sleep(2000);
    stopNode(hostName, ctl_port, second_node);

    string third_node = conn.run("getNodeAlias()")->getString();
    EXPECT_TRUE(third_node != second_node);
    EXPECT_TRUE(conn.run("`temp in objs(true).name")->getBool());
    cout << "Now datanode has changed to " + third_node << endl;

}


TEST_F(HighAvailableTest, test_connHA_datanodeNotRunning)
{
    conn.connect(hostName, port, "admin", "123456");
    string node_name = conn.run("getNodeAlias()")->getString();
    string init_script = "try{undef(`share_t, SHARED)}catch(ex){};t=table(`1`2`3 as col1, 1 2 3 as col2);share t as share_t;go";
    stopNode(hostName, ctl_port, node_name);

    thread th1 = thread([&]
                        {
        DBConnection conn(false, false);
        cout << "th1 msg: Try to connect to " + node_name<<endl;
        conn.connect(hostName, port, "admin", "123456", init_script, true, sites, 7200, false);
        string second_node = conn.run("getNodeAlias()")->getString();
        cout << "th1 msg: Actually connect to " + second_node << endl;
        EXPECT_NE(node_name, second_node);
        EXPECT_TRUE(conn.run("`share_t in objs(true).name and (exec shared from objs(true) where name = `share_t)[0]")->getBool());
        conn.close();
    });
    th1.join();
}


TEST_F(HighAvailableTest, test_connHA_datanodeAllStopped)
{
    string init_script = "try{undef(`share_t, SHARED)}catch(ex){};t=table(`1`2`3 as col1, 1 2 3 as col2);share t as share_t;go";
    DBConnection ctl(false, false);
    ctl.connect(hostName, ctl_port, "admin", "123456");
    for (auto &node : nodeNames)
    {
        stopNode(hostName, ctl_port, node);
    }

    const string restart_node = nodeNames[2];
    thread th1 = thread([&]
                        {
        DBConnection conn(false, false);
        conn.connect(hostName, port, "admin", "123456", init_script, true, sites, 7200, false);
        string nodeName = conn.run("getNodeAlias()")->getString();
        cout << "th1 msg: Actually connect to " + nodeName<<endl;
        EXPECT_EQ(nodeName, restart_node);
        EXPECT_TRUE(conn.run("`share_t in objs(true).name and (exec shared from objs(true) where name = `share_t)[0]")->getBool());
        conn.close();
        });
    thread th2 = thread(restartNode, hostName, ctl_port, restart_node);
    th1.join();
    th2.join();
}


TEST_F(HighAvailableTest, test_MTW_HA)
{
    const int test_rows = 1000000;
    DBConnection conn(false, false);
    conn.connect(hostName, port, "admin", "123456", "", true, sites, 7200, false);
    string node_name = conn.run("getNodeAlias()")->getString();

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
    mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, true, &sites, 100, 1, 1, "date");
    string sym[] = {"A", "B", "C", "D"};

    for (auto i = 0; i < test_rows; i++)
    {
        EXPECT_TRUE(mulwrite->insert(pErrorInfo, sym[rand() % 4], i + 200, i + 99));
        if (i == test_rows / 2)
        {
            stopNode(hostName, ctl_port, node_name);
        }
    }
    mulwrite->waitForThreadCompletion();
    EXPECT_FALSE(pErrorInfo.hasError());

    ConstantSP t1 = conn.run("select * from loadTable(\"dfs://test_MTW_HA\",`pt) order by date;");
    EXPECT_EQ(t1->rows(), test_rows);
    conn.close();
}



TEST_F(HighAvailableTest, test_MTW_HA_2)
{
    const int test_rows = 1000000;
    DBConnection conn(false, false);
    conn.connect(hostName, port, "admin", "123456", "", true, sites, 7200, false);
    string node_name = conn.run("getNodeAlias()")->getString();

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
    mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, true, &sites, 1000, 1, 4, "date");
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
    MultithreadedTableWriter::Status status;
    std::thread th1 = thread([&]
                             { mulwrite->insertUnwrittenData(datas, pErrorInfo); });
    std::thread th2 = thread([&]{
        do{
            mulwrite->getStatus(status);
            if(status.sentRows >= test_rows/2){
                stopNode(hostName, ctl_port, node_name);
                break;
            }
        }while (true); 
    });

    th2.join();
    th1.join();

    mulwrite->waitForThreadCompletion();
    EXPECT_FALSE(pErrorInfo.hasError());

    EXPECT_EQ(conn.run("exec count(*) from loadTable(\"dfs://test_MTW_HA_2\", `pt)")->getInt(), test_rows);
    conn.close();
}



TEST_F(HighAvailableTest, test_DBConnectionPool_HA)
{
    const int test_rows = 100000;
    DBConnectionPool pool(hostName, port, 10, "admin", "123456", false, true);
    DBConnection conn(false, false);
    conn.connect(hostName, port, "admin", "123456", "", true, sites, 7200, false);
    string node_name = conn.run("getNodeAlias()")->getString();

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
            stopNode(hostName, ctl_port, node_name);
            break;
        }
    };

    EXPECT_TRUE(pool.getData(1)->getBool());

    conn.upload("tab", tab);
    conn.run("share tab as temp");
    ConstantSP res = conn.run("res = exec * from loadTable(\"dfs://test_dfs_HA\", `pt_dfs_HA) order by id;each(eqObj,res.values(),tab.values())");
    for (int i = 0; i < res->size(); i++)
    {
        EXPECT_TRUE(res->get(i)->getBool());
    }

    conn.close();
    pool.shutDown();
}

#endif // not windows