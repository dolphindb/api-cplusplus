class DBConnectHighAvailableTest:public testing::Test
{
protected:
    //Suite
    static void SetUpTestCase() {
        //DBConnection conn;
        conn.initialize();
        bool ret = conn.connect(hostName, port, "admin", "123456", "", true, sites, 7200, false);
        if (!ret) {
            cout << "Failed to connect to the server" << endl;
        }
    }
    static void TearDownTestCase(){
        conn.close();
    }

    //Case
    virtual void SetUp()
    {
        cout<<"check connect...";
		ConstantSP res = conn.run("1+1");
		if(!(res->getBool())){
			cout<<"Server not responed, please check."<<endl;
		}
		else
		{
			cout<<"ok"<<endl;
            
		}
    }
    virtual void TearDown()
    {
        DBConnection conn_restartnode(false,false);
        bool ret = conn_restartnode.connect(hostName, ctl_port, "admin", "123456");
        for(unsigned int j = 0; j < nodeNames.size(); j++){
            conn_restartnode.run("try{startDataNode(\"" + nodeNames[j] + "\")}catch(ex){};");
        }
        conn_restartnode.close();
    }
};

bool shellStopCurNode(vector<string>& nodes, string& cur_node){
    cout<<"Try to stop current Datanode: "+cur_node<<endl;
    DBConnection conn2(false,false);
    conn2.connect(hostName, ctl_port, "admin", "123456");

    conn2.run("try{stopDataNode(\"" + cur_node + "\")}catch(ex){};");
    cout<<"Stop successfully."<<endl;
    conn2.close();
    return true;

}

TEST_F(DBConnectHighAvailableTest, test_basicHA){
    string first_node = conn.run("getNodeAlias()")->getString();

    cout << "Current datanode is "+first_node<<endl;
    Util::sleep(2000);
    shellStopCurNode(nodeNames, first_node);

    string second_node = conn.run("getNodeAlias()")->getString(); 
    EXPECT_FALSE(first_node==second_node);
    cout << "Now datanode has changed to "+second_node<<endl;
    Util::sleep(2000);
    shellStopCurNode(nodeNames, second_node);

    string third_node = conn.run("getNodeAlias()")->getString();
    EXPECT_FALSE(third_node==second_node);
    cout << "Now datanode has changed to "+third_node<<endl;
    Util::sleep(2000);
}

TEST_F(DBConnectHighAvailableTest, test_uploadHA){
	vector<string> colNames = { "sym", "value","datetime"};
	vector<DATA_TYPE> colTypes = { DT_STRING, DT_INT,DT_DATETIME };
	int colNum = 3, rowNum = 1000;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
    for (int i = 0; i < rowNum; i++){
	    columnVecs[0]->set(i, Util::createString("stu_"+to_string(rand()%rowNum)));
        columnVecs[1]->set(i, Util::createInt(rand()%rowNum));
        columnVecs[2]->set(i, Util::createDateTime(rand()%rowNum));
    }

    string first_node = conn.run("getNodeAlias()")->getString();
    cout << "Current datanode is "+first_node<<endl;
    conn.upload("tab2",{tab2});
    TableSP res_1=conn.run("tab2");
    EXPECT_EQ(tab2->getString(),res_1->getString());
    cout << "upload table successed"<<endl<<endl;
    Util::sleep(2000);
    shellStopCurNode(nodeNames, first_node);

    columnVecs.clear();
    for (int i = 0; i < rowNum; i++){
	    columnVecs[0]->set(i, Util::createString("stu_"+to_string(rand()%rowNum)));
        columnVecs[1]->set(i, Util::createInt(rand()%rowNum));
        columnVecs[2]->set(i, Util::createDateTime(rand()%rowNum));
    }

    string second_node = conn.run("getNodeAlias()")->getString();  
    cout << "Now datanode has changed to "+second_node<<endl;
    conn.upload("tab2",{tab2});
    TableSP res_2=conn.run("tab2");
    // EXPECT_NE(res_1->getString(),res_2->getString());
    EXPECT_EQ(tab2->getString(),res_2->getString());
    cout << "upload table successed"<<endl<<endl;
    Util::sleep(2000);
    shellStopCurNode(nodeNames, second_node);

    columnVecs.clear();
    for (int i = 0; i < rowNum; i++){
	    columnVecs[0]->set(i, Util::createString("stu_"+to_string(rand()%rowNum)));
        columnVecs[1]->set(i, Util::createInt(rand()%rowNum));
        columnVecs[2]->set(i, Util::createDateTime(rand()%rowNum));
    }
    string third_node = conn.run("getNodeAlias()")->getString();
    cout<< "Now datanode has changed to "+third_node<<endl;
    conn.upload("tab2",{tab2});
    TableSP res_3=conn.run("tab2");
    // EXPECT_NE(res_3->getString(),res_2->getString());
    EXPECT_EQ(tab2->getString(),res_3->getString());
    cout << "upload table successed"<<endl<<endl;
}

TEST_F(DBConnectHighAvailableTest, test_streamingHA){
    // 手工回放流表脚本如下，GUI执行
    // try{ dropStreamTable(`SDoutTables);dropStreamTable(`st2);dropDistributedInMemoryTable(`t1_SDPT);dropDistributedInMemoryTable(`t2_SDPT) }catch(ex){}
    // go
    // login("admin","123456")
    // st2 = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])
    // enableTableShareAndPersistence(table=st2, tableName=`SDoutTables, asynWrite=true, compress=true, cacheSize=200000,retentionMinutes=180, preCache = 0)
    // go
    // setStreamTableFilterColumn(SDoutTables, `sym)
    // go
    // n = 1000;
    // pt1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);
    // pt2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);
    // table1_SDPT=createDistributedInMemoryTable(`t1_SDPT,`datetimev`timestampv`sym`price1`price2,`DATETIME`TIMESTAMP`SYMBOL`DOUBLE`DOUBLE,HASH,[SYMBOL,2],`sym)
    // table2_SDPT=createDistributedInMemoryTable(`t2_SDPT,`datetimev`timestampv`sym`price1,`DATETIME`TIMESTAMP`SYMBOL`DOUBLE,HASH,[SYMBOL,2],`sym)
    // go
    // tableInsert(pt1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));
    // tableInsert(pt2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));
    // table1_SDPT.append!(pt1)
    // table2_SDPT.append!(pt2)
    // d = dict(['msg1','msg2'], [table1_SDPT, table2_SDPT]);
    // go
    // replay(inputTables=d, outputTables=`SDoutTables, dateColumn=`timestampv, timeColumn=`timestampv,replayRate=1,absoluteRate=true)

	DictionarySP t1s = conn.run("schema(loadDistributedInMemoryTable(`t1_SDPT))");
	DictionarySP t2s = conn.run("schema(loadDistributedInMemoryTable(`t2_SDPT))");
	unordered_map<string, DictionarySP> sym2schema;
	sym2schema["msg1"] = t1s;
	sym2schema["msg2"] = t2s;
    StreamDeserializerSP sdsp = new StreamDeserializer(sym2schema);

    srand(time(0));
    int msg_total=0;
    int listenport = rand() % 13000 + 2000;
	Signal notify;
    Mutex mutex;

    auto onehandler = [&](Message msg) {
        const string &symbol = msg.getSymbol();
        LockGuard<Mutex> lock(&mutex);
        msg_total+=1;
        cout<<msg->getString()<<endl;
        cout << "total subscribed rows: "+to_string(msg_total)<<endl;
        cout << "current datanode: "+conn.run("getNodeAlias()")->getString()<<endl<<endl;
        if (msg_total == 2000) {
            notify.set();
        }
    };
    ThreadedClient threadedClient(listenport);
    auto thread1 = threadedClient.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true,nullptr, false, false, "admin", "123456",sdsp);
    thread1->setAffinity(0);
    notify.wait();
    threadedClient.unsubscribe(hostName, port, "SDoutTables", "test_SD");

}

TEST_F(DBConnectHighAvailableTest, test_dfs_HA){
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_dfs_HA\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,HASH,[DATE,2]);";
	script += "dummy = table(1:0, `id`sym`date`value, [INT, SYMBOL, DATE, INT]);";
	script += "pt_dfs_HA = db.createPartitionedTable(dummy,`pt_dfs_HA,`date);";
    conn.run(script);

    // std::thread th1(dfs_insert_task1, hostName, port, sites);
    // th1.join();
    string script1 = "tmp=table(rand(1000,1) as id, rand(`A`B`C`D,1) as sym, rand(2012.01.10..2013.01.10,1) as date, rand(0..999,1) as value);\
                        tableInsert(loadTable(\"dfs://test_dfs_HA\",`pt_dfs_HA),tmp);";
    do{
        ConstantSP row=conn.run(script1);
        if (row->getInt() == 1)
            cout<< "Insert successfully"<<endl;
        else{
            cout<< "Insert failed"<<endl;
        }
        cout << "total insert rows are "+conn.run("exec count(*) from loadTable(\"dfs://test_dfs_HA\", `pt_dfs_HA)")->getString()<<endl;
        cout << "Now datanode has changed to "+conn.run("getNodeAlias()")->getString()<<endl;
        Util::sleep(1000); 
    }while (true);

}