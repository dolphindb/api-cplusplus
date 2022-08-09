class IPCinMemoryTableTest:public testing::Test
{
protected:
    //Suite
    static void SetUpTestCase() {
        //DBConnection conn;

        bool ret = conn.connect(hostName, port, "admin", "123456");
        if (!ret) {
            cout << "Failed to connect to the server" << endl;
        }
        else {
            conn.initialize();
            cout << "connect to " + hostName + ":" + std::to_string(port)<< endl;
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
            conn.run("try{dropIPCInMemoryTable(`pubTable);}catch(ex){};;try{undef(`pubTable,SHARED);}catch(ex){};try{dropIPCInMemoryTable(`shm_test);}catch(ex){};\n\
                    try{undef(`t1,SHARED);}catch(ex){};try{undef(`shm_test,SHARED);}catch(ex){};try{unsubscribeTable(tableName=\"pubTable\", actionName=\"act3\")}catch(ex){}\n\
                    share streamTable(10000:0,`timestamp`temperature`currenttime, [TIMESTAMP,DOUBLE,NANOTIMESTAMP]) as pubTable\n\
                    share createIPCInMemoryTable(1000000, \"pubTable\", `timestamp`temperature`currenttime, [TIMESTAMP, DOUBLE, NANOTIMESTAMP]) as shm_test\n\
                    def shm_append(msg) {shm_test.append!(msg)};\
                    topic2 = subscribeTable(tableName=\"pubTable\", actionName=\"act3\", offset=0, handler=shm_append, msgAsTable=true)");
		}
    }
    virtual void TearDown()
    {
        pass;
    }
};

int insertTask(int insertNum,int sleepMS){
    DBConnection connNew(false,false);
    connNew.connect(hostName,port,"admin","123456");
    connNew.run("for (i in 1.."+to_string(insertNum)+"){tableInsert(pubTable,rand(1..100,1),norm(2,0.4,1),take(now(true),1));sleep("+to_string(sleepMS)+")}");
    connNew.close();
    cout<<"insert finished!"<<endl;
    return 0;
}

void print(TableSP table) {
    ConstantSP time_spend=conn.run("cur_tm=now(true);insert_total_time = exec max(currenttime) from pubTable;\n\
                                    tm_spend=cur_tm-insert_total_time;\n\
                                    nanotime(tm_spend)");
    cout<<"total get rows: "<<table->rows()<<endl<<"total time spend: "<<time_spend->getString()<<endl<<endl;
}

void insertRow(TableSP table) {
    AutoFitTableAppender appender("", "t1", conn);
    appender.append(table);
    return;
}

TEST_F(IPCinMemoryTableTest,test_basic){
    thread th1= thread(insertTask,10000,0);
    string tableName = "pubTable";
    IPCInMemoryStreamClient memTable;

    vector<string> colNames = {"timestamp", "temperature","currenttime"};
    vector<DATA_TYPE> colTypes = {DT_TIMESTAMP, DT_DOUBLE,DT_NANOTIMESTAMP};
    int rowNum = 0, indexCapacity=10000;
    TableSP outputTable = Util::createTable(colNames, colTypes, rowNum, indexCapacity);

    ThreadSP thread0 = memTable.subscribe(tableName, nullptr, outputTable, false);

    this_thread::sleep_for(chrono::seconds(1));
    th1.join();

    Util::sleep(2000);
    memTable.unsubscribe(tableName);
    conn.upload("outputTable",{outputTable});
    for(int i=0;i<100;i++){
        EXPECT_EQ(conn.run("pubTable")->getColumn(0)->get(i)->getString(),conn.run("outputTable")->getColumn(0)->get(i)->getString());
        EXPECT_EQ(conn.run("pubTable")->getColumn(1)->get(i)->getString(),conn.run("outputTable")->getColumn(1)->get(i)->getString());
        EXPECT_EQ(conn.run("pubTable")->getColumn(2)->get(i)->getString(),conn.run("outputTable")->getColumn(2)->get(i)->getString());
    }

}

TEST_F(IPCinMemoryTableTest,test_subscribe_tableNameNullstr){
    string tableName = "pubTable";
    IPCInMemoryStreamClient memTable;

    vector<string> colNames = {"timestamp", "temperature","currenttime"};
    vector<DATA_TYPE> colTypes = {DT_TIMESTAMP, DT_DOUBLE,DT_NANOTIMESTAMP};
    int rowNum = 0, indexCapacity=10000;
    TableSP outputTable = Util::createTable(colNames, colTypes, rowNum, indexCapacity);

    EXPECT_ANY_THROW(ThreadSP thread0 = memTable.subscribe("", nullptr, outputTable, false));

}

TEST_F(IPCinMemoryTableTest,test_subscribe_tableNameNotExist){
    string tableName = "pubTable";
    IPCInMemoryStreamClient memTable;

    vector<string> colNames = {"timestamp", "temperature","currenttime"};
    vector<DATA_TYPE> colTypes = {DT_TIMESTAMP, DT_DOUBLE,DT_NANOTIMESTAMP};
    int rowNum = 0, indexCapacity=10000;
    TableSP outputTable = Util::createTable(colNames, colTypes, rowNum, indexCapacity);

    EXPECT_ANY_THROW(ThreadSP thread0 = memTable.subscribe("errTableName", nullptr, outputTable, false));

}


TEST_F(IPCinMemoryTableTest,test_subscribe_handlerNull){
    thread th1= thread(insertTask,10000,0);
    string tableName = "pubTable";
    IPCInMemoryStreamClient memTable;

    vector<string> colNames = {"timestamp", "temperature","currenttime"};
    vector<DATA_TYPE> colTypes = {DT_TIMESTAMP, DT_DOUBLE,DT_NANOTIMESTAMP};
    int rowNum = 0, indexCapacity=10000;
    TableSP outputTable = Util::createTable(colNames, colTypes, rowNum, indexCapacity);

    ThreadSP thread0 = memTable.subscribe(tableName, nullptr, outputTable, false);
    this_thread::sleep_for(chrono::seconds(1));
    th1.join();

    Util::sleep(2000);
    memTable.unsubscribe(tableName);
    conn.upload("outputTable",{outputTable});
    for(int i=0;i<100;i++){
        EXPECT_EQ(conn.run("pubTable")->getColumn(0)->get(i)->getString(),conn.run("outputTable")->getColumn(0)->get(i)->getString());
        EXPECT_EQ(conn.run("pubTable")->getColumn(1)->get(i)->getString(),conn.run("outputTable")->getColumn(1)->get(i)->getString());
        EXPECT_EQ(conn.run("pubTable")->getColumn(2)->get(i)->getString(),conn.run("outputTable")->getColumn(2)->get(i)->getString());
    }
}

TEST_F(IPCinMemoryTableTest,test_subscribe_outputTableNull){
    thread th1= thread(insertTask,10000,0);
    string tableName = "pubTable";
    IPCInMemoryStreamClient memTable;

    vector<string> colNames = {"timestamp", "temperature","currenttime"};
    vector<DATA_TYPE> colTypes = {DT_TIMESTAMP, DT_DOUBLE,DT_NANOTIMESTAMP};
    int rowNum = 0, indexCapacity=10000;
    TableSP outputTable = Util::createTable(colNames, colTypes, rowNum, indexCapacity);
    conn.upload("t",{outputTable});
    conn.run("share t as t1");

    ThreadSP thread0 = memTable.subscribe(tableName, insertRow, nullptr, false);
    this_thread::sleep_for(chrono::seconds(10));
    th1.join();

    memTable.unsubscribe(tableName);
    EXPECT_EQ(conn.run("each(eqObj, t1.values(), pubTable.values())")->getString(),"[1,1,1]");
}

TEST_F(IPCinMemoryTableTest,test_subscribe_outputTableErrorColType){
    string tableName = "pubTable";
    IPCInMemoryStreamClient memTable;

    vector<string> colNames = {"timestamp", "temperature","errCol"};
    vector<DATA_TYPE> colTypes = {DT_TIMESTAMP, DT_DOUBLE,DT_STRING};
    int rowNum = 0, indexCapacity=10000;
    TableSP outputTable = Util::createTable(colNames, colTypes, rowNum, indexCapacity);

    EXPECT_ANY_THROW(ThreadSP thread0 = memTable.subscribe(tableName, nullptr, outputTable, false));
}

TEST_F(IPCinMemoryTableTest,test_subscribe_outputTableErrorColNum){
    string tableName = "pubTable";
    IPCInMemoryStreamClient memTable;

    vector<string> colNames = {"timestamp", "temperature","currenttime","errCol"};
    vector<DATA_TYPE> colTypes = {DT_TIMESTAMP, DT_DOUBLE,DT_NANOTIMESTAMP,DT_STRING};
    int rowNum = 0, indexCapacity=10000;
    TableSP outputTable = Util::createTable(colNames, colTypes, rowNum, indexCapacity);

    EXPECT_ANY_THROW(ThreadSP thread0 = memTable.subscribe(tableName, nullptr, outputTable, false));
}

TEST_F(IPCinMemoryTableTest,test_subscribe_overwriteTrue){
    thread th1= thread(insertTask,10000,0);
    string tableName = "pubTable";
    IPCInMemoryStreamClient memTable;

    vector<string> colNames = {"timestamp", "temperature","currenttime"};
    vector<DATA_TYPE> colTypes = {DT_TIMESTAMP, DT_DOUBLE,DT_NANOTIMESTAMP};
    int rowNum = 0, indexCapacity=10000;
    TableSP outputTable = Util::createTable(colNames, colTypes, rowNum, indexCapacity);

    ThreadSP thread0 = memTable.subscribe(tableName, nullptr, outputTable, true);
    this_thread::sleep_for(chrono::seconds(1));
    th1.join();

    Util::sleep(2000);
    memTable.unsubscribe(tableName);
    conn.upload("outputTable",{outputTable});
    EXPECT_EQ(conn.run("outputTable.size()")->getInt(),0);
}

TEST_F(IPCinMemoryTableTest,test_unsubscribe_tableNameNullstr){
    thread th1= thread(insertTask,10000,0);
    string tableName = "pubTable";
    IPCInMemoryStreamClient memTable;

    vector<string> colNames = {"timestamp", "temperature","currenttime"};
    vector<DATA_TYPE> colTypes = {DT_TIMESTAMP, DT_DOUBLE,DT_NANOTIMESTAMP};
    int rowNum = 0, indexCapacity=100;
    TableSP outputTable = Util::createTable(colNames, colTypes, rowNum, indexCapacity);

    ThreadSP thread0 = memTable.subscribe(tableName, nullptr, outputTable, false);
    this_thread::sleep_for(chrono::seconds(1));
    th1.join();

    Util::sleep(2000);
    EXPECT_ANY_THROW(memTable.unsubscribe(""));
    memTable.unsubscribe(tableName);

}

TEST_F(IPCinMemoryTableTest,test_unsubscribe_tableNameNotExist){
    thread th1= thread(insertTask,10000,0);
    string tableName = "pubTable";
    IPCInMemoryStreamClient memTable;

    vector<string> colNames = {"timestamp", "temperature","currenttime"};
    vector<DATA_TYPE> colTypes = {DT_TIMESTAMP, DT_DOUBLE,DT_NANOTIMESTAMP};
    int rowNum = 0, indexCapacity=100;
    TableSP outputTable = Util::createTable(colNames, colTypes, rowNum, indexCapacity);

    ThreadSP thread0 = memTable.subscribe(tableName, nullptr, outputTable, false);
    this_thread::sleep_for(chrono::seconds(1));
    th1.join();

    Util::sleep(2000);
    EXPECT_ANY_THROW(memTable.unsubscribe("errname"));
    memTable.unsubscribe(tableName);

}

TEST_F(IPCinMemoryTableTest,test_subscribe_twice){
    thread th1= thread(insertTask,10000,0);
    string tableName = "pubTable";
    IPCInMemoryStreamClient memTable;

    vector<string> colNames = {"timestamp", "temperature","currenttime"};
    vector<DATA_TYPE> colTypes = {DT_TIMESTAMP, DT_DOUBLE,DT_NANOTIMESTAMP};
    int rowNum = 0, indexCapacity=100;
    TableSP outputTable = Util::createTable(colNames, colTypes, rowNum, indexCapacity);

    ThreadSP thread0 = memTable.subscribe(tableName, nullptr, outputTable, false);
    EXPECT_ANY_THROW(ThreadSP thread1 = memTable.subscribe(tableName, nullptr, outputTable, false));
    this_thread::sleep_for(chrono::seconds(1));
    th1.join();

    Util::sleep(2000);
    memTable.unsubscribe(tableName);

}

TEST_F(IPCinMemoryTableTest,test_unsubscribe_twice){
    thread th1= thread(insertTask,10000,0);
    string tableName = "pubTable";
    IPCInMemoryStreamClient memTable;

    vector<string> colNames = {"timestamp", "temperature","currenttime"};
    vector<DATA_TYPE> colTypes = {DT_TIMESTAMP, DT_DOUBLE,DT_NANOTIMESTAMP};
    int rowNum = 0, indexCapacity=100;
    TableSP outputTable = Util::createTable(colNames, colTypes, rowNum, indexCapacity);

    ThreadSP thread0 = memTable.subscribe(tableName, nullptr, outputTable, false);
    this_thread::sleep_for(chrono::seconds(1));
    th1.join();

    Util::sleep(2000);
    memTable.unsubscribe(tableName);
    memTable.unsubscribe(tableName);

}