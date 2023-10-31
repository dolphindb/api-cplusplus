class IPCinMemoryTableTest : public testing::Test
{
protected:
    // Suite
    static void SetUpTestCase()
    {
        // DBConnection conn;

        bool ret = conn.connect(hostName, port, "admin", "123456");
        if (!ret)
        {
            cout << "Failed to connect to the server" << endl;
        }
        else
        {
            conn.initialize();
            cout << "connect to " + hostName + ":" + std::to_string(port) << endl;
        }
    }
    static void TearDownTestCase()
    {
        conn.run("try{unsubscribeTable(tableName=\"pubTable\", actionName=\"act3\")}catch(ex){};\
                    try{undef(`pubTable,SHARED);}catch(ex){};try{dropIPCInMemoryTable(`pubTable);}catch(ex){};");
        conn.close();
    }

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
        conn.run("try{dropIPCInMemoryTable(`pubTable);}catch(ex){};try{undef(`pubTable,SHARED);}catch(ex){};try{dropIPCInMemoryTable(`shm_test);}catch(ex){};\n\
                try{undef(`t1,SHARED);}catch(ex){};try{undef(`shm_test,SHARED);}catch(ex){};try{unsubscribeTable(tableName=\"pubTable\", actionName=\"act3\")}catch(ex){}\n\
                share streamTable(10000:0,`timestamp`temperature`currenttime, [TIMESTAMP,DOUBLE,NANOTIMESTAMP]) as pubTable\n\
                share createIPCInMemoryTable(1000000, \"pubTable\", `timestamp`temperature`currenttime, [TIMESTAMP, DOUBLE, NANOTIMESTAMP]) as shm_test\n\
                def shm_append(msg) {shm_test.append!(msg)};\
                topic2 = subscribeTable(tableName=\"pubTable\", actionName=\"act3\", offset=0, handler=shm_append, msgAsTable=true)");
    }
    virtual void TearDown()
    {
        conn.run("undef all;");
    }
};

int insertTask(int insertNum, int sleepMS)
{
    DBConnection connNew(false, false);
    connNew.connect(hostName, port, "admin", "123456");
    for (auto i = 0; i < insertNum; i++)
        connNew.run("tableInsert(pubTable,rand(1..100,1),norm(2,0.4,1),take(now(true),1));sleep(" + to_string(sleepMS) + ")");
    connNew.close();
    cout << "insert finished!" << endl;
    return 0;
}

void print(TableSP table)
{
    ConstantSP time_spend = conn.run("cur_tm=now(true);insert_total_time = exec max(currenttime) from pubTable;\n\
                                    tm_spend=cur_tm-insert_total_time;\n\
                                    nanotime(tm_spend)");
    cout << "total get rows: " << table->rows() << endl
         << "total time spend: " << time_spend->getString() << endl
         << endl;
}

void insertRow(TableSP table)
{
    AutoFitTableAppender appender("", "t1", conn);
    appender.append(table);
    return;
}

TEST_F(IPCinMemoryTableTest, test_basic)
{
    thread th1 = thread(insertTask, 10000, 0);
    string tableName = "pubTable";
    IPCInMemoryStreamClient IPCMclient;

    vector<string> colNames = {"timestamp", "temperature", "currenttime"};
    vector<DATA_TYPE> colTypes = {DT_TIMESTAMP, DT_DOUBLE, DT_NANOTIMESTAMP};
    int rowNum = 0, indexCapacity = 10000;
    TableSP outputTable = Util::createTable(colNames, colTypes, rowNum, indexCapacity);

    ThreadSP thread0 = IPCMclient.subscribe(tableName, nullptr, outputTable, false);

    this_thread::sleep_for(chrono::seconds(1));
    th1.join();

    Util::sleep(2000);
    IPCMclient.unsubscribe(tableName);
    conn.upload("outputTable", {outputTable});
    VectorSP res = conn.run("each(eqObj, outputTable.values(), pubTable.values())");
    for (auto i = 0; i < res->size(); i++)
    {
        EXPECT_TRUE(res->get(i)->getBool());
    }
}

TEST_F(IPCinMemoryTableTest, test_subscribe_tableNameNullstr)
{
    string tableName = "pubTable";
    IPCInMemoryStreamClient IPCMclient;

    vector<string> colNames = {"timestamp", "temperature", "currenttime"};
    vector<DATA_TYPE> colTypes = {DT_TIMESTAMP, DT_DOUBLE, DT_NANOTIMESTAMP};
    int rowNum = 0, indexCapacity = 10000;
    TableSP outputTable = Util::createTable(colNames, colTypes, rowNum, indexCapacity);

    EXPECT_ANY_THROW(ThreadSP thread0 = IPCMclient.subscribe("", nullptr, outputTable, false));
}

TEST_F(IPCinMemoryTableTest, test_subscribe_tableNameNotExist)
{
    string tableName = "pubTable";
    IPCInMemoryStreamClient IPCMclient;

    vector<string> colNames = {"timestamp", "temperature", "currenttime"};
    vector<DATA_TYPE> colTypes = {DT_TIMESTAMP, DT_DOUBLE, DT_NANOTIMESTAMP};
    int rowNum = 0, indexCapacity = 10000;
    TableSP outputTable = Util::createTable(colNames, colTypes, rowNum, indexCapacity);

    EXPECT_ANY_THROW(ThreadSP thread0 = IPCMclient.subscribe("errTableName", nullptr, outputTable, false));
}

TEST_F(IPCinMemoryTableTest, test_subscribe_handlerNull)
{
    thread th1 = thread(insertTask, 10000, 0);
    string tableName = "pubTable";
    IPCInMemoryStreamClient IPCMclient;

    vector<string> colNames = {"timestamp", "temperature", "currenttime"};
    vector<DATA_TYPE> colTypes = {DT_TIMESTAMP, DT_DOUBLE, DT_NANOTIMESTAMP};
    int rowNum = 0, indexCapacity = 10000;
    TableSP outputTable = Util::createTable(colNames, colTypes, rowNum, indexCapacity);

    ThreadSP thread0 = IPCMclient.subscribe(tableName, nullptr, outputTable, false);
    this_thread::sleep_for(chrono::seconds(1));
    th1.join();

    Util::sleep(2000);
    IPCMclient.unsubscribe(tableName);
    conn.upload("outputTable", {outputTable});
    for (int i = 0; i < 100; i++)
    {
        EXPECT_EQ(conn.run("pubTable")->getColumn(0)->get(i)->getString(), conn.run("outputTable")->getColumn(0)->get(i)->getString());
        EXPECT_EQ(conn.run("pubTable")->getColumn(1)->get(i)->getString(), conn.run("outputTable")->getColumn(1)->get(i)->getString());
        EXPECT_EQ(conn.run("pubTable")->getColumn(2)->get(i)->getString(), conn.run("outputTable")->getColumn(2)->get(i)->getString());
    }
}

TEST_F(IPCinMemoryTableTest, test_subscribe_outputTableNull)
{
    thread th1 = thread(insertTask, 10000, 0);
    string tableName = "pubTable";
    IPCInMemoryStreamClient IPCMclient;

    vector<string> colNames = {"timestamp", "temperature", "currenttime"};
    vector<DATA_TYPE> colTypes = {DT_TIMESTAMP, DT_DOUBLE, DT_NANOTIMESTAMP};
    int rowNum = 0, indexCapacity = 10000;
    TableSP outputTable = Util::createTable(colNames, colTypes, rowNum, indexCapacity);
    conn.upload("t", {outputTable});
    conn.run("share t as t1");

    ThreadSP thread0 = IPCMclient.subscribe(tableName, insertRow, nullptr, false);
    this_thread::sleep_for(chrono::seconds(10));
    th1.join();

    IPCMclient.unsubscribe(tableName);
    EXPECT_EQ(conn.run("each(eqObj, t1.values(), pubTable.values())")->getString(), "[1,1,1]");
}

TEST_F(IPCinMemoryTableTest, test_subscribe_outputTableErrorColType)
{
    string tableName = "pubTable";
    IPCInMemoryStreamClient IPCMclient;

    vector<string> colNames = {"timestamp", "temperature", "errCol"};
    vector<DATA_TYPE> colTypes = {DT_TIMESTAMP, DT_DOUBLE, DT_STRING};
    int rowNum = 0, indexCapacity = 10000;
    TableSP outputTable = Util::createTable(colNames, colTypes, rowNum, indexCapacity);

    EXPECT_ANY_THROW(ThreadSP thread0 = IPCMclient.subscribe(tableName, nullptr, outputTable, false));
}

TEST_F(IPCinMemoryTableTest, test_subscribe_outputTableErrorColNum)
{
    string tableName = "pubTable";
    IPCInMemoryStreamClient IPCMclient;

    vector<string> colNames = {"timestamp", "temperature", "currenttime", "errCol"};
    vector<DATA_TYPE> colTypes = {DT_TIMESTAMP, DT_DOUBLE, DT_NANOTIMESTAMP, DT_STRING};
    int rowNum = 0, indexCapacity = 10000;
    TableSP outputTable = Util::createTable(colNames, colTypes, rowNum, indexCapacity);

    EXPECT_ANY_THROW(ThreadSP thread0 = IPCMclient.subscribe(tableName, nullptr, outputTable, false));
}

TEST_F(IPCinMemoryTableTest, test_subscribe_overwriteTrue)
{
    thread th1 = thread(insertTask, 10000, 0);
    string tableName = "pubTable";
    IPCInMemoryStreamClient IPCMclient;

    vector<string> colNames = {"timestamp", "temperature", "currenttime"};
    vector<DATA_TYPE> colTypes = {DT_TIMESTAMP, DT_DOUBLE, DT_NANOTIMESTAMP};
    int rowNum = 0, indexCapacity = 10000;
    TableSP outputTable = Util::createTable(colNames, colTypes, rowNum, indexCapacity);

    ThreadSP thread0 = IPCMclient.subscribe(tableName, nullptr, outputTable, true);
    this_thread::sleep_for(chrono::seconds(1));
    th1.join();

    Util::sleep(2000);
    IPCMclient.unsubscribe(tableName);
    conn.upload("outputTable", {outputTable});
    EXPECT_EQ(conn.run("outputTable.size()")->getInt(), 0);
}

TEST_F(IPCinMemoryTableTest, test_unsubscribe_tableNameNullstr)
{
    thread th1 = thread(insertTask, 10000, 0);
    string tableName = "pubTable";
    IPCInMemoryStreamClient IPCMclient;

    vector<string> colNames = {"timestamp", "temperature", "currenttime"};
    vector<DATA_TYPE> colTypes = {DT_TIMESTAMP, DT_DOUBLE, DT_NANOTIMESTAMP};
    int rowNum = 0, indexCapacity = 100;
    TableSP outputTable = Util::createTable(colNames, colTypes, rowNum, indexCapacity);

    ThreadSP thread0 = IPCMclient.subscribe(tableName, nullptr, outputTable, false);
    this_thread::sleep_for(chrono::seconds(1));
    th1.join();

    Util::sleep(2000);
    EXPECT_ANY_THROW(IPCMclient.unsubscribe(""));
    IPCMclient.unsubscribe(tableName);
}

TEST_F(IPCinMemoryTableTest, test_unsubscribe_tableNameNotExist)
{
    thread th1 = thread(insertTask, 10000, 0);
    string tableName = "pubTable";
    IPCInMemoryStreamClient IPCMclient;

    vector<string> colNames = {"timestamp", "temperature", "currenttime"};
    vector<DATA_TYPE> colTypes = {DT_TIMESTAMP, DT_DOUBLE, DT_NANOTIMESTAMP};
    int rowNum = 0, indexCapacity = 100;
    TableSP outputTable = Util::createTable(colNames, colTypes, rowNum, indexCapacity);

    ThreadSP thread0 = IPCMclient.subscribe(tableName, nullptr, outputTable, false);
    this_thread::sleep_for(chrono::seconds(1));
    th1.join();

    Util::sleep(2000);
    EXPECT_ANY_THROW(IPCMclient.unsubscribe("errname"));
    IPCMclient.unsubscribe(tableName);
}

TEST_F(IPCinMemoryTableTest, test_subscribe_twice)
{
    thread th1 = thread(insertTask, 10000, 0);
    string tableName = "pubTable";
    IPCInMemoryStreamClient IPCMclient;

    vector<string> colNames = {"timestamp", "temperature", "currenttime"};
    vector<DATA_TYPE> colTypes = {DT_TIMESTAMP, DT_DOUBLE, DT_NANOTIMESTAMP};
    int rowNum = 0, indexCapacity = 100;
    TableSP outputTable = Util::createTable(colNames, colTypes, rowNum, indexCapacity);

    ThreadSP thread0 = IPCMclient.subscribe(tableName, nullptr, outputTable, false);
    EXPECT_ANY_THROW(ThreadSP thread1 = IPCMclient.subscribe(tableName, nullptr, outputTable, false));
    this_thread::sleep_for(chrono::seconds(1));
    th1.join();

    Util::sleep(2000);
    IPCMclient.unsubscribe(tableName);
}

TEST_F(IPCinMemoryTableTest, test_unsubscribe_twice)
{
    thread th1 = thread(insertTask, 10000, 0);
    string tableName = "pubTable";
    IPCInMemoryStreamClient IPCMclient;

    vector<string> colNames = {"timestamp", "temperature", "currenttime"};
    vector<DATA_TYPE> colTypes = {DT_TIMESTAMP, DT_DOUBLE, DT_NANOTIMESTAMP};
    int rowNum = 0, indexCapacity = 100;
    TableSP outputTable = Util::createTable(colNames, colTypes, rowNum, indexCapacity);

    ThreadSP thread0 = IPCMclient.subscribe(tableName, nullptr, outputTable, false);
    this_thread::sleep_for(chrono::seconds(1));
    th1.join();

    Util::sleep(2000);
    IPCMclient.unsubscribe(tableName);
    IPCMclient.unsubscribe(tableName);
}

TEST_F(IPCinMemoryTableTest, test_subscribe_hugetable)
{
    thread th1 = thread(insertTask, 1000000, 0);
    string tableName = "pubTable";
    IPCInMemoryStreamClient IPCMclient;

    vector<string> colNames = {"timestamp", "temperature", "currenttime"};
    vector<DATA_TYPE> colTypes = {DT_TIMESTAMP, DT_DOUBLE, DT_NANOTIMESTAMP};
    int rowNum = 0, indexCapacity = 1000000;
    TableSP outputTable = Util::createTable(colNames, colTypes, rowNum, indexCapacity);

    ThreadSP thread0 = IPCMclient.subscribe(tableName, nullptr, outputTable, false);

    this_thread::sleep_for(chrono::seconds(1));
    th1.join();

    Util::sleep(2000);
    IPCMclient.unsubscribe(tableName);
    conn.upload("outputTable", {outputTable});
    // for (int i = 0; i < 1000000; i++)
    // {
    //     EXPECT_EQ(conn.run("pubTable")->getColumn(0)->get(i)->getString(), conn.run("outputTable")->getColumn(0)->get(i)->getString());
    //     EXPECT_EQ(conn.run("pubTable")->getColumn(1)->get(i)->getString(), conn.run("outputTable")->getColumn(1)->get(i)->getString());
    //     EXPECT_EQ(conn.run("pubTable")->getColumn(2)->get(i)->getString(), conn.run("outputTable")->getColumn(2)->get(i)->getString());
    // }
    VectorSP res = conn.run("each(eqObj, outputTable.values(), pubTable.values())");
    for (auto i = 0; i < res->size(); i++)
    {
        EXPECT_TRUE(res->get(i)->getBool());
    }
}

// https://dolphindb1.atlassian.net/browse/AC-216
TEST_F(IPCinMemoryTableTest, test_subscribeUnsubscribe_with_gt1048576rows)
{
    string tableName = "pubSharedSnapshotTb";

    thread th_new = thread([=]()
                           {
        DBConnection conn_interval(false,false);
        conn_interval.connect(hostName, port, "admin", "123456");
        cout << "create IPCM table and insert datas\n";
        string s = "//创建跨进程共享内存表\n"
                "try{undef(`shm_SnapshortTable,SHARED)}catch(ex){};go;"
                "try{dropIPCInMemoryTable(`pubSharedSnapshotTb)}catch(ex){};go;"
                "share createIPCInMemoryTable(1000000, `pubSharedSnapshotTb,"
                "`marketType`securityCode`origTime`tradingPhaseCode`preClosePrice`openPrice`highPrice`lowPrice`lastPrice`closePrice`bidPrice1`bidPrice2`bidPrice3`bidPrice4`bidPrice5`bidPrice6`bidPrice7`bidPrice8`bidPrice9`bidPrice10`bidVolume1`bidVolume2`bidVolume3`bidVolume4`bidVolume5`bidVolume6`bidVolume7`bidVolume8`bidVolume9`bidVolume10`offerPrice1`offerPrice2`offerPrice3`offerPrice4`offerPrice5`offerPrice6`offerPrice7`offerPrice8`offerPrice9`offerPrice10`offerVolume1`offerVolume2`offerVolume3`offerVolume4`offerVolume5`offerVolume6`offerVolume7`offerVolume8`offerVolume9`offerVolume10`numTrades`totalVolumeTrade`totalValueTrade`totalBidVolume`totalOfferVolume`weightedAvgBidPrice`weightedAvgOfferPrice`ioPV`yieldToMaturity`highLimited`lowLimited`priceEarningRatio1`priceEarningRatio2`change1`change2`channelNo`mdStreamID`instrumentStatus`preCloseIOPV`altWeightedAvgBidPrice`altWeightedAvgOfferPrice`etfBuyNumber`etfBuyAmount`etfBuyMoney`etfSellNumber`etfSellAmount`etfSellMoney`totalWarrantExecVolume`warLowerPrice`warUpperPrice`withdrawBuyNumber`withdrawBuyAmount`withdrawBuyMoney`withdrawSellNumber`withdrawSellAmount`withdrawSellMoney`totalBidNumber`totalOfferNumber`bidTradeMaxDuration`offerTradeMaxDuration`numBidOrders`bnumOfferOrders`lastTradeTime`varietyCategory`receivedTime`dailyIndex,"
                "[INT, SYMBOL, TIMESTAMP, STRING,"
                "LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG,"
                "LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG,"
                "LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG,"
                "LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG,"
                "INT, STRING, STRING,"
                "LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG,"
                "INT, INT, INT, INT, LONG, CHAR, NANOTIMESTAMP, INT]) as shm_SnapshortTable;go;"
                "for(i in 1:1100000){tableInsert(shm_SnapshortTable, "
                "rand(1 2 3 4, 1), rand(`apl`ffffffffffffffffffffffffffffffffgoog`ms`agggggggggggggma, 1), now(), rand(`s1asdgzxcaw`s2gggggggg`s333333333`s4aaaaasdzx,1), "
                "rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),"
                "rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),"
                "rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),"
                "rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),"
                "rand(100000, 1),rand(`a1zzzzzzz`a2xxxxxxxx`a3qqqqqqqqqqqqqq`a4ghhhhhhhhhhhhhhhhh, 1),rand(`b1nnnnnnnnnnnnnnnn`b2bbbbbbbbbbbbbbbb`b3hhhhhhhhhhhhhhhh`b4uuuuuuuuuuuuuuuuuuuuuuuuuuu, 1),"
                "rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),"
                "rand(100000, 1),rand(100000, 1),rand(100000, 1),rand(100000, 1),rand(10000000l, 1),rand(127c, 1), now(true),rand(100000, 1));if(i % 100000 == 0){print(string(i)+' rows inserted finished!');}}";

        conn_interval.run(s);
        cout<< "insert all datas finished!\n";
        conn_interval.close();
        return; });

    vector<string> colNames = {"marketType", "securityCode", "origTime", "tradingPhaseCode", "preClosePrice", "openPrice", "highPrice", "lowPrice", "lastPrice", "closePrice", "bidPrice1", "bidPrice2", "bidPrice3", "bidPrice4", "bidPrice5", "bidPrice6", "bidPrice7", "bidPrice8", "bidPrice9", "bidPrice10", "bidVolume1", "bidVolume2", "bidVolume3", "bidVolume4", "bidVolume5", "bidVolume6", "bidVolume7", "bidVolume8", "bidVolume9", "bidVolume10", "offerPrice1", "offerPrice2", "offerPrice3", "offerPrice4", "offerPrice5", "offerPrice6", "offerPrice7", "offerPrice8", "offerPrice9", "offerPrice10", "offerVolume1", "offerVolume2", "offerVolume3", "offerVolume4", "offerVolume5", "offerVolume6", "offerVolume7", "offerVolume8", "offerVolume9", "offerVolume10", "numTrades", "totalVolumeTrade", "totalValueTrade", "totalBidVolume", "totalOfferVolume", "weightedAvgBidPrice", "weightedAvgOfferPrice", "ioPV", "yieldToMaturity", "highLimited", "lowLimited", "priceEarningRatio1", "priceEarningRatio2", "change1", "change2", "channelNo", "mdStreamID", "instrumentStatus", "preCloseIOPV", "altWeightedAvgBidPrice", "altWeightedAvgOfferPrice", "etfBuyNumber", "etfBuyAmount", "etfBuyMoney", "etfSellNumber", "etfSellAmount", "etfSellMoney", "totalWarrantExecVolume", "warLowerPrice", "warUpperPrice", "withdrawBuyNumber", "withdrawBuyAmount", "withdrawBuyMoney", "withdrawSellNumber", "withdrawSellAmount", "withdrawSellMoney", "totalBidNumber", "totalOfferNumber", "bidTradeMaxDuration", "offerTradeMaxDuration", "numBidOrders", "bnumOfferOrders", "lastTradeTime", "varietyCategory", "receivedTime", "dailyIndex"};
    vector<DATA_TYPE> colTypes = {DT_INT, DT_SYMBOL, DT_TIMESTAMP, DT_STRING, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_INT, DT_STRING, DT_STRING, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_INT, DT_INT, DT_INT, DT_INT, DT_LONG, DT_CHAR, DT_NANOTIMESTAMP, DT_INT};
    int rowNum = 0, indexCapacity = 10000;
    TableSP outputTable = Util::createTable(colNames, colTypes, rowNum, indexCapacity); // 创建一个和共享内存表结构相同的表
    cout << "outputTable created successfully\n";
    auto handler = [=](TableSP tb)
    {
        // auto row = tb->rows();
        // cout << "[handler]: now get rows: " << row << '\n';
        // cout<<"handler table: \n"<<tb->getString()<<'\n';
    };
    int preRow = 0;

    thread th2 = thread([&]()
                        {
        Util::sleep(1000);
        for(auto i=0;i<50;i++){
            IPCInMemoryStreamClient IPCMclient;
            
            ThreadSP thread0 = IPCMclient.subscribe(tableName, handler, outputTable, false);
            Util::sleep(500);

            IPCMclient.unsubscribe(tableName);
            int curRow = outputTable->rows();
            EXPECT_TRUE(curRow > preRow);
            preRow = curRow;
            Util::sleep(2000);
        } });

    th_new.join();
    th2.join();
}
// https://dolphindb1.atlassian.net/browse/AC-216
TEST_F(IPCinMemoryTableTest, test_subscribeUnsubscribe_overwriteTrue_with_gt1048576rows)
{
    string tableName = "pubSharedSnapshotTb";

    thread th_new = thread([=]()
                           {
        DBConnection conn_interval(false,false);
        conn_interval.connect(hostName, port, "admin", "123456");
        cout << "create IPCM table and insert datas\n";
        string s = "//创建跨进程共享内存表\n"
                "try{undef(`shm_SnapshortTable,SHARED)}catch(ex){};go;"
                "try{dropIPCInMemoryTable(`pubSharedSnapshotTb)}catch(ex){};go;"
                "share createIPCInMemoryTable(1000000, `pubSharedSnapshotTb,"
                "`marketType`securityCode`origTime`tradingPhaseCode`preClosePrice`openPrice`highPrice`lowPrice`lastPrice`closePrice`bidPrice1`bidPrice2`bidPrice3`bidPrice4`bidPrice5`bidPrice6`bidPrice7`bidPrice8`bidPrice9`bidPrice10`bidVolume1`bidVolume2`bidVolume3`bidVolume4`bidVolume5`bidVolume6`bidVolume7`bidVolume8`bidVolume9`bidVolume10`offerPrice1`offerPrice2`offerPrice3`offerPrice4`offerPrice5`offerPrice6`offerPrice7`offerPrice8`offerPrice9`offerPrice10`offerVolume1`offerVolume2`offerVolume3`offerVolume4`offerVolume5`offerVolume6`offerVolume7`offerVolume8`offerVolume9`offerVolume10`numTrades`totalVolumeTrade`totalValueTrade`totalBidVolume`totalOfferVolume`weightedAvgBidPrice`weightedAvgOfferPrice`ioPV`yieldToMaturity`highLimited`lowLimited`priceEarningRatio1`priceEarningRatio2`change1`change2`channelNo`mdStreamID`instrumentStatus`preCloseIOPV`altWeightedAvgBidPrice`altWeightedAvgOfferPrice`etfBuyNumber`etfBuyAmount`etfBuyMoney`etfSellNumber`etfSellAmount`etfSellMoney`totalWarrantExecVolume`warLowerPrice`warUpperPrice`withdrawBuyNumber`withdrawBuyAmount`withdrawBuyMoney`withdrawSellNumber`withdrawSellAmount`withdrawSellMoney`totalBidNumber`totalOfferNumber`bidTradeMaxDuration`offerTradeMaxDuration`numBidOrders`bnumOfferOrders`lastTradeTime`varietyCategory`receivedTime`dailyIndex,"
                "[INT, SYMBOL, TIMESTAMP, STRING,"
                "LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG,"
                "LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG,"
                "LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG,"
                "LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG,"
                "INT, STRING, STRING,"
                "LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG,"
                "INT, INT, INT, INT, LONG, CHAR, NANOTIMESTAMP, INT]) as shm_SnapshortTable;go;"
                "for(i in 1:1100000){tableInsert(shm_SnapshortTable, "
                "rand(1 2 3 4, 1), rand(`apl`ffffffffffffffffffffffffffffffffgoog`ms`agggggggggggggma, 1), now(), rand(`s1asdgzxcaw`s2gggggggg`s333333333`s4aaaaasdzx,1), "
                "rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),"
                "rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),"
                "rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),"
                "rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),"
                "rand(100000, 1),rand(`a1zzzzzzz`a2xxxxxxxx`a3qqqqqqqqqqqqqq`a4ghhhhhhhhhhhhhhhhh, 1),rand(`b1nnnnnnnnnnnnnnnn`b2bbbbbbbbbbbbbbbb`b3hhhhhhhhhhhhhhhh`b4uuuuuuuuuuuuuuuuuuuuuuuuuuu, 1),"
                "rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),rand(1000000l,1),"
                "rand(100000, 1),rand(100000, 1),rand(100000, 1),rand(100000, 1),rand(10000000l, 1),rand(127c, 1), now(true),rand(100000, 1));if(i % 100000 == 0){print(string(i)+' rows inserted finished!');}}";

        conn_interval.run(s);
        cout<< "insert all datas finished!\n";
        conn_interval.close();
        return; });

    vector<string> colNames = {"marketType", "securityCode", "origTime", "tradingPhaseCode", "preClosePrice", "openPrice", "highPrice", "lowPrice", "lastPrice", "closePrice", "bidPrice1", "bidPrice2", "bidPrice3", "bidPrice4", "bidPrice5", "bidPrice6", "bidPrice7", "bidPrice8", "bidPrice9", "bidPrice10", "bidVolume1", "bidVolume2", "bidVolume3", "bidVolume4", "bidVolume5", "bidVolume6", "bidVolume7", "bidVolume8", "bidVolume9", "bidVolume10", "offerPrice1", "offerPrice2", "offerPrice3", "offerPrice4", "offerPrice5", "offerPrice6", "offerPrice7", "offerPrice8", "offerPrice9", "offerPrice10", "offerVolume1", "offerVolume2", "offerVolume3", "offerVolume4", "offerVolume5", "offerVolume6", "offerVolume7", "offerVolume8", "offerVolume9", "offerVolume10", "numTrades", "totalVolumeTrade", "totalValueTrade", "totalBidVolume", "totalOfferVolume", "weightedAvgBidPrice", "weightedAvgOfferPrice", "ioPV", "yieldToMaturity", "highLimited", "lowLimited", "priceEarningRatio1", "priceEarningRatio2", "change1", "change2", "channelNo", "mdStreamID", "instrumentStatus", "preCloseIOPV", "altWeightedAvgBidPrice", "altWeightedAvgOfferPrice", "etfBuyNumber", "etfBuyAmount", "etfBuyMoney", "etfSellNumber", "etfSellAmount", "etfSellMoney", "totalWarrantExecVolume", "warLowerPrice", "warUpperPrice", "withdrawBuyNumber", "withdrawBuyAmount", "withdrawBuyMoney", "withdrawSellNumber", "withdrawSellAmount", "withdrawSellMoney", "totalBidNumber", "totalOfferNumber", "bidTradeMaxDuration", "offerTradeMaxDuration", "numBidOrders", "bnumOfferOrders", "lastTradeTime", "varietyCategory", "receivedTime", "dailyIndex"};
    vector<DATA_TYPE> colTypes = {DT_INT, DT_SYMBOL, DT_TIMESTAMP, DT_STRING, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_INT, DT_STRING, DT_STRING, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_LONG, DT_INT, DT_INT, DT_INT, DT_INT, DT_LONG, DT_CHAR, DT_NANOTIMESTAMP, DT_INT};
    int rowNum = 0, indexCapacity = 10000;
    TableSP outputTable = Util::createTable(colNames, colTypes, rowNum, indexCapacity); // 创建一个和共享内存表结构相同的表
    cout << "outputTable created successfully\n";
    auto handler = [=](TableSP tb)
    {
        // auto row = tb->rows();
        // cout << "[handler]: now get rows: " << row << '\n';
    };

    thread th2 = thread([&]()
                        {
        Util::sleep(1000);
        for(auto i=0;i<50;i++){
            IPCInMemoryStreamClient IPCMclient;
            
            ThreadSP thread0 = IPCMclient.subscribe(tableName, handler, outputTable, true);
            Util::sleep(500);

            IPCMclient.unsubscribe(tableName);
            EXPECT_TRUE(outputTable->rows() == 1 | outputTable->rows() == 0 );
            Util::sleep(2000);
        } });

    th_new.join();
    th2.join();
}