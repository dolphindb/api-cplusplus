class streamingDeserilizerTester:public testing::Test
{
protected:
    //Suite
    static void SetUpTestCase() {
        //DBConnection conn;
		conn.initialize();
        bool ret = conn.connect(hostName, port, "admin", "123456");
        if (!ret) {
            cout << "Failed to connect to the server" << endl;
        }
        else {
            cout << "connect to " + hostName + ":" + std::to_string(port)<<endl;
        }
    }
    static void TearDownTestCase(){
        usedPorts.clear();
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
            
            string del_streamtable="login(\"admin\",\"123456\");\
                                    try{ dropStreamTable(`SDoutTables);dropStreamTable(`st2) }\
                                    catch(ex){}";
            conn.run(del_streamtable);
		}
    }
    virtual void TearDown()
    {
        pass;
    }
};

StreamDeserializerSP createStreamDeserializer() {
	string script = "login(\"admin\",\"123456\")\n\
            st2 = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n\
            enableTableShareAndPersistence(table=st2, tableName=`SDoutTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\n\
            go\n\
            setStreamTableFilterColumn(SDoutTables, `sym)";
	conn.run(script);

	string replayScript = "n = 1000;table1_SDPT = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);\
            table2_SDPT = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);\
            tableInsert(table1_SDPT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));\
            tableInsert(table2_SDPT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));\
            d = dict(['msg1','msg2'], [table1_SDPT, table2_SDPT]);\
            replay(inputTables=d, outputTables=`SDoutTables, dateColumn=`timestampv, timeColumn=`timestampv)";
	conn.run(replayScript);

	DictionarySP t1s = conn.run("schema(table1_SDPT)");
	DictionarySP t2s = conn.run("schema(table2_SDPT)");
	unordered_map<string, DictionarySP> sym2schema;
	sym2schema["msg1"] = t1s;
	sym2schema["msg2"] = t2s;
    StreamDeserializerSP sdsp = new StreamDeserializer(sym2schema);
    return sdsp;
}

TEST_F(streamingDeserilizerTester,test_Threadclient_onehandler_subscribeWithstreamDeserilizer){
    int msg1_total=0;
    int msg2_total=0;
    int index1=0;
    int index2=0;
    srand(time(0));

    int listenport = rand() % 13000 + 2000;
    while(find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };
	Signal notify;
    Mutex mutex;
    StreamDeserializerSP sdsp = createStreamDeserializer();

    TableSP table1_SDPT = conn.run("table1_SDPT");
    TableSP table2_SDPT = conn.run("table2_SDPT");

    auto onehandler = [&](Message msg) {
        const string &symbol = msg.getSymbol();
        LockGuard<Mutex> lock(&mutex);
        if (symbol == "msg1"){ 
            msg1_total+=1;
            EXPECT_EQ(msg->get(0)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("datetimev"))->getString());
            EXPECT_EQ(msg->get(1)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("timestampv"))->getString());
            EXPECT_EQ(msg->get(2)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("sym"))->getString());
            EXPECT_EQ(msg->get(3)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("price1"))->getString());
            EXPECT_EQ(msg->get(4)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("price2"))->getString());
            index1++;
        }
        else if (symbol == "msg2"){ 
            msg2_total+=1;
            EXPECT_EQ(msg->get(0)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("datetimev"))->getString());
            EXPECT_EQ(msg->get(1)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("timestampv"))->getString());
            EXPECT_EQ(msg->get(2)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("sym"))->getString());
            EXPECT_EQ(msg->get(3)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("price1"))->getString());
            index2++;
        }
        if (msg1_total + msg2_total == 2000) {
            notify.set();
        }
    };

    ThreadedClient threadedClient(listenport);
    auto thread1 = threadedClient.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true,nullptr, false, false, "admin", "123456",sdsp);
    thread1->setAffinity(0);
    notify.wait();

    threadedClient.unsubscribe(hostName, port, "SDoutTables", "test_SD");

    EXPECT_EQ(msg1_total, 1000);
    EXPECT_EQ(msg2_total, 1000);
    usedPorts.push_back(listenport);
}

TEST_F(streamingDeserilizerTester,test_Threadclient_batchhandler_subscribeWithstreamDeserilizer){
    int msg1_total=0;
    int msg2_total=0;
    int index1=0;
    int index2=0;
    srand(time(0));

    int listenport = rand() % 13000 + 2000;
    while (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };

	Signal notify;
    Mutex mutex;
    StreamDeserializerSP sdsp = createStreamDeserializer();

    TableSP table1_SDPT = conn.run("table1_SDPT");
    TableSP table2_SDPT = conn.run("table2_SDPT");

    auto batchhandler = [&](vector<Message> msgs) {
        LockGuard<Mutex> lock(&mutex);
        for (auto &msg : msgs) {
            const string &symbol = msg.getSymbol();
            if (symbol == "msg1"){ 
                msg1_total+=1;
                // cout<<"index1= "<<index1<<endl;
                // cout<<msg->get(0)->getString()<<",";
                // cout<<msg->get(1)->getString()<<",";
                // cout<<msg->get(2)->getString()<<",";
                // cout<<msg->get(3)->getString()<<",";
                // cout<<msg->get(4)->getString()<<endl;
                // cout<<table1_SDPT->getRow(index1)->get(Util::createString("datetimev"))->getString()<<",";
                // cout<<table1_SDPT->getRow(index1)->get(Util::createString("timestampv"))->getString()<<",";
                // cout<<table1_SDPT->getRow(index1)->get(Util::createString("sym"))->getString()<<",";
                // cout<<table1_SDPT->getRow(index1)->get(Util::createString("price1"))->getString()<<",";
                // cout<<table1_SDPT->getRow(index1)->get(Util::createString("price2"))->getString()<<endl<<endl;

                // EXPECT_EQ(msg->get(0)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("datetimev"))->getString());
                // EXPECT_EQ(msg->get(1)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("timestampv"))->getString());
                // EXPECT_EQ(msg->get(2)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("sym"))->getString());
                // EXPECT_EQ(msg->get(3)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("price1"))->getString());
                // EXPECT_EQ(msg->get(4)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("price2"))->getString());
                index1++;
            }
            else if (symbol == "msg2"){ 
                msg2_total+=1;
                // cout<<"index2= "<<index2<<endl;
                // cout<<msg->get(0)->getString()<<",";
                // cout<<msg->get(1)->getString()<<",";
                // cout<<msg->get(2)->getString()<<",";
                // cout<<msg->get(3)->getString()<<endl;
                // cout<<table2_SDPT->getRow(index2)->get(Util::createString("datetimev"))->getString()<<",";
                // cout<<table2_SDPT->getRow(index2)->get(Util::createString("timestampv"))->getString()<<",";
                // cout<<table2_SDPT->getRow(index2)->get(Util::createString("sym"))->getString()<<",";
                // cout<<table2_SDPT->getRow(index2)->get(Util::createString("price1"))->getString()<<endl<<endl;

                // EXPECT_EQ(msg->get(0)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("datetimev"))->getString());
                // EXPECT_EQ(msg->get(1)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("timestampv"))->getString());
                // EXPECT_EQ(msg->get(2)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("sym"))->getString());
                // EXPECT_EQ(msg->get(3)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("price1"))->getString());
                index2++;
            }
            if (msg1_total + msg2_total == 2000) {
                notify.set();
            }
        }
    };

    ThreadedClient threadedClient(listenport);
    auto thread2 = threadedClient.subscribe(hostName, port, batchhandler, "SDoutTables", "mutiSchemaBatch",0,true, nullptr, true, 1,1.0,false,"admin", "123456",sdsp);
    thread2->setAffinity(0);
    notify.wait();

    threadedClient.unsubscribe(hostName, port, "SDoutTables", "mutiSchemaBatch");

    EXPECT_EQ(msg1_total, 1000);
    EXPECT_EQ(msg2_total, 1000);
    usedPorts.push_back(listenport);
}

TEST_F(streamingDeserilizerTester,test_Pollingclient_subscribeWithstreamDeserilizer){
    int msg1_total=0;
    int msg2_total=0;
    int index1=0;
    int index2=0;
    srand(time(0));

    int listenport = rand() % 13000 + 2000;
    while (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };

    StreamDeserializerSP sdsp = createStreamDeserializer();

    TableSP table1_SDPT = conn.run("table1_SDPT");
    TableSP table2_SDPT = conn.run("table2_SDPT");

    PollingClient client(listenport);
    auto queue = client.subscribe(hostName, port,"SDoutTables", "actionTest", 0, true,nullptr, false,false, "admin","123456",sdsp);

    vector<Message> msgs;
    while(queue->pop(msgs,1000)) {
        for (auto &msg : msgs) {
            const string &symbol = msg.getSymbol();
            if (symbol == "msg1"){ 
                msg1_total+=1;
                // cout<<"index1= "<<index1<<endl;
                // cout<<msg->get(0)->getString()<<",";
                // cout<<msg->get(1)->getString()<<",";
                // cout<<msg->get(2)->getString()<<",";
                // cout<<msg->get(3)->getString()<<",";
                // cout<<msg->get(4)->getString()<<endl;
                // cout<<table1_SDPT->getRow(index1)->get(Util::createString("datetimev"))->getString()<<",";
                // cout<<table1_SDPT->getRow(index1)->get(Util::createString("timestampv"))->getString()<<",";
                // cout<<table1_SDPT->getRow(index1)->get(Util::createString("sym"))->getString()<<",";
                // cout<<table1_SDPT->getRow(index1)->get(Util::createString("price1"))->getString()<<",";
                // cout<<table1_SDPT->getRow(index1)->get(Util::createString("price2"))->getString()<<endl<<endl;

                EXPECT_EQ(msg->get(0)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("datetimev"))->getString());
                EXPECT_EQ(msg->get(1)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("timestampv"))->getString());
                EXPECT_EQ(msg->get(2)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("sym"))->getString());
                EXPECT_EQ(msg->get(3)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("price1"))->getString());
                EXPECT_EQ(msg->get(4)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("price2"))->getString());
                index1++;
            }
            else if (symbol == "msg2"){ 
                msg2_total+=1;
                // cout<<"index2= "<<index2<<endl;
                // cout<<msg->get(0)->getString()<<",";
                // cout<<msg->get(1)->getString()<<",";
                // cout<<msg->get(2)->getString()<<",";
                // cout<<msg->get(3)->getString()<<endl;
                // cout<<table2_SDPT->getRow(index2)->get(Util::createString("datetimev"))->getString()<<",";
                // cout<<table2_SDPT->getRow(index2)->get(Util::createString("timestampv"))->getString()<<",";
                // cout<<table2_SDPT->getRow(index2)->get(Util::createString("sym"))->getString()<<",";
                // cout<<table2_SDPT->getRow(index2)->get(Util::createString("price1"))->getString()<<endl<<endl;
                
                EXPECT_EQ(msg->get(0)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("datetimev"))->getString());
                EXPECT_EQ(msg->get(1)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("timestampv"))->getString());
                EXPECT_EQ(msg->get(2)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("sym"))->getString());
                EXPECT_EQ(msg->get(3)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("price1"))->getString());
                index2++;
            }
        }
    };
    Util::sleep(2000);
    
    client.unsubscribe(hostName, port, "SDoutTables", "actionTest");

    EXPECT_EQ(msg1_total, 1000);
    EXPECT_EQ(msg2_total, 1000);
    usedPorts.push_back(listenport);
}

TEST_F(streamingDeserilizerTester,test_Threadpooledclient_threadCount_1_subscribeWithstreamDeserilizer){
    int msg1_total=0;
    int msg2_total=0;
    int index1=0;
    int index2=0;
    srand(time(0));

    int listenport = rand() % 13000 + 2000;
    while(find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };
	Signal notify;
    Mutex mutex;
    StreamDeserializerSP sdsp = createStreamDeserializer();

    TableSP table1_SDPT = conn.run("table1_SDPT");
    TableSP table2_SDPT = conn.run("table2_SDPT");

    auto onehandler = [&](Message msg) {
        const string &symbol = msg.getSymbol();
        LockGuard<Mutex> lock(&mutex);
        if (symbol == "msg1"){ 
            msg1_total+=1;
            EXPECT_EQ(msg->get(0)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("datetimev"))->getString());
            EXPECT_EQ(msg->get(1)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("timestampv"))->getString());
            EXPECT_EQ(msg->get(2)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("sym"))->getString());
            EXPECT_EQ(msg->get(3)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("price1"))->getString());
            EXPECT_EQ(msg->get(4)->getString(),table1_SDPT->getRow(index1)->get(Util::createString("price2"))->getString());
            index1++;
        }
        else if (symbol == "msg2"){ 
            msg2_total+=1;
            EXPECT_EQ(msg->get(0)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("datetimev"))->getString());
            EXPECT_EQ(msg->get(1)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("timestampv"))->getString());
            EXPECT_EQ(msg->get(2)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("sym"))->getString());
            EXPECT_EQ(msg->get(3)->getString(),table2_SDPT->getRow(index2)->get(Util::createString("price1"))->getString());
            index2++;
        }
        if (msg1_total + msg2_total == 2000) {
            notify.set();
        }
    };

    ThreadPooledClient client(listenport, 1);
    auto threadVec = client.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true,nullptr, false,false, "admin","123456",sdsp);
    EXPECT_EQ(threadVec.size(),1);
    notify.wait();

    client.unsubscribe(hostName, port, "SDoutTables", "test_SD");

    EXPECT_EQ(msg1_total, 1000);
    EXPECT_EQ(msg2_total, 1000);
    usedPorts.push_back(listenport);
}

TEST_F(streamingDeserilizerTester,test_Threadpooledclient_threadCount_2_subscribeWithstreamDeserilizer){
    int msg1_total=0;
    int msg2_total=0;

    srand(time(0));

    int listenport = rand() % 13000 + 2000;
    while(find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };

    StreamDeserializerSP sdsp = createStreamDeserializer();

    TableSP table1_SDPT = conn.run("table1_SDPT");
    TableSP table2_SDPT = conn.run("table2_SDPT");

	Signal notify;
    Mutex mutex;
    auto onehandler = [&](Message msg) {
        const string &symbol = msg.getSymbol();
        LockGuard<Mutex> lock(&mutex);
        if (symbol == "msg1"){ 
            msg1_total+=1;
        }
        else if (symbol == "msg2"){ 
            msg2_total+=1;
        }
        if (msg1_total + msg2_total == 2000) {
            notify.set();
        }
    };

    ThreadPooledClient client(listenport, 2);
    auto threadVec = client.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true,nullptr, false,false, "admin","123456",sdsp);
    EXPECT_EQ(threadVec.size(),2);
    notify.wait();

    client.unsubscribe(hostName, port, "SDoutTables", "test_SD");

    EXPECT_EQ(msg1_total, 1000);
    EXPECT_EQ(msg2_total, 1000);
    usedPorts.push_back(listenport);
}

TEST_F(streamingDeserilizerTester,test_Threadclient_with_msgAsTable_True){
    srand(time(0));

    int listenport = rand() % 13000 + 2000;
    while(find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };

    StreamDeserializerSP sdsp = createStreamDeserializer();

    auto onehandler = [&](Message msg) {
        const string &symbol = msg.getSymbol();
        cout<<symbol<<endl;
    };

    ThreadedClient threadedClient(listenport);
    EXPECT_ANY_THROW(auto thread1 = threadedClient.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true,nullptr, true, false, "admin", "123456",sdsp));
    
    usedPorts.push_back(listenport);
}

TEST_F(streamingDeserilizerTester,test_Pollingclient_with_msgAsTable_True){
    srand(time(0));

    int listenport = rand() % 13000 + 2000;
    while(find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };

    StreamDeserializerSP sdsp = createStreamDeserializer();

    auto onehandler = [&](Message msg) {
        const string &symbol = msg.getSymbol();
        cout<<symbol<<endl;
    };

    PollingClient client(listenport);
    EXPECT_ANY_THROW(auto queue = client.subscribe(hostName, port,"SDoutTables", "actionTest", 0, true,nullptr, true,false, "admin","123456",sdsp));
    
    usedPorts.push_back(listenport);
}

TEST_F(streamingDeserilizerTester,test_Threadpooledclient_with_msgAsTable_True){
    srand(time(0));

    int listenport = rand() % 13000 + 2000;
    while(find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };

    StreamDeserializerSP sdsp = createStreamDeserializer();

    auto onehandler = [&](Message msg) {
        const string &symbol = msg.getSymbol();
        cout<<symbol<<endl;
    };

    ThreadPooledClient client(listenport, 1);
    
    EXPECT_ANY_THROW(auto threadVec = client.subscribe(hostName, port, onehandler, "SDoutTables", "test_SD", 0, true,nullptr, true,false, "admin","123456",sdsp));
    
    usedPorts.push_back(listenport);
}