//#include <unistd.h>
#include <chrono>
#include <iostream>

class StreamingThreadPooledClientTester:public testing::Test
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
            string del_streamtable="login(\"admin\",\"123456\");\n\
                                    try{ dropStreamTable(`outTables);dropStreamTable(`st1) }\
                                    catch(ex){}";
            conn.run(del_streamtable);
		}
    }
    virtual void TearDown()
    {
        pass;
    }
};

void createSharedTableAndReplay1() {
	string script = "login(\"admin\",\"123456\")\n\
            st1 = streamTable(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE])\n\
            enableTableShareAndPersistence(table=st1, tableName=`outTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\n\
            go\n\
            setStreamTableFilterColumn(outTables, `sym)";
	conn.run(script);

	string replayScript = "n = 1000;table1_STPCT = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);\
            tableInsert(table1_STPCT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));\
            replay(inputTables=table1_STPCT, outputTables=`outTables, dateColumn=`timestampv, timeColumn=`timestampv)";
	conn.run(replayScript);
}

TEST_F(StreamingThreadPooledClientTester,test_client_threadCount_1){
    createSharedTableAndReplay1();
	int msg_total = 0;

	Signal notify;
    Mutex mutex;
	auto onehandler = [&](Message msg) {
        LockGuard<Mutex> lock(&mutex);
		msg_total += 1;
		if (msg_total == 1000) {
            notify.set();
        }
		// cout << msg->getString() << endl;
	};

	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };
    ThreadPooledClient client(listenport, 1);
    auto threadVec = client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0);
    EXPECT_EQ(threadVec.size(),1);

    notify.wait();
    cout << "total size:" << msg_total << endl;
    client.unsubscribe(hostName, port, "outTables", "actionTest");
    usedPorts.push_back(listenport);
    
    EXPECT_EQ(msg_total,1000);
    for (auto &t:threadVec){
        EXPECT_EQ(client.getQueueDepth(t),0);
    }
}

TEST_F(StreamingThreadPooledClientTester,test_client_threadCount_2){
    createSharedTableAndReplay1();
	int msg_total = 0;

	Signal notify;
    Mutex mutex;
	auto onehandler = [&](Message msg) {
        LockGuard<Mutex> lock(&mutex);
		msg_total += 1;
		if (msg_total == 1000) {
            notify.set();
        }
		// cout << msg->getString() << endl;
	};

	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };
    ThreadPooledClient client(listenport, 2);
    auto threadVec = client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0);
    EXPECT_EQ(threadVec.size(),2);

    notify.wait();
    cout << "total size:" << msg_total << endl;
    client.unsubscribe(hostName, port, "outTables", "actionTest");
    usedPorts.push_back(listenport);
    
    EXPECT_EQ(msg_total,1000);
    for (auto &t:threadVec){
        EXPECT_EQ(client.getQueueDepth(t),0);
    }
}

TEST_F(StreamingThreadPooledClientTester,test_client_threadCount_4){
    createSharedTableAndReplay1();
	int msg_total = 0;

	Signal notify;
    Mutex mutex;
	auto onehandler = [&](Message msg) {
        LockGuard<Mutex> lock(&mutex);
		msg_total += 1;
		if (msg_total == 1000) {
            notify.set();
        }
		// cout << msg->getString() << endl;
	};

	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };
    ThreadPooledClient client(listenport, 4);
    auto threadVec = client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0);
    EXPECT_EQ(threadVec.size(),4);

    notify.wait();
    cout << "total size:" << msg_total << endl;
    client.unsubscribe(hostName, port, "outTables", "actionTest");
    usedPorts.push_back(listenport);
    
    EXPECT_EQ(msg_total,1000);
    for (auto &t:threadVec){
        EXPECT_EQ(client.getQueueDepth(t),0);
    }
}

TEST_F(StreamingThreadPooledClientTester,test_client_threadCount_8){
    createSharedTableAndReplay1();
	int msg_total = 0;

	Signal notify;
    Mutex mutex;
	auto onehandler = [&](Message msg) {
        LockGuard<Mutex> lock(&mutex);
		msg_total += 1;
		if (msg_total == 1000) {
            notify.set();
        }
		// cout << msg->getString() << endl;
	};

	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };
    ThreadPooledClient client(listenport, 8);
    auto threadVec = client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0);
    EXPECT_EQ(threadVec.size(),8);

    notify.wait();
    cout << "total size:" << msg_total << endl;
    client.unsubscribe(hostName, port, "outTables", "actionTest");
    usedPorts.push_back(listenport);
    
    EXPECT_EQ(msg_total,1000);
    for (auto &t:threadVec){
        EXPECT_EQ(client.getQueueDepth(t),0);
    }
}

TEST_F(StreamingThreadPooledClientTester,test_client_threadCount_16){
    createSharedTableAndReplay1();
	int msg_total = 0;

	Signal notify;
    Mutex mutex;
	auto onehandler = [&](Message msg) {
        LockGuard<Mutex> lock(&mutex);
		msg_total += 1;
		if (msg_total == 1000) {
            notify.set();
        }
		// cout << msg->getString() << endl;
	};

	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };
    ThreadPooledClient client(listenport, 16);
    auto threadVec = client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0);
    EXPECT_EQ(threadVec.size(),16);

    notify.wait();
    cout << "total size:" << msg_total << endl;
    client.unsubscribe(hostName, port, "outTables", "actionTest");
    usedPorts.push_back(listenport);
    
    EXPECT_EQ(msg_total,1000);
    for (auto &t:threadVec){
        EXPECT_EQ(client.getQueueDepth(t),0);
    }
}

TEST_F(StreamingThreadPooledClientTester,test_client_threadCount_32){
    createSharedTableAndReplay1();
	int msg_total = 0;

	Signal notify;
    Mutex mutex;
	auto onehandler = [&](Message msg) {
        LockGuard<Mutex> lock(&mutex);
		msg_total += 1;
		if (msg_total == 1000) {
            notify.set();
        }
		// cout << msg->getString() << endl;
	};

	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };
    ThreadPooledClient client(listenport, 32);
    auto threadVec = client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0);
    EXPECT_EQ(threadVec.size(),32);

    notify.wait();
    cout << "total size:" << msg_total << endl;
    client.unsubscribe(hostName, port, "outTables", "actionTest");
    usedPorts.push_back(listenport);
    
    EXPECT_EQ(msg_total,1000);
    for (auto &t:threadVec){
        EXPECT_EQ(client.getQueueDepth(t),0);
    }
}

TEST_F(StreamingThreadPooledClientTester,test_subscribe_onehandler_hostNull){
    createSharedTableAndReplay1();
	int msg_total = 0;

	auto onehandler = [&](Message msg) {
		msg_total += 1;
		// cout << msg->getString() << endl;
	};

	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };

    usedPorts.push_back(listenport);
    ThreadPooledClient client(listenport, 2);
    auto threadVec = client.subscribe("", port, onehandler, "outTables", "actionTest", 0);
    cout << "total size:" << msg_total << endl;
    client.unsubscribe(hostName, port, "outTables", "actionTest");
    usedPorts.push_back(listenport);

    EXPECT_EQ(msg_total,0);
    for (auto &t:threadVec){
        EXPECT_EQ(client.getQueueDepth(t),0);
    }

}

TEST_F(StreamingThreadPooledClientTester,test_subscribe_onehandler_portNull){
    createSharedTableAndReplay1();
	int msg_total = 0;

	auto onehandler = [&](Message msg) {
		msg_total += 1;
		// cout << msg->getString() << endl;
	};

	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };

    usedPorts.push_back(listenport);
    ThreadPooledClient client(listenport, 2);
    auto threadVec = client.subscribe(hostName, NULL, onehandler, "outTables", "actionTest", 0);
    cout << "total size:" << msg_total << endl;
    client.unsubscribe(hostName, port, "outTables", "actionTest");
    usedPorts.push_back(listenport);

    EXPECT_EQ(msg_total,0);
    for (auto &t:threadVec){
        EXPECT_EQ(client.getQueueDepth(t),0);
    }
}

TEST_F(StreamingThreadPooledClientTester,test_subscribe_onehandler_actionNameNull){
    createSharedTableAndReplay1();
	int msg_total = 0;

	Signal notify;
    Mutex mutex;
	auto onehandler = [&](Message msg) {
        LockGuard<Mutex> lock(&mutex);
		msg_total += 1;
		if (msg_total == 1000) {
            notify.set();
        }
		// cout << msg->getString() << endl;
	};

	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };

    usedPorts.push_back(listenport);
    ThreadPooledClient client(listenport, 2);
    auto threadVec = client.subscribe(hostName, port, onehandler, "outTables", "", 0);
    notify.wait();
    TableSP stat = conn.run("getStreamingStat().pubTables");

    EXPECT_EQ(stat->getColumn(0)->getRow(0)->getString(),"outTables");
    EXPECT_EQ(stat->getColumn(2)->getRow(0)->getInt(),1000);
    EXPECT_EQ(stat->getColumn(3)->getRow(0)->getString(),"");

    cout << "total size:" << msg_total << endl;
    client.unsubscribe(hostName, port, "outTables", "");
    usedPorts.push_back(listenport);

    EXPECT_EQ(msg_total,1000);
    for (auto &t:threadVec){
        EXPECT_EQ(client.getQueueDepth(t),0);
    }

}

TEST_F(StreamingThreadPooledClientTester,test_subscribe_onehandler_tableNameNull){
    createSharedTableAndReplay1();
	int msg_total = 0;

	auto onehandler = [&](Message msg) {
		msg_total += 1;
		cout << msg->getString() << endl;
	};

	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };

    ThreadPooledClient client(listenport, 2);
    usedPorts.push_back(listenport);
    auto threadVec = client.subscribe(hostName, port, onehandler, "", "actionTest", 0);
    cout << "total size:" << msg_total << endl;
    client.unsubscribe(hostName, port, "outTables", "actionTest");
    usedPorts.push_back(listenport);
    
    EXPECT_EQ(msg_total,0);
    for (auto &t:threadVec){
        EXPECT_EQ(client.getQueueDepth(t),0);
    }
}

TEST_F(StreamingThreadPooledClientTester,test_subscribe_onehandler_offsetNegative){
    createSharedTableAndReplay1();
	int msg_total = 0;

	auto onehandler = [&](Message msg) {
		msg_total += 1;
		// cout << msg->getString() << endl;
	};

	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };

    ThreadPooledClient client(listenport, 2);
    auto threadVec = client.subscribe(hostName, port, onehandler, "outTables", "actionTest", -1);
    cout << "total size:" << msg_total << endl;
    client.unsubscribe(hostName, port, "outTables", "actionTest");
    usedPorts.push_back(listenport);

    EXPECT_EQ(msg_total,0);
    for (auto &t:threadVec){
        EXPECT_EQ(client.getQueueDepth(t),0);
    }
}

TEST_F(StreamingThreadPooledClientTester,test_subscribe_onehandler_filter){
    createSharedTableAndReplay1();
	int msg_total = 0;

	Signal notify;
    Mutex mutex;
	auto onehandler = [&](Message msg) {
        LockGuard<Mutex> lock(&mutex);
		msg_total += 1;
		if (msg_total >0) {
            notify.set();
        }
		// cout << msg->getString() << endl;
	};

	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };

    ThreadPooledClient client(listenport, 2);
    VectorSP filter=Util::createVector(DT_SYMBOL,1,1);
    filter->setString(0,"b");
    auto threadVec = client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0, true, filter);
    cout << "total size:" << msg_total << endl;
    notify.wait();

    client.unsubscribe(hostName, port, "outTables", "actionTest");    
    usedPorts.push_back(listenport);

    EXPECT_EQ(msg_total>0,true);
    for (auto &t:threadVec){
        EXPECT_EQ(client.getQueueDepth(t),0);
    }
}

TEST_F(StreamingThreadPooledClientTester,test_subscribe_onehandler_msgAsTable){
    createSharedTableAndReplay1();
	int msg_total = 0;

	Signal notify;
    Mutex mutex;
	auto onehandler = [&](Message msg) {
        LockGuard<Mutex> lock(&mutex);
        TableSP t = (TableSP)msg;
        msg_total += t->rows();
        EXPECT_EQ(msg->getForm(),6);
		if (msg_total == 1000) {
            notify.set();
        }
	};

	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };

    ThreadPooledClient client(listenport, 2);
    auto threadVec = client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0, true, nullptr,true);
    notify.wait();
    cout << "total size:" << msg_total << endl;
    client.unsubscribe(hostName, port, "outTables", "actionTest");
    usedPorts.push_back(listenport);

    EXPECT_EQ(msg_total,1000);
    for (auto &t:threadVec){
        EXPECT_EQ(client.getQueueDepth(t),0);
    }
}

TEST_F(StreamingThreadPooledClientTester,test_subscribe_onehandler_allowExists){
    createSharedTableAndReplay1();
	int msg_total = 0;

	Signal notify;
    Mutex mutex;
	auto onehandler = [&](Message msg) {
        LockGuard<Mutex> lock(&mutex);
		msg_total += 1;
		if (msg_total == 1000) {
            notify.set();
        }
		// cout << msg->getString() << endl;
	};

	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };

    ThreadPooledClient client(listenport, 2);
    auto threadVec = client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0, true, nullptr,false,true);
    notify.wait();
    cout << "total size:" << msg_total << endl;
    client.unsubscribe(hostName, port, "outTables", "actionTest");
    usedPorts.push_back(listenport);

    EXPECT_EQ(msg_total,1000);
    for (auto &t:threadVec){
        EXPECT_EQ(client.getQueueDepth(t),0);
    }
}