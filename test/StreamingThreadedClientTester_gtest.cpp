#include <iostream>

class StreamingThreadedClientTester:public testing::Test
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

void createSharedTableAndReplay() {
	string script = "login(\"admin\",\"123456\")\n\
            st1 = streamTable(100:0, `datetimev`timestampv`sym`price1`price2,[DATETIME,TIMESTAMP,SYMBOL,DOUBLE,DOUBLE])\n\
            enableTableShareAndPersistence(table=st1, tableName=`outTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\n\
            go\n\
            setStreamTableFilterColumn(outTables, `sym)";
	conn.run(script);

	string replayScript = "n = 1000;table1_STCT = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);\
            tableInsert(table1_STCT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));\
            replay(inputTables=table1_STCT, outputTables=`outTables, dateColumn=`timestampv, timeColumn=`timestampv)";
	conn.run(replayScript);
}


TEST_F(StreamingThreadedClientTester,test_subscribe_onehandler){
    createSharedTableAndReplay();
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
    ThreadedClient threadedClient(listenport);
    auto thread = threadedClient.subscribe(hostName, port, onehandler, "outTables", "mutiSchemaOne", 0);
    Util::sleep(2000);
    cout << "total size: " << msg_total << endl;
    threadedClient.unsubscribe(hostName, port, "outTables", "mutiSchemaOne");
    usedPorts.push_back(listenport);
    EXPECT_EQ(threadedClient.getQueueDepth(thread),0);
    EXPECT_EQ(msg_total,1000);
}

TEST_F(StreamingThreadedClientTester,test_subscribe_batchhandler){
    createSharedTableAndReplay();
	int msg_total = 0;

	auto batchhandler = [&](vector<Message> msgs) {
		for (auto &msg : msgs) {
			msg_total += 1;
			// cout  << msg->getString() << endl;
		}
	};

	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };

    ThreadedClient threadedClient(listenport);
    auto thread = threadedClient.subscribe(hostName, port, batchhandler, "outTables", "mutiSchemaBatch", 0);
    Util::sleep(2000);
    cout << "total size: " << msg_total << endl;
    threadedClient.unsubscribe(hostName, port, "outTables", "mutiSchemaBatch");
    usedPorts.push_back(listenport);

    EXPECT_EQ(threadedClient.getQueueDepth(thread),0);
    EXPECT_EQ(msg_total,1000);
}

TEST_F(StreamingThreadedClientTester,test_subscribe_onehandler_tableNameNull){
    createSharedTableAndReplay();
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

    ThreadedClient threadedClient(listenport);
    usedPorts.push_back(listenport);
    auto thread = threadedClient.subscribe(hostName, port, onehandler, "");
    Util::sleep(2000);
    cout << "total size: " << msg_total << endl;
    threadedClient.unsubscribe(hostName, port, "outTables", "mutiSchemaOne");
    usedPorts.push_back(listenport);

    EXPECT_EQ(threadedClient.getQueueDepth(thread),0);
    EXPECT_EQ(msg_total,0);
}

TEST_F(StreamingThreadedClientTester,test_subscribe_batchhandler_tableNameNull){
    createSharedTableAndReplay();
	int msg_total = 0;

	auto batchhandler = [&](vector<Message> msgs) {
		for (auto &msg : msgs) {
			msg_total += 1;
			// cout  << msg->getString() << endl;
		}
	};

	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };

    ThreadedClient threadedClient(listenport);
    usedPorts.push_back(listenport);
    auto thread = threadedClient.subscribe(hostName, port, batchhandler, "");
    Util::sleep(2000);
    cout << "total size: " << msg_total << endl;
    threadedClient.unsubscribe(hostName, port, "outTables", "mutiSchemaOne");
    usedPorts.push_back(listenport);
    
    EXPECT_EQ(threadedClient.getQueueDepth(thread),0);
    EXPECT_EQ(msg_total,0);
}

TEST_F(StreamingThreadedClientTester,test_subscribe_onehandler_offsetNegative){
    createSharedTableAndReplay();
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

    ThreadedClient threadedClient(listenport);
    auto thread = threadedClient.subscribe(hostName, port, onehandler, "outTables", "actionTest", -1);
    Util::sleep(2000);
    cout << "total size: " << msg_total << endl;
    threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
    usedPorts.push_back(listenport);

    EXPECT_EQ(threadedClient.getQueueDepth(thread),0);
    EXPECT_EQ(msg_total,0);
}

TEST_F(StreamingThreadedClientTester,test_subscribe_batchhandler_offsetNegative){
    createSharedTableAndReplay();
	int msg_total = 0;

	auto batchhandler = [&](vector<Message> msgs) {
		for (auto &msg : msgs) {
			msg_total += 1;
			// cout  << msg->getString() << endl;
		}
	};

	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };
    ThreadedClient threadedClient(listenport);
    auto thread = threadedClient.subscribe(hostName, port, batchhandler, "outTables", "actionTest", -1);
    Util::sleep(2000);
    cout << "total size: " << msg_total << endl;
    threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
    usedPorts.push_back(listenport);

    EXPECT_EQ(threadedClient.getQueueDepth(thread),0);
    EXPECT_EQ(msg_total,0);
}

// TEST_F(StreamingThreadedClientTester,test_subscribe_onehandler_offsetMoreThanRows){
//     createSharedTableAndReplay();
// 	int msg_total = 0;

// 	auto onehandler = [&](Message msg) {
// 		msg_total += 1;
// 		// cout << msg->getString() << endl;
// 	};

// 	srand(time(0));

    // int listenport = rand() % 13000 + 2000;
    // if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
    //     listenport = rand() % 13000 + 2000;
    // };

//     ThreadedClient threadedClient(listenport);
//     auto thread = threadedClient.subscribe(hostName, port, onehandler, "outTables", "actionTest", 2001);
//     Util::sleep(2000);
//     cout << "total size: " << msg_total << endl;
//     threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
    // usedPorts.push_back(listenport);
//     EXPECT_EQ(msg_total,0);
// }

// TEST_F(StreamingThreadedClientTester,test_subscribe_batchhandler_offsetMoreThanRows){
//     createSharedTableAndReplay();
// 	int msg_total = 0;

	// auto batchhandler = [&](vector<Message> msgs) {
	// 	for (auto &msg : msgs) {
	// 		msg_total += 1;
	// 		// cout  << msg->getString() << endl;
	// 	}
	// };

// 	srand(time(0));

    // int listenport = rand() % 13000 + 2000;
    // if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
    //     listenport = rand() % 13000 + 2000;
    // };

//     ThreadedClient threadedClient(listenport);
//     auto thread = threadedClient.subscribe(hostName, port, batchhandler, "outTables", "actionTest", 2001);
//     Util::sleep(2000);
//     cout << "total size: " << msg_total << endl;
//     threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
    // usedPorts.push_back(listenport);
//     EXPECT_EQ(msg_total,0);
// }

TEST_F(StreamingThreadedClientTester,test_subscribe_onehandler_filter){
    createSharedTableAndReplay();
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

    ThreadedClient threadedClient(listenport);
    VectorSP filter=Util::createVector(DT_SYMBOL,1,1);
    filter->setString(0,"b");
    auto thread = threadedClient.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0, true, filter);
    Util::sleep(2000);
    cout << "total size: " << msg_total << endl;
    threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");    
    usedPorts.push_back(listenport);

    EXPECT_EQ(threadedClient.getQueueDepth(thread),0);
    EXPECT_EQ(msg_total>0,true);
}

TEST_F(StreamingThreadedClientTester,test_subscribe_batchhandler_filter){
    createSharedTableAndReplay();
	int msg_total = 0;

	auto batchhandler = [&](vector<Message> msgs) {
		for (auto &msg : msgs) {
			msg_total += 1;
			// cout  << msg->getString() << endl;
		}
	};

	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };

    ThreadedClient threadedClient(listenport);
    VectorSP filter=Util::createVector(DT_SYMBOL,1,1);
    filter->setString(0,"b");
    auto thread = threadedClient.subscribe(hostName, port, batchhandler, "outTables", "actionTest", 0, true, filter);
    Util::sleep(2000);
    cout << "total size: " << msg_total << endl;
    threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");    
    usedPorts.push_back(listenport);

    EXPECT_EQ(threadedClient.getQueueDepth(thread),0);
    EXPECT_EQ(msg_total>0,true);
}

TEST_F(StreamingThreadedClientTester,test_subscribe_onehandler_msgAsTable){
    createSharedTableAndReplay();
	int msg_total = 0;

	auto onehandler = [&](Message msg) {
        TableSP t = (TableSP)msg;
        msg_total += t->rows();
        EXPECT_EQ(msg->getForm(),6);
	};

	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };

    ThreadedClient threadedClient(listenport);
    auto thread = threadedClient.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0, true, nullptr,true);
    Util::sleep(2000);
    cout << "total size: " << msg_total << endl;
    threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
    usedPorts.push_back(listenport);

    EXPECT_EQ(threadedClient.getQueueDepth(thread),0);
    EXPECT_EQ(msg_total,1000);
}

// TEST_F(StreamingThreadedClientTester,test_subscribe_batchhandler_msgAsTable){
//     createSharedTableAndReplay();
// 	int msg_total = 0;

// 	auto batchhandler = [&](vector<Message> msgs) {
// 		for (auto &msg : msgs) {
//             TableSP t = (TableSP)msg;
//             msg_total += t->rows();
//             EXPECT_EQ(msg->getForm(),6);
// 		}
// 	};

// 	srand(time(0));

    // int listenport = rand() % 13000 + 2000;
    // if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
    //     listenport = rand() % 13000 + 2000;
    // };

//     ThreadedClient threadedClient(listenport);
//     auto thread = threadedClient.subscribe(hostName, port, batchhandler, "outTables", "actionTest", 0, true, nullptr,true);
//     Util::sleep(2000);
//     cout << "total size: " << msg_total << endl;
//     threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
//     usedPorts.push_back(listenport);
//     EXPECT_EQ(threadedClient.getQueueDepth(thread),0);
    // EXPECT_EQ(msg_total,1000);
// }

TEST_F(StreamingThreadedClientTester,test_subscribe_onehandler_allowExists){
    createSharedTableAndReplay();
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

    ThreadedClient threadedClient(listenport);
    auto thread = threadedClient.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0, true, nullptr,false,true);
    Util::sleep(2000);
    cout << "total size: " << msg_total << endl;
    threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
    usedPorts.push_back(listenport);

    EXPECT_EQ(threadedClient.getQueueDepth(thread),0);
    EXPECT_EQ(msg_total,1000);
}

TEST_F(StreamingThreadedClientTester,test_subscribe_batchhandler_allowExists){
    createSharedTableAndReplay();
	int msg_total = 0;

	auto batchhandler = [&](vector<Message> msgs) {
		for (auto &msg : msgs) {
			msg_total += 1;
			// cout  << msg->getString() << endl;
		}
	};

	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };

    ThreadedClient threadedClient(listenport);
    auto thread = threadedClient.subscribe(hostName, port, batchhandler, "outTables", "actionTest", 0, true, nullptr,false,true);
    Util::sleep(2000);
    cout << "total size: " << msg_total << endl;
    threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
    usedPorts.push_back(listenport);

    EXPECT_EQ(threadedClient.getQueueDepth(thread),0);
    EXPECT_EQ(msg_total,1000);
}

TEST_F(StreamingThreadedClientTester,test_subscribe_batchhandler_batchSize){
    createSharedTableAndReplay();
	int msg_total = 0;

	auto batchhandler = [&](vector<Message> msgs) {
		for (auto &msg : msgs) {
			msg_total += 1;
			// cout  << msg->getString() << endl;
		}
	};

	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };

    ThreadedClient threadedClient(listenport);
    auto thread = threadedClient.subscribe(hostName, port, batchhandler, "outTables", "actionTest",0,true,nullptr,false,1000);
    Util::sleep(2000);
    cout << "total size: " << msg_total << endl;
    threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
    usedPorts.push_back(listenport);

    EXPECT_EQ(threadedClient.getQueueDepth(thread),0);
    EXPECT_EQ(msg_total,1000);
}

TEST_F(StreamingThreadedClientTester,test_subscribe_batchhandler_throttle){
    createSharedTableAndReplay();
	int msg_total = 0;

	auto batchhandler = [&](vector<Message> msgs) {
		for (auto &msg : msgs) {
			msg_total += 1;
			// cout  << msg->getString() << endl;
		}
	};

	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };

    ThreadedClient threadedClient(listenport);
    auto thread = threadedClient.subscribe(hostName, port, batchhandler, "outTables", "actionTest",0,true,nullptr,false,1000,1.0);
    Util::sleep(2000);
    cout << "total size: " << msg_total << endl;
    threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
    usedPorts.push_back(listenport);

    EXPECT_EQ(threadedClient.getQueueDepth(thread),0);
    EXPECT_EQ(msg_total,1000);
}

TEST_F(StreamingThreadedClientTester,test_subscribe_onehandler_hostNull){
    createSharedTableAndReplay();
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
    ThreadedClient threadedClient(listenport);
    auto thread = threadedClient.subscribe("", port, onehandler, "outTables", "actionTest", 0);
    Util::sleep(2000);
    cout << "total size: " << msg_total << endl;
    threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
    usedPorts.push_back(listenport);

    EXPECT_EQ(threadedClient.getQueueDepth(thread),0);
    EXPECT_EQ(msg_total,0);

}

TEST_F(StreamingThreadedClientTester,test_subscribe_batchhandler_hostNull){
    createSharedTableAndReplay();
	int msg_total = 0;

	auto batchhandler = [&](vector<Message> msgs) {
		for (auto &msg : msgs) {
			msg_total += 1;
			// cout  << msg->getString() << endl;
		}
	};

	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };

    usedPorts.push_back(listenport);
    ThreadedClient threadedClient(listenport);
    auto thread = threadedClient.subscribe("", port, batchhandler, "outTables", "actionTest", 0);
    Util::sleep(2000);
    cout << "total size: " << msg_total << endl;
    threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
    usedPorts.push_back(listenport);

    EXPECT_EQ(threadedClient.getQueueDepth(thread),0);
    EXPECT_EQ(msg_total,0);

}

// TEST_F(StreamingThreadedClientTester,test_subscribe_onehandler_portNull){
//     createSharedTableAndReplay();
// 	int msg_total = 0;

// 	auto onehandler = [&](Message msg) {
// 		msg_total += 1;
// 		// cout << msg->getString() << endl;
// 	};

// 	srand(time(0));

    // int listenport = rand() % 13000 + 2000;
    // if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
    //     listenport = rand() % 13000 + 2000;
    // };
//     usedPorts.push_back(listenport);
//     ThreadedClient threadedClient(listenport);
//     EXPECT_ANY_THROW(auto thread = threadedClient.subscribe(hostName, NULL, onehandler, "outTables", "actionTest", 0));

// }

TEST_F(StreamingThreadedClientTester,test_subscribe_onehandler_actionNameNull){
    createSharedTableAndReplay();
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
    ThreadedClient threadedClient(listenport);
    auto thread = threadedClient.subscribe(hostName, port, onehandler, "outTables", "", 0);
    Util::sleep(2000);
    TableSP stat = conn.run("getStreamingStat().pubTables");

    EXPECT_EQ(stat->getColumn(0)->getRow(0)->getString(),"outTables");
    EXPECT_EQ(stat->getColumn(2)->getRow(0)->getInt(),1000);
    EXPECT_EQ(stat->getColumn(3)->getRow(0)->getString(),"");

    cout << "total size: " << msg_total << endl;
    threadedClient.unsubscribe(hostName, port, "outTables", "");
    usedPorts.push_back(listenport);

    EXPECT_EQ(threadedClient.getQueueDepth(thread),0);
    EXPECT_EQ(msg_total,1000);

}

TEST_F(StreamingThreadedClientTester,test_subscribe_batchhandler_actionNameNull){
    createSharedTableAndReplay();
	int msg_total = 0;

	auto batchhandler = [&](vector<Message> msgs) {
		for (auto &msg : msgs) {
			msg_total += 1;
			// cout  << msg->getString() << endl;
		}
	};

	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };

    usedPorts.push_back(listenport);
    ThreadedClient threadedClient(listenport);
    auto thread = threadedClient.subscribe(hostName, port, batchhandler, "outTables", "", 0);
    Util::sleep(2000);
    TableSP stat = conn.run("getStreamingStat().pubTables");

    EXPECT_EQ(stat->getColumn(0)->getRow(0)->getString(),"outTables");
    EXPECT_EQ(stat->getColumn(2)->getRow(0)->getInt(),1000);
    EXPECT_EQ(stat->getColumn(3)->getRow(0)->getString(),"");

    cout << "total size: " << msg_total << endl;
    threadedClient.unsubscribe(hostName, port, "outTables", "");
    usedPorts.push_back(listenport);

    EXPECT_EQ(threadedClient.getQueueDepth(thread),0);
    EXPECT_EQ(msg_total,1000);

}

TEST_F(StreamingThreadedClientTester,test_unsubscribe_onehandler_hostNull){
    createSharedTableAndReplay();
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

    ThreadedClient threadedClient(listenport);
    auto thread = threadedClient.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0);
    Util::sleep(2000);
    cout << "total size: " << msg_total << endl;
    
    usedPorts.push_back(listenport);
    EXPECT_ANY_THROW(threadedClient.unsubscribe("", port, "outTables", "actionTest"));

    threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
}

TEST_F(StreamingThreadedClientTester,test_unsubscribe_portNull){
    createSharedTableAndReplay();
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

    ThreadedClient threadedClient(listenport);
    auto thread = threadedClient.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0);
    Util::sleep(2000);
    cout << "total size: " << msg_total << endl;
    
    usedPorts.push_back(listenport);
    EXPECT_ANY_THROW(threadedClient.unsubscribe(hostName, NULL, "outTables", "actionTest"));

    threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
}

TEST_F(StreamingThreadedClientTester,test_unsubscribe_tableNameNull){
    createSharedTableAndReplay();
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

    ThreadedClient threadedClient(listenport);
    auto thread = threadedClient.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0);
    Util::sleep(2000);
    cout << "total size: " << msg_total << endl;
    
    usedPorts.push_back(listenport);
    EXPECT_ANY_THROW(threadedClient.unsubscribe(hostName, port, "", "actionTest"));

    threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
}

TEST_F(StreamingThreadedClientTester,test_unsubscribe_actionNameNull){
    createSharedTableAndReplay();
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

    ThreadedClient threadedClient(listenport);
    auto thread = threadedClient.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0);
    Util::sleep(2000);
    cout << "total size: " << msg_total << endl;
    
    usedPorts.push_back(listenport);
    threadedClient.unsubscribe(hostName, port, "outTables", "");
    TableSP stat = conn.run("getStreamingStat().pubTables");

    EXPECT_EQ(stat->getColumn(0)->getRow(0)->getString(),"outTables");
    EXPECT_EQ(stat->getColumn(2)->getRow(0)->getInt(),1000);
    EXPECT_EQ(stat->getColumn(3)->getRow(0)->getString(),"actionTest");

    threadedClient.unsubscribe(hostName, port, "outTables", "actionTest");
}

TEST_F(StreamingThreadedClientTester,tes_onehandlert_subscribe_twice){
    createSharedTableAndReplay();
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
    ThreadedClient threadedClient(listenport);
    auto thread1 = threadedClient.subscribe(hostName, port, onehandler, "outTables", "mutiSchemaOne", 0);
    Util::sleep(2000);
    cout << "total size: " << msg_total << endl;
    auto thread2 = threadedClient.subscribe(hostName, port, onehandler, "outTables", "mutiSchemaOne", 0);
    Util::sleep(2000);
    cout << "total size: " << msg_total << endl;
    threadedClient.unsubscribe(hostName, port, "outTables", "mutiSchemaOne");
    usedPorts.push_back(listenport);

    EXPECT_EQ(threadedClient.getQueueDepth(thread1),0);
    EXPECT_EQ(msg_total, 1000);
}

TEST_F(StreamingThreadedClientTester,test_batchhandler_subscribe_twice){
    createSharedTableAndReplay();
	int msg_total = 0;

	auto batchhandler = [&](vector<Message> msgs) {
		for (auto &msg : msgs) {
			msg_total += 1;
			// cout  << msg->getString() << endl;
		}
	};

	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };

    ThreadedClient threadedClient(listenport);
    auto thread1 = threadedClient.subscribe(hostName, port, batchhandler, "outTables", "mutiSchemaBatch", 0);
    Util::sleep(2000);
    cout << "total size: " << msg_total << endl;
    auto thread2 = threadedClient.subscribe(hostName, port, batchhandler, "outTables", "mutiSchemaBatch", 0);
    Util::sleep(2000);
    cout << "total size: " << msg_total << endl;
    threadedClient.unsubscribe(hostName, port, "outTables", "mutiSchemaBatch");
    usedPorts.push_back(listenport);

    EXPECT_EQ(threadedClient.getQueueDepth(thread1),0);
    EXPECT_EQ(msg_total, 1000);
}