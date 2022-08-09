//#include <unistd.h>
#include <chrono>
#include <iostream>

class StreamingPollingClientTester:public testing::Test
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

void createSharedTableAndReplay2() {
	string script = "login(\"admin\",\"123456\")\n\
            st1 = streamTable(100:0, `datetimev`timestampv`sym`price1`price2,[DATETIME,TIMESTAMP,SYMBOL,DOUBLE,DOUBLE])\n\
            enableTableShareAndPersistence(table=st1, tableName=`outTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\n\
            go\n\
            setStreamTableFilterColumn(outTables, `sym)";
	conn.run(script);

	string replayScript = "n = 1000;table1_SPCT = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);\
            tableInsert(table1_SPCT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));\
            replay(inputTables=table1_SPCT, outputTables=`outTables, dateColumn=`timestampv, timeColumn=`timestampv)";
	conn.run(replayScript);
}

TEST_F(StreamingPollingClientTester,test_subscribeUnsubscribe_normal){
    createSharedTableAndReplay2();
	int msg_total= 0;
	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };
    PollingClient client(listenport);
    auto queue = client.subscribe(hostName, port,"outTables", "actionTest", 0);

    Util::sleep(2000);
    vector<Message> msgs;
    while(queue->pop(msgs,1000)) {
        for (auto &msg : msgs) {
            msg_total+= 1;
            // cout << msg->getString() << endl;            
            // handle msg
        }
    }
    cout << "total size: " << msg_total<< endl;
    client.unsubscribe(hostName, port, "outTables","actionTest");
    usedPorts.push_back(listenport);
    
    EXPECT_EQ(msg_total, 1000);

}

TEST_F(StreamingPollingClientTester,test_subscribe_hostNull){
    createSharedTableAndReplay2();
	int msg_total= 0;
	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };
    PollingClient client(listenport);
    auto queue = client.subscribe("", port,"outTables", "actionTest", 0);

    Util::sleep(2000);
    vector<Message> msgs;
    while(queue->pop(msgs,1000)) {
        for (auto &msg : msgs) {
            msg_total+= 1;
            // cout << msg->getString() << endl;            
            // handle msg
        }
    }
    cout << "total size: " << msg_total<< endl;
    client.unsubscribe(hostName, port, "outTables","actionTest");
    usedPorts.push_back(listenport);
    
    EXPECT_EQ(msg_total, 0);

}

TEST_F(StreamingPollingClientTester,test_subscribe_portNull){
    createSharedTableAndReplay2();
	int msg_total= 0;
	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };
    PollingClient client(listenport);
    auto queue = client.subscribe(hostName, NULL,"outTables", "actionTest", 0);

    Util::sleep(2000);
    vector<Message> msgs;
    while(queue->pop(msgs,1000)) {
        for (auto &msg : msgs) {
            msg_total+= 1;
            // cout << msg->getString() << endl;            
            // handle msg
        }
    }
    cout << "total size: " << msg_total<< endl;
    client.unsubscribe(hostName, port, "outTables","actionTest");
    usedPorts.push_back(listenport);
    
    EXPECT_EQ(msg_total, 0);

}

TEST_F(StreamingPollingClientTester,test_subscribe_tableNameNull){
    createSharedTableAndReplay2();
	int msg_total= 0;
	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };
    PollingClient client(listenport);
    auto queue = client.subscribe(hostName, port,"", "actionTest", 0);

    Util::sleep(2000);
    vector<Message> msgs;
    while(queue->pop(msgs,1000)) {
        for (auto &msg : msgs) {
            msg_total+= 1;
            // cout << msg->getString() << endl;            
            // handle msg
        }
    }
    cout << "total size: " << msg_total<< endl;
    client.unsubscribe(hostName, port, "outTables","actionTest");
    usedPorts.push_back(listenport);
    
    EXPECT_EQ(msg_total, 0);

}

TEST_F(StreamingPollingClientTester,test_subscribe_actionNameNull){
    createSharedTableAndReplay2();
	int msg_total= 0;
	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };
    PollingClient client(listenport);
    auto queue = client.subscribe(hostName, port,"outTables", "", 0);

    Util::sleep(2000);
    vector<Message> msgs;
    while(queue->pop(msgs,1000)) {
        for (auto &msg : msgs) {
            msg_total+= 1;
            // cout << msg->getString() << endl;            
            // handle msg
        }
    }
    cout << "total size: " << msg_total<< endl;
    client.unsubscribe(hostName, port, "outTables","");
    usedPorts.push_back(listenport);
    
    EXPECT_EQ(msg_total, 1000);

}

TEST_F(StreamingPollingClientTester,test_subscribe_offsetNegative){
    createSharedTableAndReplay2();
	int msg_total= 0;
	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };
    PollingClient client(listenport);
    auto queue = client.subscribe(hostName, port,"outTables", "actionTest", -1);

    Util::sleep(2000);
    vector<Message> msgs;
    while(queue->pop(msgs,1000)) {
        for (auto &msg : msgs) {
            msg_total+= 1;
            // cout << msg->getString() << endl;            
            // handle msg
        }
    }
    cout << "total size: " << msg_total<< endl;
    client.unsubscribe(hostName, port, "outTables","actionTest");
    usedPorts.push_back(listenport);
    
    EXPECT_EQ(msg_total, 0);

}

TEST_F(StreamingPollingClientTester,test_subscribe_filter){
    createSharedTableAndReplay2();
	int msg_total= 0;
	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };

    VectorSP filter=Util::createVector(DT_SYMBOL,1,1);
    filter->setString(0,"a");

    PollingClient client(listenport);
    auto queue = client.subscribe(hostName, port,"outTables", "actionTest", 0, true, filter);

    Util::sleep(2000);
    vector<Message> msgs;
    while(queue->pop(msgs,1000)) {
        for (auto &msg : msgs) {
            msg_total+= 1;
            // cout << msg->getString() << endl;            
            // handle msg
        }
    }
    cout << "total size: " << msg_total<< endl;
    client.unsubscribe(hostName, port, "outTables","actionTest");
    usedPorts.push_back(listenport);
    
    EXPECT_EQ(msg_total>0,true);

}

TEST_F(StreamingPollingClientTester,test_subscribe_msgAsTable){
    createSharedTableAndReplay2();
	int msg_total= 0;
	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };
    PollingClient client(listenport);
    auto queue = client.subscribe(hostName, port,"outTables", "actionTest", 0, true, nullptr, true);

    Util::sleep(2000);
    vector<Message> msgs;
    while(queue->pop(msgs,1000)) {
        for (auto &msg : msgs) {
            TableSP t = (TableSP)msg;
            msg_total+= t->rows();
            EXPECT_EQ(msg->getForm(),6);
            // cout << msg->getString() << endl;            
            // handle msg
        }
    }
    cout << "total size: " << msg_total<< endl;
    client.unsubscribe(hostName, port, "outTables","actionTest");
    usedPorts.push_back(listenport);
    
    EXPECT_EQ(msg_total, 1000);

}

TEST_F(StreamingPollingClientTester,test_subscribe_allowExists){
    createSharedTableAndReplay2();
	int msg_total= 0;
	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };
    PollingClient client(listenport);
    auto queue = client.subscribe(hostName, port,"outTables", "actionTest", 0, true, nullptr, false, true);

    Util::sleep(2000);
    vector<Message> msgs;
    while(queue->pop(msgs,1000)) {
        for (auto &msg : msgs) {
            msg_total+= 1;
            // cout << msg->getString() << endl;            
            // handle msg
        }
    }
    cout << "total size: " << msg_total<< endl;
    client.unsubscribe(hostName, port, "outTables","actionTest");
    usedPorts.push_back(listenport);
    
    EXPECT_EQ(msg_total, 1000);

}

TEST_F(StreamingPollingClientTester,test_unsubscribe_hostNull){
    createSharedTableAndReplay2();
	int msg_total= 0;
	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };
    PollingClient client(listenport);
    auto queue = client.subscribe(hostName, port,"outTables", "actionTest", 0);

    Util::sleep(2000);
    vector<Message> msgs;
    while(queue->pop(msgs,1000)) {
        for (auto &msg : msgs) {
            msg_total+= 1;
            // cout << msg->getString() << endl;            
            // handle msg
        }
    }
    cout << "total size: " << msg_total<< endl;
    EXPECT_ANY_THROW(client.unsubscribe("", port, "outTables","actionTest"));
    usedPorts.push_back(listenport);

    client.unsubscribe(hostName, port, "outTables","actionTest");
}

TEST_F(StreamingPollingClientTester,test_unsubscribe_portNull){
    createSharedTableAndReplay2();
	int msg_total= 0;
	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };
    PollingClient client(listenport);
    auto queue = client.subscribe(hostName, port,"outTables", "actionTest", 0);

    Util::sleep(2000);
    vector<Message> msgs;
    while(queue->pop(msgs,1000)) {
        for (auto &msg : msgs) {
            msg_total+= 1;
            // cout << msg->getString() << endl;            
            // handle msg
        }
    }
    cout << "total size: " << msg_total<< endl;
    EXPECT_ANY_THROW(client.unsubscribe(hostName, NULL, "outTables","actionTest"));
    usedPorts.push_back(listenport);

    client.unsubscribe(hostName, port, "outTables","actionTest");
}

TEST_F(StreamingPollingClientTester,test_unsubscribe_tableNameNull){
    createSharedTableAndReplay2();
	int msg_total= 0;
	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };
    PollingClient client(listenport);
    auto queue = client.subscribe(hostName, port,"outTables", "actionTest", 0);

    Util::sleep(2000);
    vector<Message> msgs;
    while(queue->pop(msgs,1000)) {
        for (auto &msg : msgs) {
            msg_total+= 1;
            // cout << msg->getString() << endl;            
            // handle msg
        }
    }
    cout << "total size: " << msg_total<< endl;
    EXPECT_ANY_THROW(client.unsubscribe(hostName, port, "","actionTest"));
    usedPorts.push_back(listenport);

    client.unsubscribe(hostName, port, "outTables","actionTest");
}

TEST_F(StreamingPollingClientTester,test_unsubscribe_actionNameNull){
    createSharedTableAndReplay2();
	int msg_total= 0;
	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };
    PollingClient client(listenport);
    auto queue = client.subscribe(hostName, port,"outTables", "actionTest", 0);

    Util::sleep(2000);
    vector<Message> msgs;
    while(queue->pop(msgs,1000)) {
        for (auto &msg : msgs) {
            msg_total+= 1;
            // cout << msg->getString() << endl;            
            // handle msg
        }
    }
    cout << "total size: " << msg_total<< endl;
    client.unsubscribe(hostName, port, "outTables","");
    usedPorts.push_back(listenport);

    TableSP stat = conn.run("getStreamingStat().pubTables");
    EXPECT_EQ(stat->getColumn(0)->getRow(0)->getString(),"outTables");
    EXPECT_EQ(stat->getColumn(2)->getRow(0)->getInt(),1000);
    EXPECT_EQ(stat->getColumn(3)->getRow(0)->getString(),"actionTest");
    client.unsubscribe(hostName, port, "outTables","actionTest");
}

TEST_F(StreamingPollingClientTester,test_subscribe_twice){
    createSharedTableAndReplay2();
	int msg_total= 0;
	srand(time(0));

    int listenport = rand() % 13000 + 2000;
    if (find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };
    PollingClient client(listenport);
    auto queue = client.subscribe(hostName, port,"outTables", "actionTest", 0);

    Util::sleep(2000);
    vector<Message> msgs;
    while(queue->pop(msgs,1000)) {
        for (auto &msg : msgs) {
            msg_total+= 1;
            // cout << msg->getString() << endl;            
            // handle msg
        }
    }
    cout << "total size: " << msg_total<< endl;
    auto queue1 = client.subscribe(hostName, port,"outTables", "actionTest", 0);

    client.unsubscribe(hostName, port, "outTables","actionTest");
    usedPorts.push_back(listenport);
    
    EXPECT_EQ(msg_total, 1000);

}