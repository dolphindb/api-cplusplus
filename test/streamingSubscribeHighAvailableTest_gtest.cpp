class streamingSubscribeHighAvailableTest:public testing::Test
{
protected:
    //Suite
    static void SetUpTestCase() {
        //DBConnection conn;

		conn.initialize();
        bool ret = conn.connect(hostName, port, "admin", "123456","",true,sites);
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
            string createHAstreamTable="try{dropStreamTable(`tradesHA);}catch(ex){};\
                                    colNames = `timestamp`sym`qty`price\n\
                                    colTypes = [TIMESTAMP,SYMBOL,INT,DOUBLE]\n\
                                    t=table(100000:0,colNames,colTypes)\n\
                                    haStreamTable("+raftsGroup+",t,`tradesHA,100000);";
            conn.run(createHAstreamTable);
		}
    }
    virtual void TearDown()
    {
        pass;
    }
};

string getStreamRaftLeader(){
    return conn.run("getStreamingLeader(getStreamingRaftGroups()[0][\"id\"])")->getString();
}

pair<string,int> getFollowerInfo(string cur_NodeName){
    string Host;
    int Port;
    for (int i=0;i<sites.size();i++){
        string nodeName = sites[i].substr(18,25);
        if(nodeName != getStreamRaftLeader()){
            Host=sites[i].substr(0,12);
            Port=atoi(sites[i].substr(13).substr(0,4).c_str());
            break;
        }
    }
    return {Host,Port};
}

pair<string,int> getLeaderInfo(string cur_NodeName){
    string Host;
    int Port;
    for (int i=0;i<sites.size();i++){
        string nodeName = sites[i].substr(18,25);
        if(nodeName == getStreamRaftLeader()){
            Host=sites[i].substr(0,12);
            Port=atoi(sites[i].substr(13).substr(0,4).c_str());
            break;
        }
    }
    return {Host,Port};
}

void insert_task(string host, int port, int totalInsertNum){
    DBConnection conn_1;
    conn_1.connect(host,port,"admin","123456","",true,sites);
    cout<<"insert datas to HAstreamtable..."<<endl;
    conn_1.run("for (i in 1.."+to_string(totalInsertNum)+"){tableInsert(`tradesHA,rand(timestamp(10000)..timestamp(20000),1),rand(`a`b`c`d,1),rand(1000,1),rand(100.00,1));sleep(1000)}");
    cout<<"insert finished!"<<endl;
    Util::sleep(1000);
    conn_1.close();
}

static int insert1_total=10; //total rows you want to insert and test.

TEST_F(streamingSubscribeHighAvailableTest,test_Threadclient_onehandler_subscribestreamTableHA_onFollower){
    int msg1_total=0;
    srand(time(0));

    string cur_NodeName=conn.run("getNodeAlias()")->getString();
    string followerHost=getFollowerInfo(cur_NodeName).first;
    int followerPort=getFollowerInfo(cur_NodeName).second;

    cout<<"subscribe the streamTable on "<<followerHost+":"+to_string(followerPort)<<endl;
    thread th1=thread(insert_task,followerHost,followerPort,insert1_total);

    int listenport = rand() % 13000 + 2000;
    while(find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };
	Signal notify;
    Mutex mutex;

    auto onehandler = [&](Message msg) {
        // cout<<msg->get(0)->getString()+","+msg->get(1)->getString()+","+msg->get(2)->getString()+","+msg->get(3)->getString()<<endl;
        LockGuard<Mutex> lock(&mutex);
        msg1_total+=1;

        if (msg1_total == insert1_total) {
            cout<<"get all msg secceed!"<<endl;
            notify.set();
        }
    };

    ThreadedClient threadedClient(listenport);
    auto thread1 = threadedClient.subscribe(followerHost, followerPort, onehandler, "tradesHA", "test_streamHA", 0, true,nullptr, false, false, "admin", "123456");
    thread1->setAffinity(0);
    this_thread::sleep_for(chrono::seconds(1));
    notify.wait();
    th1.join();

    threadedClient.unsubscribe(followerHost, followerPort, "tradesHA", "test_streamHA");

    EXPECT_EQ(msg1_total, insert1_total);
    usedPorts.push_back(listenport);
}

TEST_F(streamingSubscribeHighAvailableTest,test_Threadclient_batchhandler_subscribestreamTableHA_onFollower){
    int msg1_total=0;
    srand(time(0));

    string cur_NodeName=conn.run("getNodeAlias()")->getString();
    string followerHost=getFollowerInfo(cur_NodeName).first;
    int followerPort=getFollowerInfo(cur_NodeName).second;

    cout<<"subscribe the streamTable on "<<followerHost+":"+to_string(followerPort)<<endl;
    thread th1=thread(insert_task,followerHost,followerPort,insert1_total);

    int listenport = rand() % 13000 + 2000;
    while(find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };
	Signal notify;
    Mutex mutex;

    auto batchhandler = [&](vector<Message> msgs) {
        LockGuard<Mutex> lock(&mutex);
        for (auto &msg : msgs) {
            LockGuard<Mutex> lock(&mutex);
            msg1_total+=1;

            if (msg1_total == insert1_total) {
                cout<<"get all msg secceed!"<<endl;
                notify.set();
            }
        }
    };

    ThreadedClient threadedClient(listenport);
    auto thread1 = threadedClient.subscribe(followerHost, followerPort, batchhandler, "tradesHA", "test_streamHA",0,true, nullptr, true, 1,1.0,false,"admin", "123456");
    thread1->setAffinity(0);
    this_thread::sleep_for(chrono::seconds(1));
    notify.wait();
    th1.join();

    threadedClient.unsubscribe(followerHost, followerPort, "tradesHA", "test_streamHA");

    EXPECT_EQ(msg1_total, insert1_total);
    usedPorts.push_back(listenport);
}

// TEST_F(streamingSubscribeHighAvailableTest,test_Pollingclient_subscribestreamTableHA_onFollower){
//     int msg1_total=0;
//     srand(time(0));

//     string cur_NodeName=conn.run("getNodeAlias()")->getString();
//     string followerHost=getFollowerInfo(cur_NodeName).first;
//     int followerPort=getFollowerInfo(cur_NodeName).second;

//     cout<<"subscribe the streamTable on "<<followerHost+":"+to_string(followerPort)<<endl;
//     thread th1=thread(insert_task,followerHost,followerPort,insert1_total);

//     int listenport = rand() % 13000 + 2000;
//     while(find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
//         listenport = rand() % 13000 + 2000;
//     };
// 	Signal notify;
//     Mutex mutex;
//     PollingClient client(listenport);
//     auto queue = client.subscribe(followerHost, followerPort,"tradesHA", "test_streamHA", 0, true,nullptr, false,false, "admin","123456");

//     vector<Message> msgs;
//     while(queue->pop(msgs,1000)) {
//         for (auto &msg : msgs) {
//             msg1_total+=1;
//             cout<<msg1_total<<endl;
//             cout<<msg->getString()<<endl;
//             if (msg1_total == insert1_total) {
//                 cout<<"get all msg secceed!"<<endl;
//                 notify.set();
//             }
//         }
//     };

//     this_thread::sleep_for(chrono::seconds(1));
//     notify.wait();
//     th1.join();

//     // client.unsubscribe(hostName, port, "tradesHA", "tradesHA");
//     client.unsubscribe(followerHost, 9004, "tradesHA", "tradesHA");

//     EXPECT_EQ(msg1_total, insert1_total);
//     usedPorts.push_back(listenport);
// }

TEST_F(streamingSubscribeHighAvailableTest,test_Threadpooledclient_threadCount_1_subscribestreamTableHA_onFollower){
    int msg1_total=0;
    srand(time(0));

    string cur_NodeName=conn.run("getNodeAlias()")->getString();
    string followerHost=getFollowerInfo(cur_NodeName).first;
    int followerPort=getFollowerInfo(cur_NodeName).second;

    cout<<"subscribe the streamTable on "<<followerHost+":"+to_string(followerPort)<<endl;
    thread th1=thread(insert_task,followerHost,followerPort,insert1_total);

    int listenport = rand() % 13000 + 2000;
    while(find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };
	Signal notify;
    Mutex mutex;

    auto onehandler = [&](Message msg) {
        // cout<<msg->get(0)->getString()+","+msg->get(1)->getString()+","+msg->get(2)->getString()+","+msg->get(3)->getString()<<endl;
        LockGuard<Mutex> lock(&mutex);
        msg1_total+=1;

        if (msg1_total == insert1_total) {
            cout<<"get all msg secceed!"<<endl;
            notify.set();
        }
    };

    ThreadPooledClient client(listenport, 1);
    auto threadVec = client.subscribe(followerHost, followerPort, onehandler, "tradesHA", "test_streamHA", 0, true,nullptr, false,false, "admin","123456");
    this_thread::sleep_for(chrono::seconds(1));
    notify.wait();
    th1.join();

    client.unsubscribe(followerHost, followerPort, "tradesHA", "test_streamHA");

    EXPECT_EQ(msg1_total, insert1_total);
    usedPorts.push_back(listenport);
}


TEST_F(streamingSubscribeHighAvailableTest,test_Threadpooledclient_threadCount_2_subscribestreamTableHA_onFollower){
    int msg1_total=0;
    srand(time(0));

    string cur_NodeName=conn.run("getNodeAlias()")->getString();
    string followerHost=getFollowerInfo(cur_NodeName).first;
    int followerPort=getFollowerInfo(cur_NodeName).second;

    cout<<"subscribe the streamTable on "<<followerHost+":"+to_string(followerPort)<<endl;
    thread th1=thread(insert_task,followerHost,followerPort,insert1_total);

    int listenport = rand() % 13000 + 2000;
    while(find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };
	Signal notify;
    Mutex mutex;

    auto onehandler = [&](Message msg) {
        // cout<<msg->get(0)->getString()+","+msg->get(1)->getString()+","+msg->get(2)->getString()+","+msg->get(3)->getString()<<endl;
        LockGuard<Mutex> lock(&mutex);
        msg1_total+=1;

        if (msg1_total == insert1_total) {
            cout<<"get all msg secceed!"<<endl;
            notify.set();
        }
    };

    ThreadPooledClient client(listenport, 2);
    auto threadVec = client.subscribe(followerHost, followerPort, onehandler, "tradesHA", "test_streamHA", 0, true,nullptr, false,false, "admin","123456");
    this_thread::sleep_for(chrono::seconds(1));
    notify.wait();
    th1.join();

    client.unsubscribe(followerHost, followerPort, "tradesHA", "test_streamHA");

    EXPECT_EQ(msg1_total, insert1_total);
    usedPorts.push_back(listenport);
}

TEST_F(streamingSubscribeHighAvailableTest,test_Threadclient_onehandler_subscribestreamTableHA_onLeader){
    int msg1_total=0;
    srand(time(0));

    string cur_NodeName=conn.run("getNodeAlias()")->getString();
    string leaderHost=getLeaderInfo(cur_NodeName).first;
    int leaderPort=getLeaderInfo(cur_NodeName).second;

    cout<<"subscribe the streamTable on "<<leaderHost+":"+to_string(leaderPort)<<endl;
    thread th1=thread(insert_task,leaderHost,leaderPort,insert1_total);

    int listenport = rand() % 13000 + 2000;
    while(find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };
	Signal notify;
    Mutex mutex;

    auto onehandler = [&](Message msg) {
        // cout<<msg->get(0)->getString()+","+msg->get(1)->getString()+","+msg->get(2)->getString()+","+msg->get(3)->getString()<<endl;
        LockGuard<Mutex> lock(&mutex);
        msg1_total+=1;

        if (msg1_total == insert1_total) {
            cout<<"get all msg secceed!"<<endl;
            notify.set();
        }
    };

    ThreadedClient threadedClient(listenport);
    auto thread1 = threadedClient.subscribe(leaderHost, leaderPort, onehandler, "tradesHA", "test_streamHA", 0, true,nullptr, false, false, "admin", "123456");
    thread1->setAffinity(0);
    this_thread::sleep_for(chrono::seconds(1));
    notify.wait();
    th1.join();

    threadedClient.unsubscribe(leaderHost, leaderPort, "tradesHA", "test_streamHA");

    EXPECT_EQ(msg1_total, insert1_total);
    usedPorts.push_back(listenport);
}

TEST_F(streamingSubscribeHighAvailableTest,test_Threadclient_batchhandler_subscribestreamTableHA_onLeader){
    int msg1_total=0;
    srand(time(0));

    string cur_NodeName=conn.run("getNodeAlias()")->getString();
    string leaderHost=getLeaderInfo(cur_NodeName).first;
    int leaderPort=getLeaderInfo(cur_NodeName).second;

    cout<<"subscribe the streamTable on "<<leaderHost+":"+to_string(leaderPort)<<endl;
    thread th1=thread(insert_task,leaderHost,leaderPort,insert1_total);

    int listenport = rand() % 13000 + 2000;
    while(find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };
	Signal notify;
    Mutex mutex;

    auto batchhandler = [&](vector<Message> msgs) {
        LockGuard<Mutex> lock(&mutex);
        for (auto &msg : msgs) {
            LockGuard<Mutex> lock(&mutex);
            msg1_total+=1;

            if (msg1_total == insert1_total) {
                cout<<"get all msg secceed!"<<endl;
                notify.set();
            }
        }
    };

    ThreadedClient threadedClient(listenport);
    auto thread1 = threadedClient.subscribe(leaderHost, leaderPort, batchhandler, "tradesHA", "test_streamHA",0,true, nullptr, true, 1,1.0,false,"admin", "123456");
    thread1->setAffinity(0);
    this_thread::sleep_for(chrono::seconds(1));
    notify.wait();
    th1.join();

    threadedClient.unsubscribe(leaderHost, leaderPort, "tradesHA", "test_streamHA");

    EXPECT_EQ(msg1_total, insert1_total);
    usedPorts.push_back(listenport);
}

// TEST_F(streamingSubscribeHighAvailableTest,test_Pollingclient_subscribestreamTableHA_onLeader){
//     int msg1_total=0;
//     srand(time(0));

//     string cur_NodeName=conn.run("getNodeAlias()")->getString();
//     string leaderHost=getLeaderInfo(cur_NodeName).first;
//     int leaderPort=getLeaderInfo(cur_NodeName).second;

//     cout<<"subscribe the streamTable on "<<leaderHost+":"+to_string(leaderPort)<<endl;
//     thread th1=thread(insert_task,leaderHost,leaderPort,insert1_total);

//     int listenport = rand() % 13000 + 2000;
//     while(find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
//         listenport = rand() % 13000 + 2000;
//     };
// 	Signal notify;
//     Mutex mutex;
//     PollingClient client(listenport);
//     auto queue = client.subscribe(leaderHost, leaderPort,"tradesHA", "test_streamHA", 0, true,nullptr, false,false, "admin","123456");

//     vector<Message> msgs;
//     while(queue->pop(msgs,1000)) {
//         for (auto &msg : msgs) {
//             msg1_total+=1;
//             cout<<msg1_total<<endl;
//             cout<<msg->getString()<<endl;
//             if (msg1_total == insert1_total) {
//                 cout<<"get all msg secceed!"<<endl;
//                 notify.set();
//             }
//         }
//     };

//     this_thread::sleep_for(chrono::seconds(1));
//     notify.wait();
//     th1.join();

//     client.unsubscribe(leaderHost, leaderPort, "tradesHA", "tradesHA");

//     EXPECT_EQ(msg1_total, insert1_total);
//     usedPorts.push_back(listenport);
// }

TEST_F(streamingSubscribeHighAvailableTest,test_Threadpooledclient_threadCount_1_subscribestreamTableHA_onLeader){
    int msg1_total=0;
    srand(time(0));

    string cur_NodeName=conn.run("getNodeAlias()")->getString();
    string leaderHost=getLeaderInfo(cur_NodeName).first;
    int leaderPort=getLeaderInfo(cur_NodeName).second;

    cout<<"subscribe the streamTable on "<<leaderHost+":"+to_string(leaderPort)<<endl;
    thread th1=thread(insert_task,leaderHost,leaderPort,insert1_total);

    int listenport = rand() % 13000 + 2000;
    while(find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };
	Signal notify;
    Mutex mutex;

    auto onehandler = [&](Message msg) {
        // cout<<msg->get(0)->getString()+","+msg->get(1)->getString()+","+msg->get(2)->getString()+","+msg->get(3)->getString()<<endl;
        LockGuard<Mutex> lock(&mutex);
        msg1_total+=1;

        if (msg1_total == insert1_total) {
            cout<<"get all msg secceed!"<<endl;
            notify.set();
        }
    };

    ThreadPooledClient client(listenport, 1);
    auto threadVec = client.subscribe(leaderHost, leaderPort, onehandler, "tradesHA", "test_streamHA", 0, true,nullptr, false,false, "admin","123456");
    this_thread::sleep_for(chrono::seconds(1));
    notify.wait();
    th1.join();

    client.unsubscribe(leaderHost, leaderPort, "tradesHA", "test_streamHA");

    EXPECT_EQ(msg1_total, insert1_total);
    usedPorts.push_back(listenport);
}


TEST_F(streamingSubscribeHighAvailableTest,test_Threadpooledclient_threadCount_2_subscribestreamTableHA_onLeader){
    int msg1_total=0;
    srand(time(0));

    string cur_NodeName=conn.run("getNodeAlias()")->getString();
    string leaderHost=getLeaderInfo(cur_NodeName).first;
    int leaderPort=getLeaderInfo(cur_NodeName).second;

    cout<<"subscribe the streamTable on "<<leaderHost+":"+to_string(leaderPort)<<endl;
    thread th1=thread(insert_task,leaderHost,leaderPort,insert1_total);

    int listenport = rand() % 13000 + 2000;
    while(find(usedPorts.begin(),usedPorts.end(),listenport) != usedPorts.end()){
        listenport = rand() % 13000 + 2000;
    };
	Signal notify;
    Mutex mutex;

    auto onehandler = [&](Message msg) {
        // cout<<msg->get(0)->getString()+","+msg->get(1)->getString()+","+msg->get(2)->getString()+","+msg->get(3)->getString()<<endl;
        LockGuard<Mutex> lock(&mutex);
        msg1_total+=1;

        if (msg1_total == insert1_total) {
            cout<<"get all msg secceed!"<<endl;
            notify.set();
        }
    };

    ThreadPooledClient client(listenport, 2);
    auto threadVec = client.subscribe(leaderHost, leaderPort, onehandler, "tradesHA", "test_streamHA", 0, true,nullptr, false,false, "admin","123456");
    this_thread::sleep_for(chrono::seconds(1));
    notify.wait();
    th1.join();

    client.unsubscribe(leaderHost, leaderPort, "tradesHA", "test_streamHA");

    EXPECT_EQ(msg1_total, insert1_total);
    usedPorts.push_back(listenport);
}