class StreamingSubscribeHighAvailableTest : public testing::Test, public ::testing::WithParamInterface<int>
{
public:
    // Suite
    static void SetUpTestCase()
    {
        // DBConnection conn;

        conn.initialize();
        bool ret = conn.connect(hostName, port, "admin", "123456", "", true, sites);
        if (!ret)
        {
            cout << "Failed to connect to the server" << endl;
        }
        else
        {
            cout << "connect to " + hostName + ":" + std::to_string(port) << endl;
            isNewVersion = conn.run("flag = 1;v = split(version(), ' ')[0];\
                            tmp=int(v.split('.'));\
                            if((tmp[0]==2 && tmp[1]==00 && tmp[2]>=9 )||(tmp[0]==2 && tmp[1]==10)){flag=1;}else{flag=0};\
                            flag")
                               ->getBool();
        }
    }
    static void TearDownTestCase()
    {
        usedPorts.clear();
        conn.close();
    }

    // Case
    virtual void SetUp()
    {
        cout << "check connect...";
        ConstantSP res = conn.run("1+1");
        if (!(res->getBool()))
        {
            cout << "Server not responed, please check." << endl;
        }
        else
        {
            cout << "ok" << endl;
            Util::sleep(2000);
            string createHAstreamTable = "try{dropStreamTable(`tradesHA);}catch(ex){};go;\
                                    colNames = `timestamp`sym`qty`price;\
                                    colTypes = [TIMESTAMP,SYMBOL,INT,DOUBLE];\
                                    t=table(100:0,colNames,colTypes);\
                                    haStreamTable(" +
                                         raftsGroup + ",t,`tradesHA,100000);sleep(1000);";
            conn.run(createHAstreamTable);
        }
    }
    virtual void TearDown()
    {
        conn.run("undef all;");
    }
};

string getStreamRaftLeader()
{
    return conn.run("getStreamingLeader(getStreamingRaftGroups()[0][\"id\"])")->getString();
}

vector<string> split(const string &s, const string &seperator)
{
    vector<string> result;
    typedef string::size_type string_size;
    string_size i = 0;

    while (i != s.size())
    {
        // 找到字符串中首个不等于分隔符的字母；
        int flag = 0;
        while (i != s.size() && flag == 0)
        {
            flag = 1;
            for (string_size x = 0; x < seperator.size(); ++x)
                if (s[i] == seperator[x])
                {
                    ++i;
                    flag = 0;
                    break;
                }
        }

        // 找到又一个分隔符，将两个分隔符之间的字符串取出；
        flag = 0;
        string_size j = i;
        while (j != s.size() && flag == 0)
        {
            for (string_size x = 0; x < seperator.size(); ++x)
                if (s[j] == seperator[x])
                {
                    flag = 1;
                    break;
                }
            if (flag == 0)
                ++j;
        }
        if (i != j)
        {
            result.push_back(s.substr(i, j - i));
            i = j;
        }
    }
    return result;
}

pair<string, int> getFollowerInfo()
{
    string Host;
    int Port;
    for (int i = 0; i < sites.size(); i++)
    {
        string nodeName = split(sites[i], ":")[2];
        if (nodeName != getStreamRaftLeader())
        {
            Host = split(sites[i], ":")[0];
            Port = atoi(split(sites[i], ":")[1].c_str());
            break;
        }
    }
    return {Host, Port};
}

pair<string, int> getLeaderInfo()
{
    string Host;
    int Port;
    for (int i = 0; i < sites.size(); i++)
    {
        string nodeName = split(sites[i], ":")[2];
        if (nodeName == getStreamRaftLeader())
        {
            Host = split(sites[i], ":")[0];
            Port = atoi(split(sites[i], ":")[1].c_str());
            break;
        }
    }
    return {Host, Port};
}

void insert_task(string host, int port, int totalInsertNum)
{
    DBConnection conn_1;
    conn_1.connect(host, port, "admin", "123456", "", true, sites);
    cout << "insert datas to HAstreamtable..." << endl;
    conn_1.run("for (i in 1.." + to_string(totalInsertNum) + "){tableInsert(`tradesHA,rand(timestamp(10000)..timestamp(20000),1),rand(`a`b`c`d,1),rand(1000,1),rand(100.00,1));}");
    cout << "insert finished!" << endl;
    Util::sleep(1000);
    conn_1.close();
}

static int insert_total_rows = 2000; // total rows you want to insert and test.
INSTANTIATE_TEST_CASE_P(StreamingReverse, StreamingSubscribeHighAvailableTest, testing::Values(0, rand() % 1000 + 13000));
TEST_P(StreamingSubscribeHighAvailableTest, test_Threadclient_onehandler_subscribeStreamTableHA_onFollower)
{
    int msg1_total = 0;
    srand(time(0));

    string cur_NodeName = conn.run("getNodeAlias()")->getString();
    string followerHost = getFollowerInfo().first, leaderHost = getLeaderInfo().first;
    int followerPort = getFollowerInfo().second, leaderPort = getLeaderInfo().second;

    DBConnection conn_leader(false, false);
    bool res = conn_leader.connect(leaderHost, leaderPort, "admin", "123456");

    cout << "subscribe the streamTable on " << followerHost + ":" + to_string(followerPort) << endl;

    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;

    auto onehandler = [&](Message msg)
    {
        // cout<<msg->get(0)->getString()+","+msg->get(1)->getString()+","+msg->get(2)->getString()+","+msg->get(3)->getString()<<endl;
        LockGuard<Mutex> lock(&mutex);
        msg1_total += 1;

        if (msg1_total == insert_total_rows)
        {
            cout << "get all msg successfully!" << endl;
            notify.set();
        }
    };

    ThreadedClient threadedClient(listenport);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(threadedClient.subscribe(followerHost, followerPort, onehandler, "tradesHA", "test_streamHA", 0, true, nullptr, false, false, "admin", "123456"));
    }
    else
    {
        thread th1 = thread(insert_task, followerHost, followerPort, insert_total_rows);
        auto thread1 = threadedClient.subscribe(followerHost, followerPort, onehandler, "tradesHA", "test_streamHA", 0, true, nullptr, false, false, "admin", "123456");
        thread1->setAffinity(0);

        notify.wait();
        th1.join();

        // AC-194: support unsubscribe on follower.
        threadedClient.unsubscribe(followerHost, followerPort, "tradesHA", "test_streamHA");
        Util::sleep(1000);

        // auto switch to leader DataNode when subscribing on follower, so assert no subscription on leader
        EXPECT_TRUE(conn_leader.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`tradesHA) ==0")->getBool());
        EXPECT_EQ(msg1_total, insert_total_rows);
    }

    usedPorts.push_back(listenport);
    conn_leader.close();
}

TEST_P(StreamingSubscribeHighAvailableTest, test_Threadclient_onehandler_subscribeStreamTableHAbeforeTableCreated_onFollower)
{
    string s1 = "try{dropStreamTable(`tradesHA);}catch(ex){};go;";
    conn.run(s1);
    int msg1_total = 0;
    srand(time(0));

    string cur_NodeName = conn.run("getNodeAlias()")->getString();
    string followerHost = getFollowerInfo().first, leaderHost = getLeaderInfo().first;
    int followerPort = getFollowerInfo().second, leaderPort = getLeaderInfo().second;

    DBConnection conn_leader(false, false);
    bool res = conn_leader.connect(leaderHost, leaderPort, "admin", "123456");

    cout << "subscribe the streamTable on " << followerHost + ":" + to_string(followerPort) << endl;

    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;

    auto onehandler = [&](Message msg)
    {
        // cout<<msg->get(0)->getString()+","+msg->get(1)->getString()+","+msg->get(2)->getString()+","+msg->get(3)->getString()<<endl;
        LockGuard<Mutex> lock(&mutex);
        msg1_total += 1;

        if (msg1_total == insert_total_rows)
        {
            cout << "get all msg successfully!" << endl;
            notify.set();
        }
    };

    ThreadedClient threadedClient(listenport);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(threadedClient.subscribe(followerHost, followerPort, onehandler, "tradesHA", "test_streamHA", 0, true, nullptr, false, false, "admin", "123456"));
    }
    else
    {
        thread th1 = thread([&]
                            {
        DBConnection conn_1;
        conn_1.connect(hostName, port, "admin", "123456", "", true, sites);
        cout << "insert datas to HAstreamtable..." << endl;
        conn_1.run("colNames = `timestamp`sym`qty`price;\
                    colTypes = [TIMESTAMP,SYMBOL,INT,DOUBLE];\
                    t=table(100:0,colNames,colTypes);\
                    haStreamTable(" +
                    raftsGroup + ",t,`tradesHA,100000);sleep(1000);go");
        conn_1.run("for (i in 1.." + to_string(insert_total_rows) + "){tableInsert(`tradesHA,rand(timestamp(10000)..timestamp(20000),1),rand(`a`b`c`d,1),rand(1000,1),rand(100.00,1));}");
        cout << "insert finished!" << endl;
        Util::sleep(1000);
        conn_1.close(); });
        auto thread1 = threadedClient.subscribe(followerHost, followerPort, onehandler, "tradesHA", "test_streamHA", 0, true, nullptr, false, false, "admin", "123456");
        thread1->setAffinity(0);

        notify.wait();
        th1.join();

        // AC-194: support unsubscribe on follower.
        threadedClient.unsubscribe(followerHost, followerPort, "tradesHA", "test_streamHA");
        Util::sleep(1000);

        // auto switch to leader DataNode when subscribing on follower, so assert no subscription on leader
        EXPECT_TRUE(conn_leader.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`tradesHA) ==0")->getBool());
        EXPECT_EQ(msg1_total, insert_total_rows);
    }

    usedPorts.push_back(listenport);
    conn_leader.close();
}

TEST_P(StreamingSubscribeHighAvailableTest, test_Threadclient_batchhandler_subscribeStreamTableHA_onFollower)
{
    int msg1_total = 0;
    srand(time(0));

    string cur_NodeName = conn.run("getNodeAlias()")->getString();
    string followerHost = getFollowerInfo().first, leaderHost = getLeaderInfo().first;
    int followerPort = getFollowerInfo().second, leaderPort = getLeaderInfo().second;

    DBConnection conn_leader(false, false);
    bool res = conn_leader.connect(leaderHost, leaderPort, "admin", "123456");

    cout << "subscribe the streamTable on " << followerHost + ":" + to_string(followerPort) << endl;

    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;

    auto batchhandler = [&](vector<Message> msgs)
    {
        LockGuard<Mutex> lock(&mutex);
        for (auto &msg : msgs)
        {
            LockGuard<Mutex> lock(&mutex);
            msg1_total += 1;

            if (msg1_total == insert_total_rows)
            {
                cout << "get all msg successfully!" << endl;
                notify.set();
            }
        }
    };

    ThreadedClient threadedClient(listenport);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(threadedClient.subscribe(followerHost, followerPort, batchhandler, "tradesHA", "test_streamHA", 0, true, nullptr, true, 1, 1.0, false, "admin", "123456"));
    }
    else
    {
        thread th1 = thread(insert_task, followerHost, followerPort, insert_total_rows);
        auto thread1 = threadedClient.subscribe(followerHost, followerPort, batchhandler, "tradesHA", "test_streamHA", 0, true, nullptr, true, 1, 1.0, false, "admin", "123456");
        thread1->setAffinity(0);

        notify.wait();
        th1.join();

        // AC-194: support unsubscribe on follower.
        threadedClient.unsubscribe(followerHost, followerPort, "tradesHA", "test_streamHA");
        Util::sleep(1000);

        // auto switch to leader DataNode when subscribing on follower, so assert no subscription on leader
        EXPECT_TRUE(conn_leader.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`tradesHA) ==0")->getBool());
        EXPECT_EQ(msg1_total, insert_total_rows);
    }

    usedPorts.push_back(listenport);
    conn_leader.close();
}

TEST_P(StreamingSubscribeHighAvailableTest, test_Pollingclient_subscribeStreamTableHA_onFollower)
{
    int msg1_total = 0;
    srand(time(0));

    string cur_NodeName = conn.run("getNodeAlias()")->getString();
    string followerHost = getFollowerInfo().first, leaderHost = getLeaderInfo().first;
    int followerPort = getFollowerInfo().second, leaderPort = getLeaderInfo().second;

    DBConnection conn_leader(false, false);
    bool res = conn_leader.connect(leaderHost, leaderPort, "admin", "123456");

    cout << "subscribe the streamTable on " << followerHost + ":" + to_string(followerPort) << endl;

    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    PollingClient client(listenport);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(followerHost, followerPort, "tradesHA", "test_streamHA", 0, true, nullptr, false, false, "admin", "123456"));
    }
    else
    {
        thread th1 = thread(insert_task, followerHost, followerPort, insert_total_rows);
        auto queue = client.subscribe(followerHost, followerPort, "tradesHA", "test_streamHA", 0, true, nullptr, false, false, "admin", "123456");
        Message msg;
        ThreadSP th2 = new Thread(new Executor([&]
                                               {
        while (true)
        {
            queue->pop(msg);
            if(msg.isNull()) {break;}
            else{
                msg1_total += 1;
                // cout << msg1_total << endl;
                // cout << msg->getString() << endl;
                if (msg1_total == insert_total_rows)
                {
                    cout << "get all msg successfully!" << endl;
                }
            }
        } }));

        th1.join();
        Util::sleep(1000);
        // AC-194: support unsubscribe on follower.
        client.unsubscribe(followerHost, followerPort, "tradesHA", "test_streamHA");
        th2->start();
        th2->join();
        Util::sleep(100);
        // auto switch to leader DataNode when subscribing on follower, so assert no subscription on leader
        EXPECT_TRUE(conn_leader.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`tradesHA) ==0")->getBool());
        EXPECT_EQ(msg1_total, insert_total_rows);
    }

    usedPorts.push_back(listenport);
    conn_leader.close();
}

TEST_P(StreamingSubscribeHighAvailableTest, test_Pollingclient_subscribeStreamTableHAbeforeTableCreated_onFollower)
{
    string s1 = "try{dropStreamTable(`tradesHA);}catch(ex){};go;";
    conn.run(s1);
    int msg1_total = 0;
    srand(time(0));

    string cur_NodeName = conn.run("getNodeAlias()")->getString();
    string followerHost = getFollowerInfo().first, leaderHost = getLeaderInfo().first;
    int followerPort = getFollowerInfo().second, leaderPort = getLeaderInfo().second;

    DBConnection conn_leader(false, false);
    bool res = conn_leader.connect(leaderHost, leaderPort, "admin", "123456");

    cout << "subscribe the streamTable on " << followerHost + ":" + to_string(followerPort) << endl;

    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    PollingClient client(listenport);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(followerHost, followerPort, "tradesHA", "test_streamHA", 0, true, nullptr, false, false, "admin", "123456"));
    }
    else
    {
        thread th1 = thread([&]
                            {
        DBConnection conn_1;
        conn_1.connect(hostName, port, "admin", "123456", "", true, sites);
        cout << "insert datas to HAstreamtable..." << endl;
        conn_1.run("colNames = `timestamp`sym`qty`price;\
                    colTypes = [TIMESTAMP,SYMBOL,INT,DOUBLE];\
                    t=table(100:0,colNames,colTypes);\
                    haStreamTable(" +
                    raftsGroup + ",t,`tradesHA,100000);sleep(1000);go");
        conn_1.run("for (i in 1.." + to_string(insert_total_rows) + "){tableInsert(`tradesHA,rand(timestamp(10000)..timestamp(20000),1),rand(`a`b`c`d,1),rand(1000,1),rand(100.00,1));}");
        cout << "insert finished!" << endl;
        Util::sleep(1000);
        conn_1.close(); });
        auto queue = client.subscribe(followerHost, followerPort, "tradesHA", "test_streamHA", 0, true, nullptr, false, false, "admin", "123456");
        Message msg;
        ThreadSP th2 = new Thread(new Executor([&]
                                               {
        while (true)
        {
            queue->pop(msg);
            if(msg.isNull()) {break;}
            else{
                msg1_total += 1;
                // cout << msg1_total << endl;
                // cout << msg->getString() << endl;
                if (msg1_total == insert_total_rows)
                {
                    cout << "get all msg successfully!" << endl;
                }
            }
        } }));

        th1.join();
        Util::sleep(1000);
        // AC-194: support unsubscribe on follower.
        client.unsubscribe(followerHost, followerPort, "tradesHA", "test_streamHA");
        th2->start();
        th2->join();
        Util::sleep(100);

        // auto switch to leader DataNode when subscribing on follower, so assert no subscription on leader
        EXPECT_TRUE(conn_leader.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`tradesHA) ==0")->getBool());
        EXPECT_EQ(msg1_total, insert_total_rows);
    }

    usedPorts.push_back(listenport);
    conn_leader.close();
}

TEST_P(StreamingSubscribeHighAvailableTest, test_Threadpooledclient_threadCount_1_subscribeStreamTableHA_onFollower)
{
    int msg1_total = 0;
    srand(time(0));

    string cur_NodeName = conn.run("getNodeAlias()")->getString();
    string followerHost = getFollowerInfo().first, leaderHost = getLeaderInfo().first;
    int followerPort = getFollowerInfo().second, leaderPort = getLeaderInfo().second;

    DBConnection conn_leader(false, false);
    bool res = conn_leader.connect(leaderHost, leaderPort, "admin", "123456");

    cout << "subscribe the streamTable on " << followerHost + ":" + to_string(followerPort) << endl;

    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;

    auto onehandler = [&](Message msg)
    {
        // cout<<msg->get(0)->getString()+","+msg->get(1)->getString()+","+msg->get(2)->getString()+","+msg->get(3)->getString()<<endl;
        LockGuard<Mutex> lock(&mutex);
        msg1_total += 1;

        if (msg1_total == insert_total_rows)
        {
            cout << "get all msg successfully!" << endl;
            notify.set();
        }
    };

    ThreadPooledClient client(listenport, 1);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(followerHost, followerPort, onehandler, "tradesHA", "test_streamHA", 0, true, nullptr, false, false, "admin", "123456"));
    }
    else
    {
        thread th1 = thread(insert_task, followerHost, followerPort, insert_total_rows);
        auto threadVec = client.subscribe(followerHost, followerPort, onehandler, "tradesHA", "test_streamHA", 0, true, nullptr, false, false, "admin", "123456");

        notify.wait();
        th1.join();

        // AC-194: support unsubscribe on follower.
        client.unsubscribe(followerHost, followerPort, "tradesHA", "test_streamHA");
        Util::sleep(1000);

        // auto switch to leader DataNode when subscribing on follower, so assert no subscription on leader
        EXPECT_TRUE(conn_leader.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`tradesHA) ==0")->getBool());
        EXPECT_EQ(msg1_total, insert_total_rows);
    }
    conn_leader.close();
    usedPorts.push_back(listenport);
}

TEST_P(StreamingSubscribeHighAvailableTest, test_Threadpooledclient_threadCount_2_subscribeStreamTableHAbeforeTableCreated_onFollower)
{
    string s1 = "try{dropStreamTable(`tradesHA);}catch(ex){};go;";
    conn.run(s1);
    int msg1_total = 0;
    srand(time(0));

    string cur_NodeName = conn.run("getNodeAlias()")->getString();
    string followerHost = getFollowerInfo().first, leaderHost = getLeaderInfo().first;
    int followerPort = getFollowerInfo().second, leaderPort = getLeaderInfo().second;

    DBConnection conn_leader(false, false);
    bool res = conn_leader.connect(leaderHost, leaderPort, "admin", "123456");

    cout << "subscribe the streamTable on " << followerHost + ":" + to_string(followerPort) << endl;

    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;

    auto onehandler = [&](Message msg)
    {
        // cout<<msg->get(0)->getString()+","+msg->get(1)->getString()+","+msg->get(2)->getString()+","+msg->get(3)->getString()<<endl;
        LockGuard<Mutex> lock(&mutex);
        msg1_total += 1;

        if (msg1_total == insert_total_rows)
        {
            cout << "get all msg successfully!" << endl;
            notify.set();
        }
    };

    ThreadPooledClient client(listenport, 2);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(followerHost, followerPort, onehandler, "tradesHA", "test_streamHA", 0, true, nullptr, false, false, "admin", "123456"));
    }
    else
    {
        thread th1 = thread([&]
                            {
        DBConnection conn_1;
        conn_1.connect(hostName, port, "admin", "123456", "", true, sites);
        cout << "insert datas to HAstreamtable..." << endl;
        conn_1.run("colNames = `timestamp`sym`qty`price;\
                    colTypes = [TIMESTAMP,SYMBOL,INT,DOUBLE];\
                    t=table(100:0,colNames,colTypes);\
                    haStreamTable(" +
                    raftsGroup + ",t,`tradesHA,100000);sleep(1000);go");
        conn_1.run("for (i in 1.." + to_string(insert_total_rows) + "){tableInsert(`tradesHA,rand(timestamp(10000)..timestamp(20000),1),rand(`a`b`c`d,1),rand(1000,1),rand(100.00,1));}");
        cout << "insert finished!" << endl;
        Util::sleep(1000);
        conn_1.close(); });
        auto threadVec = client.subscribe(followerHost, followerPort, onehandler, "tradesHA", "test_streamHA", 0, true, nullptr, false, false, "admin", "123456");

        notify.wait();
        th1.join();

        // AC-194: support unsubscribe on follower.
        client.unsubscribe(followerHost, followerPort, "tradesHA", "test_streamHA");
        Util::sleep(1000);

        // auto switch to leader DataNode when subscribing on follower, so assert no subscription on leader
        EXPECT_TRUE(conn_leader.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`tradesHA) ==0")->getBool());
        EXPECT_EQ(msg1_total, insert_total_rows);
    }

    usedPorts.push_back(listenport);
    conn_leader.close();
}

TEST_P(StreamingSubscribeHighAvailableTest, test_Threadpooledclient_threadCount_2_subscribeStreamTableHA_onFollower)
{
    int msg1_total = 0;
    srand(time(0));

    string cur_NodeName = conn.run("getNodeAlias()")->getString();
    string followerHost = getFollowerInfo().first, leaderHost = getLeaderInfo().first;
    int followerPort = getFollowerInfo().second, leaderPort = getLeaderInfo().second;

    DBConnection conn_leader(false, false);
    bool res = conn_leader.connect(leaderHost, leaderPort, "admin", "123456");

    cout << "subscribe the streamTable on " << followerHost + ":" + to_string(followerPort) << endl;

    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;

    auto onehandler = [&](Message msg)
    {
        // cout<<msg->get(0)->getString()+","+msg->get(1)->getString()+","+msg->get(2)->getString()+","+msg->get(3)->getString()<<endl;
        LockGuard<Mutex> lock(&mutex);
        msg1_total += 1;

        if (msg1_total == insert_total_rows)
        {
            cout << "get all msg successfully!" << endl;
            notify.set();
        }
    };

    ThreadPooledClient client(listenport, 2);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(followerHost, followerPort, onehandler, "tradesHA", "test_streamHA", 0, true, nullptr, false, false, "admin", "123456"));
    }
    else
    {
        thread th1 = thread(insert_task, followerHost, followerPort, insert_total_rows);
        auto threadVec = client.subscribe(followerHost, followerPort, onehandler, "tradesHA", "test_streamHA", 0, true, nullptr, false, false, "admin", "123456");

        notify.wait();
        th1.join();

        // AC-194: support unsubscribe on follower.
        client.unsubscribe(followerHost, followerPort, "tradesHA", "test_streamHA");
        Util::sleep(1000);

        // auto switch to leader DataNode when subscribing on follower, so assert no subscription on leader
        EXPECT_TRUE(conn_leader.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`tradesHA) ==0")->getBool());
        EXPECT_EQ(msg1_total, insert_total_rows);
    }

    usedPorts.push_back(listenport);
    conn_leader.close();
}

TEST_P(StreamingSubscribeHighAvailableTest, test_Threadclient_onehandler_subscribeStreamTableHA_onLeader)
{
    int msg1_total = 0;
    srand(time(0));

    string leaderHost = getLeaderInfo().first;
    int leaderPort = getLeaderInfo().second;
    DBConnection conn_leader(false, false);
    bool res = conn_leader.connect(leaderHost, leaderPort, "admin", "123456");

    cout << "subscribe the streamTable on " << leaderHost + ":" + to_string(leaderPort) << endl;

    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;

    auto onehandler = [&](Message msg)
    {
        // cout<<msg->get(0)->getString()+","+msg->get(1)->getString()+","+msg->get(2)->getString()+","+msg->get(3)->getString()<<endl;
        LockGuard<Mutex> lock(&mutex);
        msg1_total += 1;

        if (msg1_total == insert_total_rows)
        {
            cout << "get all msg successfully!" << endl;
            notify.set();
        }
    };

    ThreadedClient threadedClient(listenport);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(threadedClient.subscribe(leaderHost, leaderPort, onehandler, "tradesHA", "test_streamHA", 0, true, nullptr, false, false, "admin", "123456"));
    }
    else
    {
        thread th1 = thread(insert_task, leaderHost, leaderPort, insert_total_rows);
        auto thread1 = threadedClient.subscribe(leaderHost, leaderPort, onehandler, "tradesHA", "test_streamHA", 0, true, nullptr, false, false, "admin", "123456");
        thread1->setAffinity(0);

        notify.wait();
        th1.join();

        threadedClient.unsubscribe(leaderHost, leaderPort, "tradesHA", "test_streamHA");
        Util::sleep(1000);

        // auto switch to leader DataNode when subscribing on follower, so assert no subscription on leader
        EXPECT_TRUE(conn_leader.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`tradesHA) ==0")->getBool());
        EXPECT_EQ(msg1_total, insert_total_rows);
    }

    usedPorts.push_back(listenport);
    conn_leader.close();
}

TEST_P(StreamingSubscribeHighAvailableTest, test_Threadclient_batchhandler_subscribeStreamTableHA_onLeader)
{
    int msg1_total = 0;
    srand(time(0));

    string leaderHost = getLeaderInfo().first;
    int leaderPort = getLeaderInfo().second;
    DBConnection conn_leader(false, false);
    bool res = conn_leader.connect(leaderHost, leaderPort, "admin", "123456");

    cout << "subscribe the streamTable on " << leaderHost + ":" + to_string(leaderPort) << endl;

    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;

    auto batchhandler = [&](vector<Message> msgs)
    {
        LockGuard<Mutex> lock(&mutex);
        for (auto &msg : msgs)
        {
            LockGuard<Mutex> lock(&mutex);
            msg1_total += 1;

            if (msg1_total == insert_total_rows)
            {
                cout << "get all msg successfully!" << endl;
                notify.set();
            }
        }
    };

    ThreadedClient threadedClient(listenport);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(threadedClient.subscribe(leaderHost, leaderPort, batchhandler, "tradesHA", "test_streamHA", 0, true, nullptr, true, 1, 1.0, false, "admin", "123456"));
    }
    else
    {
        thread th1 = thread(insert_task, leaderHost, leaderPort, insert_total_rows);
        auto thread1 = threadedClient.subscribe(leaderHost, leaderPort, batchhandler, "tradesHA", "test_streamHA", 0, true, nullptr, true, 1, 1.0, false, "admin", "123456");
        thread1->setAffinity(0);
        notify.wait();
        th1.join();

        threadedClient.unsubscribe(leaderHost, leaderPort, "tradesHA", "test_streamHA");
        Util::sleep(1000);

        // auto switch to leader DataNode when subscribing on follower, so assert no subscription on leader
        EXPECT_TRUE(conn_leader.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`tradesHA) ==0")->getBool());
        EXPECT_EQ(msg1_total, insert_total_rows);
    }
    usedPorts.push_back(listenport);
    conn_leader.close();
}

TEST_P(StreamingSubscribeHighAvailableTest, test_Pollingclient_subscribeStreamTableHA_onLeader)
{
    int msg1_total = 0;
    srand(time(0));

    string leaderHost = getLeaderInfo().first;
    int leaderPort = getLeaderInfo().second;
    DBConnection conn_leader(false, false);
    bool res = conn_leader.connect(leaderHost, leaderPort, "admin", "123456");

    cout << "subscribe the streamTable on " << leaderHost + ":" + to_string(leaderPort) << endl;

    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    PollingClient client(listenport);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(leaderHost, leaderPort, "tradesHA", "test_streamHA", 0, true, nullptr, false, false, "admin", "123456"));
    }
    else
    {
        thread th1 = thread(insert_task, leaderHost, leaderPort, insert_total_rows);
        auto queue = client.subscribe(leaderHost, leaderPort, "tradesHA", "test_streamHA", 0, true, nullptr, false, false, "admin", "123456");
        Message msg;
        ThreadSP th2 = new Thread(new Executor([&]
                                               {
        while (true)
        {
            queue->pop(msg);
            if(msg.isNull()) {break;}
            else{
                msg1_total += 1;
                // cout << msg1_total << endl;
                // cout << msg->getString() << endl;
                if (msg1_total == insert_total_rows)
                {
                    cout << "get all msg successfully!" << endl;
                }
            }
        } }));

        th1.join();
        Util::sleep(1000);
        // AC-194: support unsubscribe on follower.
        client.unsubscribe(leaderHost, leaderPort, "tradesHA", "test_streamHA");
        th2->start();
        th2->join();
        Util::sleep(100);
        // auto switch to leader DataNode when subscribing on follower, so assert no subscription on leader
        EXPECT_TRUE(conn_leader.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`tradesHA) ==0")->getBool());
        EXPECT_EQ(msg1_total, insert_total_rows);
    }

    usedPorts.push_back(listenport);
    conn_leader.close();
}

TEST_P(StreamingSubscribeHighAvailableTest, test_Threadpooledclient_threadCount_1_subscribeStreamTableHA_onLeader)
{
    int msg1_total = 0;
    srand(time(0));

    string leaderHost = getLeaderInfo().first;
    int leaderPort = getLeaderInfo().second;
    DBConnection conn_leader(false, false);
    bool res = conn_leader.connect(leaderHost, leaderPort, "admin", "123456");

    cout << "subscribe the streamTable on " << leaderHost + ":" + to_string(leaderPort) << endl;

    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;

    auto onehandler = [&](Message msg)
    {
        // cout<<msg->get(0)->getString()+","+msg->get(1)->getString()+","+msg->get(2)->getString()+","+msg->get(3)->getString()<<endl;
        LockGuard<Mutex> lock(&mutex);
        msg1_total += 1;

        if (msg1_total == insert_total_rows)
        {
            cout << "get all msg successfully!" << endl;
            notify.set();
        }
    };

    ThreadPooledClient client(listenport, 1);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(leaderHost, leaderPort, onehandler, "tradesHA", "test_streamHA", 0, true, nullptr, false, false, "admin", "123456"));
    }
    else
    {
        thread th1 = thread(insert_task, leaderHost, leaderPort, insert_total_rows);
        auto threadVec = client.subscribe(leaderHost, leaderPort, onehandler, "tradesHA", "test_streamHA", 0, true, nullptr, false, false, "admin", "123456");
        notify.wait();
        th1.join();

        client.unsubscribe(leaderHost, leaderPort, "tradesHA", "test_streamHA");
        Util::sleep(1000);

        // auto switch to leader DataNode when subscribing on follower, so assert no subscription on leader
        EXPECT_TRUE(conn_leader.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`tradesHA) ==0")->getBool());
        EXPECT_EQ(msg1_total, insert_total_rows);
    }

    usedPorts.push_back(listenport);
    conn_leader.close();
}

TEST_P(StreamingSubscribeHighAvailableTest, test_Threadpooledclient_threadCount_2_subscribeStreamTableHA_onLeader)
{
    int msg1_total = 0;
    srand(time(0));

    string leaderHost = getLeaderInfo().first;
    int leaderPort = getLeaderInfo().second;
    DBConnection conn_leader(false, false);
    bool res = conn_leader.connect(leaderHost, leaderPort, "admin", "123456");

    cout << "subscribe the streamTable on " << leaderHost + ":" + to_string(leaderPort) << endl;

    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;

    auto onehandler = [&](Message msg)
    {
        // cout<<msg->get(0)->getString()+","+msg->get(1)->getString()+","+msg->get(2)->getString()+","+msg->get(3)->getString()<<endl;
        LockGuard<Mutex> lock(&mutex);
        msg1_total += 1;

        if (msg1_total == insert_total_rows)
        {
            cout << "get all msg successfully!" << endl;
            notify.set();
        }
    };

    ThreadPooledClient client(listenport, 2);
    if (!isNewVersion && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(leaderHost, leaderPort, onehandler, "tradesHA", "test_streamHA", 0, true, nullptr, false, false, "admin", "123456"));
    }
    else
    {
        thread th1 = thread(insert_task, leaderHost, leaderPort, insert_total_rows);
        auto threadVec = client.subscribe(leaderHost, leaderPort, onehandler, "tradesHA", "test_streamHA", 0, true, nullptr, false, false, "admin", "123456");

        notify.wait();
        th1.join();

        client.unsubscribe(leaderHost, leaderPort, "tradesHA", "test_streamHA");
        Util::sleep(1000);

        // auto switch to leader DataNode when subscribing on follower, so assert no subscription on leader
        EXPECT_TRUE(conn_leader.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`tradesHA) ==0")->getBool());
        EXPECT_EQ(msg1_total, insert_total_rows);
    }

    usedPorts.push_back(listenport);
    conn_leader.close();
}