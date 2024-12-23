#include "config.h"

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
        cout << "ok" << endl;
        CLEAR_ENV(conn);
    }
    virtual void TearDown()
    {
        CLEAR_ENV(conn);
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

void insert_task(string host, string tab, int port, int totalInsertNum)
{
    DBConnection conn_1;
    conn_1.connect(host, port, "admin", "123456", "", true, sites);
    cout << "insert datas to HAstreamtable..." << endl;
    for(auto i =0;i<totalInsertNum;i++)
        ASSERT_TRUE(conn_1.run("tableInsert(`"+tab+",rand(timestamp(10000)..timestamp(20000),1),rand(`a`b`c`d,1),rand(1000,1),rand(100.00,1))")->getBool());
    cout << "insert finished!" << endl;
    Util::sleep(1000);
    conn_1.close();
}

string createHAstreamTable(){
    string st = "tradeHA_" + getRandString(8);
    string createHAstreamTable = "colNames = `timestamp`sym`qty`price;\
                                    colTypes = [TIMESTAMP,SYMBOL,INT,DOUBLE];\
                                    t=table(1:0,colNames,colTypes);\
                                    haStreamTable(" +
                                         raftsGroup + ",t,`"+st+",100000);sleep(1000);";
    conn.run(createHAstreamTable);
    for (int i = 0; i < sites.size(); i++)
    {
        string host = split(sites[i], ":")[0];
        int port = atoi(split(sites[i], ":")[1].c_str());
        DBConnectionSP _conn = new DBConnection();
        EXPECT_TRUE(_conn->connect(host, port, "admin", "123456"));
        _conn->run(
            "do{\
                sleep(1000);\
            }while(!(`"+st+" in (exec name from objs(true))))"); // server有时候在raftGroup的某个节点上建表太慢，需要遍历每个节点并等待建表完成
        _conn->close();
    }
    return st;
}

const int insert_total_rows = 2000; // total rows you want to insert and test.
INSTANTIATE_TEST_SUITE_P(, StreamingSubscribeHighAvailableTest, testing::Values(-1, rand() % 1000 + 13000));
TEST_P(StreamingSubscribeHighAvailableTest, test_Threadclient_onehandler_subscribeStreamTableHA_onFollower)
{
    string HAtab = createHAstreamTable();
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

    ThreadedClient threadedClient = listenport == -1? ThreadedClient() : ThreadedClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(threadedClient.subscribe(followerHost, followerPort, onehandler, HAtab, "test_streamHA", 0, true, nullptr, false, false, "admin", "123456"));
    }
    else
    {
        thread th1 = thread(insert_task, followerHost, HAtab, followerPort, insert_total_rows);
        auto thread1 = threadedClient.subscribe(followerHost, followerPort, onehandler, HAtab, "test_streamHA", 0, true, nullptr, false, false, "admin", "123456");
        thread1->setAffinity(0);

        notify.wait();
        th1.join();

        // AC-194: support unsubscribe on follower.
        threadedClient.unsubscribe(followerHost, followerPort, HAtab, "test_streamHA");
        Util::sleep(1000);

        // auto switch to leader DataNode when subscribing on follower, so assert no subscription on leader
        EXPECT_TRUE(conn_leader.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+HAtab+") ==0")->getBool());
        EXPECT_EQ(msg1_total, insert_total_rows);
    }

    usedPorts.insert(listenport);
    conn_leader.close();
    conn.run("try{dropStreamTable(`"+HAtab+");}catch(ex){};go;");
}

TEST_P(StreamingSubscribeHighAvailableTest, test_Threadclient_onehandler_subscribeStreamTableHAbeforeTableCreated_onFollower)
{
    string HAtab = createHAstreamTable();
    string s1 = "try{dropStreamTable(`"+HAtab+");}catch(ex){};go;";
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

    ThreadedClient threadedClient = listenport == -1? ThreadedClient() : ThreadedClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(threadedClient.subscribe(followerHost, followerPort, onehandler, HAtab, "test_streamHA", 0, true, nullptr, false, false, "admin", "123456"));
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
                    raftsGroup + ",t,`"+HAtab+",100000);sleep(1000);go");
        for(auto i =0;i<insert_total_rows;i++)
            conn_1.run("tableInsert(`"+HAtab+",rand(timestamp(10000)..timestamp(20000),1),rand(`a`b`c`d,1),rand(1000,1),rand(100.00,1))");
        cout << "insert finished!" << endl;
        Util::sleep(1000);
        conn_1.close(); });
        auto thread1 = threadedClient.subscribe(followerHost, followerPort, onehandler, HAtab, "test_streamHA", 0, true, nullptr, false, false, "admin", "123456");
        thread1->setAffinity(0);

        notify.wait();
        th1.join();

        // AC-194: support unsubscribe on follower.
        threadedClient.unsubscribe(followerHost, followerPort, HAtab, "test_streamHA");
        Util::sleep(1000);

        // auto switch to leader DataNode when subscribing on follower, so assert no subscription on leader
        EXPECT_TRUE(conn_leader.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+HAtab+") ==0")->getBool());
        EXPECT_EQ(msg1_total, insert_total_rows);
    }

    usedPorts.insert(listenport);
    conn_leader.close();
    conn.run("try{dropStreamTable(`"+HAtab+");}catch(ex){};go;");
}

TEST_P(StreamingSubscribeHighAvailableTest, test_Threadclient_batchhandler_subscribeStreamTableHA_onFollower)
{
    string HAtab = createHAstreamTable();
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

    ThreadedClient threadedClient = listenport == -1? ThreadedClient() : ThreadedClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(threadedClient.subscribe(followerHost, followerPort, batchhandler, HAtab, "test_streamHA", 0, true, nullptr, true, 1, 1.0, false, "admin", "123456"));
    }
    else
    {
        thread th1 = thread(insert_task, followerHost, HAtab, followerPort, insert_total_rows);
        auto thread1 = threadedClient.subscribe(followerHost, followerPort, batchhandler, HAtab, "test_streamHA", 0, true, nullptr, true, 1, 1.0, false, "admin", "123456");
        thread1->setAffinity(0);

        notify.wait();
        th1.join();

        // AC-194: support unsubscribe on follower.
        threadedClient.unsubscribe(followerHost, followerPort, HAtab, "test_streamHA");
        Util::sleep(1000);

        // auto switch to leader DataNode when subscribing on follower, so assert no subscription on leader
        EXPECT_TRUE(conn_leader.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+HAtab+") ==0")->getBool());
        EXPECT_EQ(msg1_total, insert_total_rows);
    }

    usedPorts.insert(listenport);
    conn_leader.close();
    conn.run("try{dropStreamTable(`"+HAtab+");}catch(ex){};go;");
}

TEST_P(StreamingSubscribeHighAvailableTest, test_Pollingclient_subscribeStreamTableHA_onFollower)
{
    string HAtab = createHAstreamTable();
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

    PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(followerHost, followerPort, HAtab, "test_streamHA", 0, true, nullptr, false, false, "admin", "123456"));
    }
    else
    {
        thread th1 = thread(insert_task, followerHost, HAtab, followerPort, insert_total_rows);
        auto queue = client.subscribe(followerHost, followerPort, HAtab, "test_streamHA", 0, true, nullptr, false, false, "admin", "123456");
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
        client.unsubscribe(followerHost, followerPort, HAtab, "test_streamHA");
        th2->start();
        th2->join();
        Util::sleep(100);
        // auto switch to leader DataNode when subscribing on follower, so assert no subscription on leader
        EXPECT_TRUE(conn_leader.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+HAtab+") ==0")->getBool());
        EXPECT_EQ(msg1_total, insert_total_rows);
    }

    usedPorts.insert(listenport);
    conn_leader.close();
    conn.run("try{dropStreamTable(`"+HAtab+");}catch(ex){};go;");
}

TEST_P(StreamingSubscribeHighAvailableTest, test_Pollingclient_subscribeStreamTableHAbeforeTableCreated_onFollower)
{
    string HAtab = createHAstreamTable();
    string s1 = "try{dropStreamTable(`"+HAtab+");}catch(ex){};go;";
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
    PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(followerHost, followerPort, HAtab, "test_streamHA", 0, true, nullptr, false, false, "admin", "123456"));
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
                    raftsGroup + ",t,`"+HAtab+",100000);sleep(1000);go");
        for(auto i =0;i<insert_total_rows;i++)
            conn_1.run("tableInsert(`"+HAtab+",rand(timestamp(10000)..timestamp(20000),1),rand(`a`b`c`d,1),rand(1000,1),rand(100.00,1))");
        cout << "insert finished!" << endl;
        Util::sleep(1000);
        conn_1.close(); });
        auto queue = client.subscribe(followerHost, followerPort, HAtab, "test_streamHA", 0, true, nullptr, false, false, "admin", "123456");
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
        client.unsubscribe(followerHost, followerPort, HAtab, "test_streamHA");
        th2->start();
        th2->join();
        Util::sleep(100);

        // auto switch to leader DataNode when subscribing on follower, so assert no subscription on leader
        EXPECT_TRUE(conn_leader.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+HAtab+") ==0")->getBool());
        EXPECT_EQ(msg1_total, insert_total_rows);
    }

    usedPorts.insert(listenport);
    conn_leader.close();
    conn.run("try{dropStreamTable(`"+HAtab+");}catch(ex){};go;");
}

TEST_P(StreamingSubscribeHighAvailableTest, test_Threadpooledclient_threadCount_1_subscribeStreamTableHA_onFollower)
{
    string HAtab = createHAstreamTable();
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

    ThreadPooledClient client = listenport == -1? ThreadPooledClient(0, 1) : ThreadPooledClient(listenport, 1);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(followerHost, followerPort, onehandler, HAtab, "test_streamHA", 0, true, nullptr, false, false, "admin", "123456"));
    }
    else
    {
        thread th1 = thread(insert_task, followerHost, HAtab, followerPort, insert_total_rows);
        auto threadVec = client.subscribe(followerHost, followerPort, onehandler, HAtab, "test_streamHA", 0, true, nullptr, false, false, "admin", "123456");

        notify.wait();
        th1.join();

        // AC-194: support unsubscribe on follower.
        client.unsubscribe(followerHost, followerPort, HAtab, "test_streamHA");
        Util::sleep(1000);

        // auto switch to leader DataNode when subscribing on follower, so assert no subscription on leader
        EXPECT_TRUE(conn_leader.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+HAtab+") ==0")->getBool());
        EXPECT_EQ(msg1_total, insert_total_rows);
    }
    conn_leader.close();
    conn.run("try{dropStreamTable(`"+HAtab+");}catch(ex){};go;");
    usedPorts.insert(listenport);
    conn.run("try{dropStreamTable(`"+HAtab+");}catch(ex){};go;");
}

TEST_P(StreamingSubscribeHighAvailableTest, test_Threadpooledclient_threadCount_2_subscribeStreamTableHAbeforeTableCreated_onFollower)
{
    string HAtab = createHAstreamTable();
    string s1 = "try{dropStreamTable(`"+HAtab+");}catch(ex){};go;";
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

    ThreadPooledClient client = listenport == -1? ThreadPooledClient(0, 2) : ThreadPooledClient(listenport, 2);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(followerHost, followerPort, onehandler, HAtab, "test_streamHA", 0, true, nullptr, false, false, "admin", "123456"));
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
                    raftsGroup + ",t,`"+HAtab+",100000);sleep(1000);go");
        for(auto i =0;i<insert_total_rows;i++)
            conn_1.run("tableInsert(`"+HAtab+",rand(timestamp(10000)..timestamp(20000),1),rand(`a`b`c`d,1),rand(1000,1),rand(100.00,1))");
        cout << "insert finished!" << endl;
        Util::sleep(1000);
        conn_1.close(); });
        auto threadVec = client.subscribe(followerHost, followerPort, onehandler, HAtab, "test_streamHA", 0, true, nullptr, false, false, "admin", "123456");

        notify.wait();
        th1.join();

        // AC-194: support unsubscribe on follower.
        client.unsubscribe(followerHost, followerPort, HAtab, "test_streamHA");
        Util::sleep(1000);

        // auto switch to leader DataNode when subscribing on follower, so assert no subscription on leader
        EXPECT_TRUE(conn_leader.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+HAtab+") ==0")->getBool());
        EXPECT_EQ(msg1_total, insert_total_rows);
    }

    usedPorts.insert(listenport);
    conn_leader.close();
    conn.run("try{dropStreamTable(`"+HAtab+");}catch(ex){};go;");
}

TEST_P(StreamingSubscribeHighAvailableTest, test_Threadpooledclient_threadCount_2_subscribeStreamTableHA_onFollower)
{
    string HAtab = createHAstreamTable();
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

    ThreadPooledClient client = listenport == -1? ThreadPooledClient(0, 2) : ThreadPooledClient(listenport, 2);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(followerHost, followerPort, onehandler, HAtab, "test_streamHA", 0, true, nullptr, false, false, "admin", "123456"));
    }
    else
    {
        thread th1 = thread(insert_task, followerHost, HAtab, followerPort, insert_total_rows);
        auto threadVec = client.subscribe(followerHost, followerPort, onehandler, HAtab, "test_streamHA", 0, true, nullptr, false, false, "admin", "123456");

        notify.wait();
        th1.join();

        // AC-194: support unsubscribe on follower.
        client.unsubscribe(followerHost, followerPort, HAtab, "test_streamHA");
        Util::sleep(1000);

        // auto switch to leader DataNode when subscribing on follower, so assert no subscription on leader
        EXPECT_TRUE(conn_leader.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+HAtab+") ==0")->getBool());
        EXPECT_EQ(msg1_total, insert_total_rows);
    }

    usedPorts.insert(listenport);
    conn_leader.close();
    conn.run("try{dropStreamTable(`"+HAtab+");}catch(ex){};go;");
}

TEST_P(StreamingSubscribeHighAvailableTest, test_Threadclient_onehandler_subscribeStreamTableHA_onLeader)
{
    string HAtab = createHAstreamTable();
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

    ThreadedClient threadedClient = listenport == -1? ThreadedClient() : ThreadedClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(threadedClient.subscribe(leaderHost, leaderPort, onehandler, HAtab, "test_streamHA", 0, true, nullptr, false, false, "admin", "123456"));
    }
    else
    {
        thread th1 = thread(insert_task, leaderHost, HAtab, leaderPort, insert_total_rows);
        auto thread1 = threadedClient.subscribe(leaderHost, leaderPort, onehandler, HAtab, "test_streamHA", 0, true, nullptr, false, false, "admin", "123456");
        thread1->setAffinity(0);

        notify.wait();
        th1.join();

        threadedClient.unsubscribe(leaderHost, leaderPort, HAtab, "test_streamHA");
        Util::sleep(1000);

        // auto switch to leader DataNode when subscribing on follower, so assert no subscription on leader
        EXPECT_TRUE(conn_leader.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+HAtab+") ==0")->getBool());
        EXPECT_EQ(msg1_total, insert_total_rows);
    }

    usedPorts.insert(listenport);
    conn_leader.close();
    conn.run("try{dropStreamTable(`"+HAtab+");}catch(ex){};go;");
}

TEST_P(StreamingSubscribeHighAvailableTest, test_Threadclient_batchhandler_subscribeStreamTableHA_onLeader)
{
    string HAtab = createHAstreamTable();
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

    ThreadedClient threadedClient = listenport == -1? ThreadedClient() : ThreadedClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(threadedClient.subscribe(leaderHost, leaderPort, batchhandler, HAtab, "test_streamHA", 0, true, nullptr, true, 1, 1.0, false, "admin", "123456"));
    }
    else
    {
        thread th1 = thread(insert_task, leaderHost, HAtab, leaderPort, insert_total_rows);
        auto thread1 = threadedClient.subscribe(leaderHost, leaderPort, batchhandler, HAtab, "test_streamHA", 0, true, nullptr, true, 1, 1.0, false, "admin", "123456");
        thread1->setAffinity(0);
        notify.wait();
        th1.join();

        threadedClient.unsubscribe(leaderHost, leaderPort, HAtab, "test_streamHA");
        Util::sleep(1000);

        // auto switch to leader DataNode when subscribing on follower, so assert no subscription on leader
        EXPECT_TRUE(conn_leader.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+HAtab+") ==0")->getBool());
        EXPECT_EQ(msg1_total, insert_total_rows);
    }
    usedPorts.insert(listenport);
    conn_leader.close();
    conn.run("try{dropStreamTable(`"+HAtab+");}catch(ex){};go;");
}

TEST_P(StreamingSubscribeHighAvailableTest, test_Pollingclient_subscribeStreamTableHA_onLeader)
{
    string HAtab = createHAstreamTable();
    int msg1_total = 0;
    srand(time(0));

    string leaderHost = getLeaderInfo().first;
    int leaderPort = getLeaderInfo().second;
    DBConnection conn_leader(false, false);
    bool res = conn_leader.connect(leaderHost, leaderPort, "admin", "123456");

    cout << "subscribe the streamTable on " << leaderHost + ":" + to_string(leaderPort) << endl;

    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(leaderHost, leaderPort, HAtab, "test_streamHA", 0, true, nullptr, false, false, "admin", "123456"));
    }
    else
    {
        thread th1 = thread(insert_task, leaderHost, HAtab, leaderPort, insert_total_rows);
        auto queue = client.subscribe(leaderHost, leaderPort, HAtab, "test_streamHA", 0, true, nullptr, false, false, "admin", "123456");
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
        client.unsubscribe(leaderHost, leaderPort, HAtab, "test_streamHA");
        th2->start();
        th2->join();
        Util::sleep(100);
        // auto switch to leader DataNode when subscribing on follower, so assert no subscription on leader
        EXPECT_TRUE(conn_leader.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+HAtab+") ==0")->getBool());
        EXPECT_EQ(msg1_total, insert_total_rows);
    }

    usedPorts.insert(listenport);
    conn_leader.close();
    conn.run("try{dropStreamTable(`"+HAtab+");}catch(ex){};go;");
}

TEST_P(StreamingSubscribeHighAvailableTest, test_Threadpooledclient_threadCount_1_subscribeStreamTableHA_onLeader)
{
    string HAtab = createHAstreamTable();
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

    ThreadPooledClient client = listenport == -1? ThreadPooledClient(0, 1) : ThreadPooledClient(listenport, 1);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(leaderHost, leaderPort, onehandler, HAtab, "test_streamHA", 0, true, nullptr, false, false, "admin", "123456"));
    }
    else
    {
        thread th1 = thread(insert_task, leaderHost, HAtab, leaderPort, insert_total_rows);
        auto threadVec = client.subscribe(leaderHost, leaderPort, onehandler, HAtab, "test_streamHA", 0, true, nullptr, false, false, "admin", "123456");
        notify.wait();
        th1.join();

        client.unsubscribe(leaderHost, leaderPort, HAtab, "test_streamHA");
        Util::sleep(1000);

        // auto switch to leader DataNode when subscribing on follower, so assert no subscription on leader
        EXPECT_TRUE(conn_leader.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+HAtab+") ==0")->getBool());
        EXPECT_EQ(msg1_total, insert_total_rows);
    }

    usedPorts.insert(listenport);
    conn_leader.close();
    conn.run("try{dropStreamTable(`"+HAtab+");}catch(ex){};go;");
}

TEST_P(StreamingSubscribeHighAvailableTest, test_Threadpooledclient_threadCount_2_subscribeStreamTableHA_onLeader)
{
    string HAtab = createHAstreamTable();
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

    ThreadPooledClient client = listenport == -1? ThreadPooledClient(0, 2) : ThreadPooledClient(listenport, 2);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(leaderHost, leaderPort, onehandler, HAtab, "test_streamHA", 0, true, nullptr, false, false, "admin", "123456"));
    }
    else
    {
        thread th1 = thread(insert_task, leaderHost, HAtab, leaderPort, insert_total_rows);
        auto threadVec = client.subscribe(leaderHost, leaderPort, onehandler, HAtab, "test_streamHA", 0, true, nullptr, false, false, "admin", "123456");

        notify.wait();
        th1.join();

        client.unsubscribe(leaderHost, leaderPort, HAtab, "test_streamHA");
        Util::sleep(1000);

        // auto switch to leader DataNode when subscribing on follower, so assert no subscription on leader
        EXPECT_TRUE(conn_leader.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+HAtab+") ==0")->getBool());
        EXPECT_EQ(msg1_total, insert_total_rows);
    }

    usedPorts.insert(listenport);
    conn_leader.close();
    conn.run("try{dropStreamTable(`"+HAtab+");}catch(ex){};go;");
}


TEST_P(StreamingSubscribeHighAvailableTest, test_Pollingclient_backupSites)
{
    string HAtab = createHAstreamTable();
    int msg1_total = 0;
    srand(time(0));
    string leaderName = conn.run("getStreamingLeader("+raftsGroup+")")->getString();

    DBConnectionSP conn_ctl(new DBConnection(false, false));
    conn_ctl->connect(hostName, ctl_port, "admin", "123456");

    TableSP res_tradeHA = conn.run("colNames = `timestamp`sym`qty`price;\
                                    colTypes = [TIMESTAMP,SYMBOL,INT,DOUBLE];\
                                    table(1:0,colNames,colTypes);");
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(hostName, port, HAtab, "test_backupSites", 0, true, nullptr, false, false, "admin", "123456", nullptr, backupSites));
    }
    else
    {
        thread th1 = thread(insert_task, hostName, HAtab, port, insert_total_rows/2);
        auto queue = client.subscribe(hostName, port, HAtab, "test_backupSites", 0, true, nullptr, false, false, "admin", "123456", nullptr, backupSites, 500, false);
        Message msg;
        auto handler = [&]{
        while (true)
        {
            queue->pop(msg);
            if(msg.isNull()) {break;}
            else{
                msg1_total += 1;
                vector<ConstantSP> rowData(msg->size());
                INDEX insertedrows = 0;
                string errMsg;
                for (int i = 0; i < msg->size(); i++)
                {
                    rowData[i] = msg->get(i);
                }
                if (!res_tradeHA->append(rowData, insertedrows, errMsg))
                {
                    cout << "append data failed with err: " << errMsg << endl;
                    break;
                }
                // if (msg1_total % 100 == 0)
                //     cout << msg1_total << endl;
                // cout << msg->getString() << endl;
                if (msg1_total == insert_total_rows)
                {
                    cout << "get all msg successfully!" << endl;
                }
            }
        } };
        ThreadSP th_getData = new Thread(new Executor(handler));

        th1.join();
        Util::sleep(1000);
        conn_ctl->run("try{stopDataNode(`" + leaderName + ")}catch(ex){};sleep(5000);go;"
                        "do{sleep(1000);}while((exec state from getClusterPerf() where name = `" + leaderName + ")[0] != 0);"
                        "print('stop leader DataNode successfully!');");
        conn.run("do{"
                        "try{"
                            "res = getStreamingLeader("+raftsGroup+");"
                            "break;"
                        "}catch(ex){};"
                    "}while(true);"
                    "print('current leader is: '+res)");
        thread th2 = thread(insert_task, hostName, HAtab, port, insert_total_rows/2);
        th2.join();

        cout << "unsubscribe the streamTable..." << endl;
        client.unsubscribe(hostName, port, HAtab, "test_backupSites");
        cout << "unsubscribe finished!" << endl;

        th_getData->start();
        th_getData->join();
        Util::sleep(100);
        conn.upload("res_tradeHA", {res_tradeHA});
        auto res = conn.run("res = select * from res_tradeHA order by timestamp;ex = select * from "+HAtab+" order by timestamp;"
                            "each(eqObj, ex.values(), res.values())")->getString();
        EXPECT_EQ(res, "[1,1,1,1]");

        conn_ctl->run("startDataNode(`" + leaderName + ");sleep(1000);go;"
                      "do{sleep(1000);}while((exec state from getClusterPerf() where name = `" + leaderName + ")[0] != 1);");
    }

    usedPorts.insert(listenport);
    conn.run("try{dropStreamTable(`"+HAtab+");}catch(ex){};go;");
    conn_ctl->close();
}


TEST_P(StreamingSubscribeHighAvailableTest, test_ThreadedClient_onehandler_backupSites)
{
    string HAtab = createHAstreamTable();
    srand(time(0));
    string leaderName = conn.run("getStreamingLeader("+raftsGroup+")")->getString();

    DBConnectionSP conn_ctl(new DBConnection(false, false));
    conn_ctl->connect(hostName, ctl_port, "admin", "123456");

    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;
    int msg_total = 0;
    TableSP res_tradeHA = conn.run("colNames = `timestamp`sym`qty`price;\
                                    colTypes = [TIMESTAMP,SYMBOL,INT,DOUBLE];\
                                    table(1:0,colNames,colTypes);");

    auto onehandler = [&](Message msg)
    {
        LockGuard<Mutex> lock(&mutex);
        msg_total += 1;
        vector<ConstantSP> rowData(msg->size());
        INDEX insertedrows = 0;
        string errMsg;
        for (int i = 0; i < msg->size(); i++)
        {
            rowData[i] = msg->get(i);
        }
        if (!res_tradeHA->append(rowData, insertedrows, errMsg))
        {
            throw RuntimeException("append data failed with err: " + errMsg);
        }
        // if (msg_total % 100 == 0)
        //     cout << "now subscribed rows: " << msg_total << endl;
        // handle msg
        if (msg_total == insert_total_rows)
        {
            notify.set();
        }
    };

    ThreadedClient threadedClient = listenport == -1? ThreadedClient() : ThreadedClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, HAtab, "test_backupSites", 0, true, nullptr, false, false, "admin", "123456", nullptr, backupSites));
    }
    else
    {
        thread th1 = thread(insert_task, hostName, HAtab, port, insert_total_rows/2);
        auto th_sub = threadedClient.subscribe(hostName, port, onehandler, HAtab, "test_backupSites", 0, true, nullptr, false, false, "admin", "123456", nullptr, backupSites, 500, false);
        Message msg;

        th1.join();
        Util::sleep(1000);
        conn_ctl->run("try{stopDataNode(`" + leaderName + ")}catch(ex){};go;"
                        "do{sleep(1000);}while((exec state from getClusterPerf() where name = `" + leaderName + ")[0] != 0);"
                        "print('stop leader DataNode successfully!');");
        conn.run("do{"
                        "try{"
                            "res = getStreamingLeader("+raftsGroup+");"
                            "break;"
                        "}catch(ex){};"
                    "}while(true);"
                    "print('current leader is: '+res)");
        std::thread th2 = std::thread(insert_task, hostName, HAtab, port, insert_total_rows/2);
        th2.join();

        notify.wait();

        cout << "unsubscribe the streamTable..." << endl;
        threadedClient.unsubscribe(hostName, port, HAtab, "test_backupSites");
        cout << "unsubscribe finished!" << endl;
        conn.upload("res_tradeHA", {res_tradeHA});
        auto res = conn.run("res = select * from res_tradeHA order by timestamp;ex = select * from "+HAtab+" order by timestamp;"
                            "each(eqObj, ex.values(), res.values())")->getString();
        EXPECT_EQ(res, "[1,1,1,1]");

        conn_ctl->run("startDataNode(`" + leaderName + ");sleep(1000);go;"
                      "do{sleep(1000);}while((exec state from getClusterPerf() where name = `" + leaderName + ")[0] != 1);");
    }

    usedPorts.insert(listenport);
    conn.run("try{dropStreamTable(`"+HAtab+");}catch(ex){};go;");
    conn_ctl->close();
}


TEST_P(StreamingSubscribeHighAvailableTest, test_ThreadedClient_batchhandler_backupSites)
{
    string HAtab = createHAstreamTable();
    srand(time(0));
    string leaderName = conn.run("getStreamingLeader("+raftsGroup+")")->getString();

    DBConnectionSP conn_ctl(new DBConnection(false, false));
    conn_ctl->connect(hostName, ctl_port, "admin", "123456");

    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;
    int msg_total = 0;
    TableSP res_tradeHA = conn.run("colNames = `timestamp`sym`qty`price;\
                                    colTypes = [TIMESTAMP,SYMBOL,INT,DOUBLE];\
                                    table(1:0,colNames,colTypes);");

    auto batchhandler = [&](vector<Message> msgs)
    {
        LockGuard<Mutex> lock(&mutex);
        for (auto &msg : msgs)
        {
            msg_total += 1;
            vector<ConstantSP> rowData(msg->size());
            INDEX insertedrows = 0;
            string errMsg;
            for (int i = 0; i < msg->size(); i++)
            {
                rowData[i] = msg->get(i);
            }
            if (!res_tradeHA->append(rowData, insertedrows, errMsg))
            {
                throw RuntimeException("append data failed with err: " + errMsg);
            }
            // if (msg_total % 100 == 0)
            //     cout << "now subscribed rows: " << msg_total << endl;
            // handle msg
            if (msg_total == insert_total_rows)
            {
                notify.set();
            }
        }
    };

    ThreadedClient threadedClient = listenport == -1? ThreadedClient() : ThreadedClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, HAtab, "test_backupSites", 0, true, nullptr, false, 100, 1.0, false, "admin", "123456", nullptr, backupSites));
    }
    else
    {
        thread th1 = thread(insert_task, hostName, HAtab, port, insert_total_rows/2);
        auto th_sub = threadedClient.subscribe(hostName, port, batchhandler, HAtab, "test_backupSites", 0, true, nullptr, false, 100, 1.0, false, "admin", "123456", nullptr, backupSites, 500, false);
        Message msg;

        th1.join();
        Util::sleep(1000);
        conn_ctl->run("try{stopDataNode(`" + leaderName + ")}catch(ex){};go;"
                        "do{sleep(1000);}while((exec state from getClusterPerf() where name = `" + leaderName + ")[0] != 0);"
                        "print('stop leader DataNode successfully!');");
        conn.run("do{"
                        "try{"
                            "res = getStreamingLeader("+raftsGroup+");"
                            "break;"
                        "}catch(ex){};"
                    "}while(true);"
                    "print('current leader is: '+res)");
        std::thread th2 = std::thread(insert_task, hostName, HAtab, port, insert_total_rows/2);
        th2.join();

        notify.wait();

        cout << "unsubscribe the streamTable..." << endl;
        threadedClient.unsubscribe(hostName, port, HAtab, "test_backupSites");
        cout << "unsubscribe finished!" << endl;
        conn.upload("res_tradeHA", {res_tradeHA});
        auto res = conn.run("res = select * from res_tradeHA order by timestamp;ex = select * from "+HAtab+" order by timestamp;"
                            "each(eqObj, ex.values(), res.values())")->getString();
        EXPECT_EQ(res, "[1,1,1,1]");

        conn_ctl->run("startDataNode(`" + leaderName + ");sleep(1000);go;"
                      "do{sleep(1000);}while((exec state from getClusterPerf() where name = `" + leaderName + ")[0] != 1);");
    }

    usedPorts.insert(listenport);
    conn.run("try{dropStreamTable(`"+HAtab+");}catch(ex){};go;");
    conn_ctl->close();
}


TEST_P(StreamingSubscribeHighAvailableTest, test_ThreadPooledClient_backupSites)
{
    string HAtab = createHAstreamTable();
    srand(time(0));
    string leaderName = conn.run("getStreamingLeader("+raftsGroup+")")->getString();

    DBConnectionSP conn_ctl(new DBConnection(false, false));
    conn_ctl->connect(hostName, ctl_port, "admin", "123456");

    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;
    int msg_total = 0;
    TableSP res_tradeHA = conn.run("colNames = `timestamp`sym`qty`price;\
                                    colTypes = [TIMESTAMP,SYMBOL,INT,DOUBLE];\
                                    table(1:0,colNames,colTypes);");

    auto onehandler = [&](Message msg)
    {
        LockGuard<Mutex> lock(&mutex);
        msg_total += 1;
        vector<ConstantSP> rowData(msg->size());
        INDEX insertedrows = 0;
        string errMsg;
        for (int i = 0; i < msg->size(); i++)
        {
            rowData[i] = msg->get(i);
        }
        if (!res_tradeHA->append(rowData, insertedrows, errMsg))
        {
            throw RuntimeException("append data failed with err: " + errMsg);
        }
        // if (msg_total % 100 == 0)
        //     cout << "now subscribed rows: " << msg_total << endl;
        // handle msg
        if (msg_total == insert_total_rows)
        {
            notify.set();
        }
    };

    ThreadPooledClient client = listenport == -1? ThreadPooledClient() : ThreadPooledClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, HAtab, "test_backupSites", 0, true, nullptr, false, false, "admin", "123456", nullptr, backupSites));
    }
    else
    {
        thread th1 = thread(insert_task, hostName, HAtab, port, insert_total_rows/2);
        auto thv_sub = client.subscribe(hostName, port, onehandler, HAtab, "test_backupSites", 0, true, nullptr, false, false, "admin", "123456", nullptr, backupSites, 500, false);
        Message msg;

        th1.join();
        Util::sleep(1000);
        conn_ctl->run("try{stopDataNode(`" + leaderName + ")}catch(ex){};go;"
                        "do{sleep(1000);}while((exec state from getClusterPerf() where name = `" + leaderName + ")[0] != 0);"
                        "print('stop leader DataNode successfully!');");
        conn.run("do{"
                        "try{"
                            "res = getStreamingLeader("+raftsGroup+");"
                            "break;"
                        "}catch(ex){};"
                    "}while(true);"
                    "print('current leader is: '+res)");
        std::thread th2 = std::thread(insert_task, hostName, HAtab, port, insert_total_rows/2);
        th2.join();

        notify.wait();

        cout << "unsubscribe the streamTable..." << endl;
        client.unsubscribe(hostName, port, HAtab, "test_backupSites");
        cout << "unsubscribe finished!" << endl;

        conn.upload("res_tradeHA", {res_tradeHA});
        auto res = conn.run("res = select * from res_tradeHA order by timestamp;ex = select * from "+HAtab+" order by timestamp;"
                            "each(eqObj, ex.values(), res.values())")->getString();
        EXPECT_EQ(res, "[1,1,1,1]");

        conn_ctl->run("startDataNode(`" + leaderName + ");sleep(1000);go;"
                      "do{sleep(1000);}while((exec state from getClusterPerf() where name = `" + leaderName + ")[0] != 1);");
    }

    usedPorts.insert(listenport);
    conn.run("try{dropStreamTable(`"+HAtab+");}catch(ex){};go;");
    conn_ctl->close();
}


TEST_P(StreamingSubscribeHighAvailableTest, test_Pollingclient_backupSites_with_error_host_port)
{
    string HAtab = createHAstreamTable();
    int msg1_total = 0;
    srand(time(0));
    string leaderName = conn.run("getStreamingLeader("+raftsGroup+")")->getString();

    DBConnectionSP conn_ctl(new DBConnection(false, false));
    conn_ctl->connect(hostName, ctl_port, "admin", "123456");

    TableSP res_tradeHA = conn.run("colNames = `timestamp`sym`qty`price;\
                                    colTypes = [TIMESTAMP,SYMBOL,INT,DOUBLE];\
                                    table(1:0,colNames,colTypes);");
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(hostName, 0, HAtab, "test_backupSites", 0, true, nullptr, false, false, "admin", "123456", nullptr, backupSites));
    }
    else
    {
        thread th1 = thread(insert_task, hostName, HAtab, port, insert_total_rows/2);
        auto queue = client.subscribe(hostName, 0, HAtab, "test_backupSites", 0, true, nullptr, false, false, "admin", "123456", nullptr, backupSites, 500, false);
        Message msg;
        auto handler = [&]{
        while (true)
        {
            queue->pop(msg);
            if(msg.isNull()) {break;}
            else{
                msg1_total += 1;
                vector<ConstantSP> rowData(msg->size());
                INDEX insertedrows = 0;
                string errMsg;
                for (int i = 0; i < msg->size(); i++)
                {
                    rowData[i] = msg->get(i);
                }
                if (!res_tradeHA->append(rowData, insertedrows, errMsg))
                {
                    cout << "append data failed with err: " << errMsg << endl;
                    break;
                }
                // if (msg1_total % 100 == 0)
                //     cout << msg1_total << endl;
                // cout << msg->getString() << endl;
                if (msg1_total == insert_total_rows)
                {
                    cout << "get all msg successfully!" << endl;
                }
            }
        } };
        ThreadSP th_getData = new Thread(new Executor(handler));

        th1.join();
        Util::sleep(1000);
        conn_ctl->run("try{stopDataNode(`" + leaderName + ")}catch(ex){};go;"
                        "do{sleep(1000);}while((exec state from getClusterPerf() where name = `" + leaderName + ")[0] != 0);"
                        "print('stop leader DataNode successfully!');");
        conn.run("do{"
                        "try{"
                            "res = getStreamingLeader("+raftsGroup+");"
                            "break;"
                        "}catch(ex){};"
                    "}while(true);"
                    "print('current leader is: '+res)");
        thread th2 = thread(insert_task, hostName, HAtab, port, insert_total_rows/2);
        th2.join();

        cout << "unsubscribe the streamTable..." << endl;
        int actual_port = atoi(conn.run("(exec port from rpc(getControllerAlias(), getClusterPerf) where name = getStreamingLeader("+raftsGroup+"))[0]")->getString().c_str());
        client.unsubscribe(hostName, actual_port, HAtab, "test_backupSites");
        cout << "unsubscribe finished!" << endl;

        th_getData->start();
        th_getData->join();
        Util::sleep(100);
        conn.upload("res_tradeHA", {res_tradeHA});
        auto res = conn.run("res = select * from res_tradeHA order by timestamp;ex = select * from "+HAtab+" order by timestamp;"
                            "each(eqObj, ex.values(), res.values())")->getString();
        EXPECT_EQ(res, "[1,1,1,1]");

        conn_ctl->run("startDataNode(`" + leaderName + ");sleep(1000);go;"
                      "do{sleep(1000);}while((exec state from getClusterPerf() where name = `" + leaderName + ")[0] != 1);");
    }

    usedPorts.insert(listenport);
    conn.run("try{dropStreamTable(`"+HAtab+");}catch(ex){};go;");
    conn_ctl->close();
}


TEST_P(StreamingSubscribeHighAvailableTest, test_ThreadedClient_backupSites_with_error_host_port)
{
    string HAtab = createHAstreamTable();
    srand(time(0));
    string leaderName = conn.run("getStreamingLeader("+raftsGroup+")")->getString();

    DBConnectionSP conn_ctl(new DBConnection(false, false));
    conn_ctl->connect(hostName, ctl_port, "admin", "123456");

    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;
    int msg_total = 0;
    TableSP res_tradeHA = conn.run("colNames = `timestamp`sym`qty`price;\
                                    colTypes = [TIMESTAMP,SYMBOL,INT,DOUBLE];\
                                    table(1:0,colNames,colTypes);");

    auto onehandler = [&](Message msg)
    {
        LockGuard<Mutex> lock(&mutex);
        msg_total += 1;
        vector<ConstantSP> rowData(msg->size());
        INDEX insertedrows = 0;
        string errMsg;
        for (int i = 0; i < msg->size(); i++)
        {
            rowData[i] = msg->get(i);
        }
        if (!res_tradeHA->append(rowData, insertedrows, errMsg))
        {
            throw RuntimeException("append data failed with err: " + errMsg);
        }
        // if (msg_total % 100 == 0)
        //     cout << "now subscribed rows: " << msg_total << endl;
        // handle msg
        if (msg_total == insert_total_rows)
        {
            notify.set();
        }
    };

    ThreadedClient threadedClient = listenport == -1? ThreadedClient() : ThreadedClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(threadedClient.subscribe(hostName, 0, onehandler, HAtab, "test_backupSites", 0, true, nullptr, false, false, "admin", "123456", nullptr, backupSites));
    }
    else
    {
        thread th1 = thread(insert_task, hostName, HAtab, port, insert_total_rows/2);
        auto th_sub = threadedClient.subscribe(hostName, 0, onehandler, HAtab, "test_backupSites", 0, true, nullptr, false, false, "admin", "123456", nullptr, backupSites, 500, false);
        Message msg;

        th1.join();
        Util::sleep(1000);
        conn_ctl->run("try{stopDataNode(`" + leaderName + ")}catch(ex){};go;"
                        "do{sleep(1000);}while((exec state from getClusterPerf() where name = `" + leaderName + ")[0] != 0);"
                        "print('stop leader DataNode successfully!');");
        conn.run("do{"
                        "try{"
                            "res = getStreamingLeader("+raftsGroup+");"
                            "break;"
                        "}catch(ex){};"
                    "}while(true);"
                    "print('current leader is: '+res)");
        std::thread th2 = std::thread(insert_task, hostName, HAtab, port, insert_total_rows/2);
        th2.join();

        notify.wait();

        cout << "unsubscribe the streamTable..." << endl;
        int actual_port = atoi(conn.run("(exec port from rpc(getControllerAlias(), getClusterPerf) where name = getStreamingLeader("+raftsGroup+"))[0]")->getString().c_str());
        threadedClient.unsubscribe(hostName, actual_port, HAtab, "test_backupSites");
        cout << "unsubscribe finished!" << endl;
        conn.upload("res_tradeHA", {res_tradeHA});
        auto res = conn.run("res = select * from res_tradeHA order by timestamp;ex = select * from "+HAtab+" order by timestamp;"
                            "each(eqObj, ex.values(), res.values())")->getString();
        EXPECT_EQ(res, "[1,1,1,1]");

        conn_ctl->run("startDataNode(`" + leaderName + ");sleep(1000);go;"
                      "do{sleep(1000);}while((exec state from getClusterPerf() where name = `" + leaderName + ")[0] != 1);");
    }

    usedPorts.insert(listenport);
    conn.run("try{dropStreamTable(`"+HAtab+");}catch(ex){};go;");
    conn_ctl->close();
}


TEST_P(StreamingSubscribeHighAvailableTest, test_ThreadPooledClient_backupSites_with_error_host_port)
{
    string HAtab = createHAstreamTable();
    srand(time(0));
    string leaderName = conn.run("getStreamingLeader("+raftsGroup+")")->getString();

    DBConnectionSP conn_ctl(new DBConnection(false, false));
    conn_ctl->connect(hostName, ctl_port, "admin", "123456");

    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;
    int msg_total = 0;
    TableSP res_tradeHA = conn.run("colNames = `timestamp`sym`qty`price;\
                                    colTypes = [TIMESTAMP,SYMBOL,INT,DOUBLE];\
                                    table(1:0,colNames,colTypes);");

    auto onehandler = [&](Message msg)
    {
        LockGuard<Mutex> lock(&mutex);
        msg_total += 1;
        vector<ConstantSP> rowData(msg->size());
        INDEX insertedrows = 0;
        string errMsg;
        for (int i = 0; i < msg->size(); i++)
        {
            rowData[i] = msg->get(i);
        }
        if (!res_tradeHA->append(rowData, insertedrows, errMsg))
        {
            throw RuntimeException("append data failed with err: " + errMsg);
        }
        // if (msg_total % 100 == 0)
        //     cout << "now subscribed rows: " << msg_total << endl;
        // handle msg
        if (msg_total == insert_total_rows)
        {
            notify.set();
        }
    };

    ThreadPooledClient client = listenport == -1? ThreadPooledClient() : ThreadPooledClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(hostName, 0, onehandler, HAtab, "test_backupSites", 0, true, nullptr, false, false, "admin", "123456", nullptr, backupSites));
    }
    else
    {
        thread th1 = thread(insert_task, hostName, HAtab, port, insert_total_rows/2);
        auto thv_sub = client.subscribe(hostName, 0, onehandler, HAtab, "test_backupSites", 0, true, nullptr, false, false, "admin", "123456", nullptr, backupSites, 500, false);
        Message msg;

        th1.join();
        Util::sleep(1000);
        conn_ctl->run("try{stopDataNode(`" + leaderName + ")}catch(ex){};go;"
                        "do{sleep(1000);}while((exec state from getClusterPerf() where name = `" + leaderName + ")[0] != 0);"
                        "print('stop leader DataNode successfully!');");
        conn.run("do{"
                        "try{"
                            "res = getStreamingLeader("+raftsGroup+");"
                            "break;"
                        "}catch(ex){};"
                    "}while(true);"
                    "print('current leader is: '+res)");
        std::thread th2 = std::thread(insert_task, hostName, HAtab, port, insert_total_rows/2);
        th2.join();

        notify.wait();

        cout << "unsubscribe the streamTable..." << endl;
        int actual_port = atoi(conn.run("(exec port from rpc(getControllerAlias(), getClusterPerf) where name = getStreamingLeader("+raftsGroup+"))[0]")->getString().c_str());
        client.unsubscribe(hostName, actual_port, HAtab, "test_backupSites");
        cout << "unsubscribe finished!" << endl;

        conn.upload("res_tradeHA", {res_tradeHA});
        auto res = conn.run("res = select * from res_tradeHA order by timestamp;ex = select * from "+HAtab+" order by timestamp;"
                            "each(eqObj, ex.values(), res.values())")->getString();
        EXPECT_EQ(res, "[1,1,1,1]");

        conn_ctl->run("startDataNode(`" + leaderName + ");sleep(1000);go;"
                      "do{sleep(1000);}while((exec state from getClusterPerf() where name = `" + leaderName + ")[0] != 1);");
    }

    usedPorts.insert(listenport);
    conn.run("try{dropStreamTable(`"+HAtab+");}catch(ex){};go;");
    conn_ctl->close();
}


TEST_P(StreamingSubscribeHighAvailableTest, test_Pollingclient_backupSites_resub_false)
{
    string HAtab = createHAstreamTable();
    int msg1_total = 0;
    srand(time(0));
    string leaderName = conn.run("getStreamingLeader("+raftsGroup+")")->getString();

    DBConnectionSP conn_ctl(new DBConnection(false, false));
    conn_ctl->connect(hostName, ctl_port, "admin", "123456");

    TableSP res_tradeHA = conn.run("colNames = `timestamp`sym`qty`price;\
                                    colTypes = [TIMESTAMP,SYMBOL,INT,DOUBLE];\
                                    table(1:0,colNames,colTypes);");
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(hostName, port, HAtab, "test_backupSites", 0, false, nullptr, false, false, "admin", "123456", nullptr, backupSites));
    }
    else
    {
        thread th1 = thread(insert_task, hostName, HAtab, port, insert_total_rows/2);
        auto queue = client.subscribe(hostName, port, HAtab, "test_backupSites", 0, false, nullptr, false, false, "admin", "123456", nullptr, backupSites, 500, false);
        Message msg;
        auto handler = [&]{
        while (true)
        {
            queue->pop(msg);
            if(msg.isNull()) {break;}
            else{
                msg1_total += 1;
                vector<ConstantSP> rowData(msg->size());
                INDEX insertedrows = 0;
                string errMsg;
                for (int i = 0; i < msg->size(); i++)
                {
                    rowData[i] = msg->get(i);
                }
                if (!res_tradeHA->append(rowData, insertedrows, errMsg))
                {
                    cout << "append data failed with err: " << errMsg << endl;
                    break;
                }
                // if (msg1_total % 100 == 0)
                //     cout << msg1_total << endl;
                // cout << msg->getString() << endl;
                if (msg1_total == insert_total_rows/2)
                {
                    break;
                }
            }
        } };
        ThreadSP th_getData = new Thread(new Executor(handler));

        th1.join();
        Util::sleep(1000);
        conn_ctl->run("try{stopDataNode(`" + leaderName + ")}catch(ex){};go;"
                        "do{sleep(1000);}while((exec state from getClusterPerf() where name = `" + leaderName + ")[0] != 0);"
                        "print('stop leader DataNode successfully!');");
        conn.run("do{"
                        "try{"
                            "res = getStreamingLeader("+raftsGroup+");"
                            "break;"
                        "}catch(ex){};"
                    "}while(true);"
                    "print('current leader is: '+res)");
        thread th2 = thread(insert_task, hostName, HAtab, port, insert_total_rows/2);
        th2.join();

        cout << "unsubscribe the streamTable..." << endl;
        EXPECT_ANY_THROW(client.unsubscribe(hostName, port, HAtab, "test_backupSites")); // resub=false, will not reconnect after disconnecting from the previous leader node

        th_getData->start();
        th_getData->join();
        Util::sleep(100);
        EXPECT_EQ(res_tradeHA->rows(), insert_total_rows/2);

        conn_ctl->run("startDataNode(`" + leaderName + ");sleep(1000);go;"
                      "do{sleep(1000);}while((exec state from getClusterPerf() where name = `" + leaderName + ")[0] != 1);");
    }

    usedPorts.insert(listenport);
    conn.run("try{dropStreamTable(`"+HAtab+");}catch(ex){};go;");
    conn_ctl->close();
}


TEST_P(StreamingSubscribeHighAvailableTest, test_ThreadedClient_onehandler_backupSites_resub_false)
{
    string HAtab = createHAstreamTable();
    srand(time(0));
    string leaderName = conn.run("getStreamingLeader("+raftsGroup+")")->getString();

    DBConnectionSP conn_ctl(new DBConnection(false, false));
    conn_ctl->connect(hostName, ctl_port, "admin", "123456");

    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;
    int msg_total = 0;
    TableSP res_tradeHA = conn.run("colNames = `timestamp`sym`qty`price;\
                                    colTypes = [TIMESTAMP,SYMBOL,INT,DOUBLE];\
                                    table(1:0,colNames,colTypes);");

    auto onehandler = [&](Message msg)
    {
        LockGuard<Mutex> lock(&mutex);
        msg_total += 1;
        vector<ConstantSP> rowData(msg->size());
        INDEX insertedrows = 0;
        string errMsg;
        for (int i = 0; i < msg->size(); i++)
        {
            rowData[i] = msg->get(i);
        }
        if (!res_tradeHA->append(rowData, insertedrows, errMsg))
        {
            throw RuntimeException("append data failed with err: " + errMsg);
        }
        // if (msg_total % 100 == 0)
        //     cout << "now subscribed rows: " << msg_total << endl;
        // handle msg
        if (msg_total == insert_total_rows/2)
        {
            notify.set();
        }
    };

    ThreadedClient threadedClient = listenport == -1? ThreadedClient() : ThreadedClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, onehandler, HAtab, "test_backupSites", 0, false, nullptr, false, false, "admin", "123456", nullptr, backupSites));
    }
    else
    {
        thread th1 = thread(insert_task, hostName, HAtab, port, insert_total_rows/2);
        auto th_sub = threadedClient.subscribe(hostName, port, onehandler, HAtab, "test_backupSites", 0, false, nullptr, false, false, "admin", "123456", nullptr, backupSites, 500, false);
        Message msg;

        th1.join();
        Util::sleep(1000);
        conn_ctl->run("try{stopDataNode(`" + leaderName + ")}catch(ex){};go;"
                        "do{sleep(1000);}while((exec state from getClusterPerf() where name = `" + leaderName + ")[0] != 0);"
                        "print('stop leader DataNode successfully!');");
        conn.run("do{"
                        "try{"
                            "res = getStreamingLeader("+raftsGroup+");"
                            "break;"
                        "}catch(ex){};"
                    "}while(true);"
                    "print('current leader is: '+res)");
        std::thread th2 = std::thread(insert_task, hostName, HAtab, port, insert_total_rows/2);
        th2.join();

        notify.wait();

        cout << "unsubscribe the streamTable..." << endl;
        EXPECT_ANY_THROW(threadedClient.unsubscribe(hostName, port, HAtab, "test_backupSites"));

        EXPECT_EQ(res_tradeHA->rows(), insert_total_rows/2);

        conn_ctl->run("startDataNode(`" + leaderName + ");sleep(1000);go;"
                      "do{sleep(1000);}while((exec state from getClusterPerf() where name = `" + leaderName + ")[0] != 1);");
    }

    usedPorts.insert(listenport);
    conn.run("try{dropStreamTable(`"+HAtab+");}catch(ex){};go;");
    conn_ctl->close();
}


TEST_P(StreamingSubscribeHighAvailableTest, test_ThreadedClient_batchhandler_backupSites_resub_false)
{
    string HAtab = createHAstreamTable();
    srand(time(0));
    string leaderName = conn.run("getStreamingLeader("+raftsGroup+")")->getString();

    DBConnectionSP conn_ctl(new DBConnection(false, false));
    conn_ctl->connect(hostName, ctl_port, "admin", "123456");

    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;
    int msg_total = 0;
    TableSP res_tradeHA = conn.run("colNames = `timestamp`sym`qty`price;\
                                    colTypes = [TIMESTAMP,SYMBOL,INT,DOUBLE];\
                                    table(1:0,colNames,colTypes);");

    auto batchhandler = [&](vector<Message> msgs)
    {
        LockGuard<Mutex> lock(&mutex);
        for (auto &msg : msgs)
        {
            msg_total += 1;
            vector<ConstantSP> rowData(msg->size());
            INDEX insertedrows = 0;
            string errMsg;
            for (int i = 0; i < msg->size(); i++)
            {
                rowData[i] = msg->get(i);
            }
            if (!res_tradeHA->append(rowData, insertedrows, errMsg))
            {
                throw RuntimeException("append data failed with err: " + errMsg);
            }
            // if (msg_total % 100 == 0)
            //     cout << "now subscribed rows: " << msg_total << endl;
            // handle msg
            if (msg_total == insert_total_rows/2)
            {
                notify.set();
            }
        }
    };

    ThreadedClient threadedClient = listenport == -1? ThreadedClient() : ThreadedClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(threadedClient.subscribe(hostName, port, batchhandler, HAtab, "test_backupSites", 0, false, nullptr, false, 100, 1.0, false, "admin", "123456", nullptr, backupSites));
    }
    else
    {
        thread th1 = thread(insert_task, hostName, HAtab, port, insert_total_rows/2);
        auto th_sub = threadedClient.subscribe(hostName, port, batchhandler, HAtab, "test_backupSites", 0, false, nullptr, false, 100, 1.0, false, "admin", "123456", nullptr, backupSites, 500, false);
        Message msg;

        th1.join();
        Util::sleep(1000);
        conn_ctl->run("try{stopDataNode(`" + leaderName + ")}catch(ex){};go;"
                        "do{sleep(1000);}while((exec state from getClusterPerf() where name = `" + leaderName + ")[0] != 0);"
                        "print('stop leader DataNode successfully!');");
        conn.run("do{"
                        "try{"
                            "res = getStreamingLeader("+raftsGroup+");"
                            "break;"
                        "}catch(ex){};"
                    "}while(true);"
                    "print('current leader is: '+res)");
        std::thread th2 = std::thread(insert_task, hostName, HAtab, port, insert_total_rows/2);
        th2.join();

        notify.wait();

        cout << "unsubscribe the streamTable..." << endl;
        EXPECT_ANY_THROW(threadedClient.unsubscribe(hostName, port, HAtab, "test_backupSites"));

        EXPECT_EQ(res_tradeHA->rows(), insert_total_rows/2);

        conn_ctl->run("startDataNode(`" + leaderName + ");sleep(1000);go;"
                      "do{sleep(1000);}while((exec state from getClusterPerf() where name = `" + leaderName + ")[0] != 1);");
    }

    usedPorts.insert(listenport);
    conn.run("try{dropStreamTable(`"+HAtab+");}catch(ex){};go;");
    conn_ctl->close();
}


TEST_P(StreamingSubscribeHighAvailableTest, test_ThreadPooledClient_backupSites_resub_false)
{
    string HAtab = createHAstreamTable();
    srand(time(0));
    string leaderName = conn.run("getStreamingLeader("+raftsGroup+")")->getString();

    DBConnectionSP conn_ctl(new DBConnection(false, false));
    conn_ctl->connect(hostName, ctl_port, "admin", "123456");

    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;
    Signal notify;
    Mutex mutex;
    int msg_total = 0;
    TableSP res_tradeHA = conn.run("colNames = `timestamp`sym`qty`price;\
                                    colTypes = [TIMESTAMP,SYMBOL,INT,DOUBLE];\
                                    table(1:0,colNames,colTypes);");

    auto onehandler = [&](Message msg)
    {
        LockGuard<Mutex> lock(&mutex);
        msg_total += 1;
        vector<ConstantSP> rowData(msg->size());
        INDEX insertedrows = 0;
        string errMsg;
        for (int i = 0; i < msg->size(); i++)
        {
            rowData[i] = msg->get(i);
        }
        if (!res_tradeHA->append(rowData, insertedrows, errMsg))
        {
            throw RuntimeException("append data failed with err: " + errMsg);
        }
        // if (msg_total % 100 == 0)
        //     cout << "now subscribed rows: " << msg_total << endl;
        // handle msg
        if (msg_total == insert_total_rows/2)
        {
            notify.set();
        }
    };

    ThreadPooledClient client = listenport == -1? ThreadPooledClient() : ThreadPooledClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, HAtab, "test_backupSites", 0, false, nullptr, false, false, "admin", "123456", nullptr, backupSites));
    }
    else
    {
        thread th1 = thread(insert_task, hostName, HAtab, port, insert_total_rows/2);
        auto thv_sub = client.subscribe(hostName, port, onehandler, HAtab, "test_backupSites", 0, false, nullptr, false, false, "admin", "123456", nullptr, backupSites, 500, false);
        Message msg;

        th1.join();
        Util::sleep(1000);
        conn_ctl->run("try{stopDataNode(`" + leaderName + ")}catch(ex){};go;"
                        "do{sleep(1000);}while((exec state from getClusterPerf() where name = `" + leaderName + ")[0] != 0);"
                        "print('stop leader DataNode successfully!');");
        conn.run("do{"
                        "try{"
                            "res = getStreamingLeader("+raftsGroup+");"
                            "break;"
                        "}catch(ex){};"
                    "}while(true);"
                    "print('current leader is: '+res)");
        std::thread th2 = std::thread(insert_task, hostName, HAtab, port, insert_total_rows/2);
        th2.join();

        notify.wait();

        cout << "unsubscribe the streamTable..." << endl;
        EXPECT_ANY_THROW(client.unsubscribe(hostName, port, HAtab, "test_backupSites"));
        EXPECT_EQ(res_tradeHA->rows(), insert_total_rows/2);

        conn_ctl->run("startDataNode(`" + leaderName + ");sleep(1000);go;"
                      "do{sleep(1000);}while((exec state from getClusterPerf() where name = `" + leaderName + ")[0] != 1);");
    }

    usedPorts.insert(listenport);
    conn.run("try{dropStreamTable(`"+HAtab+");}catch(ex){};go;");
    conn_ctl->close();
}



TEST_P(StreamingSubscribeHighAvailableTest, test_Pollingclient_backupSites_subOnce)
{
    string HAtab = createHAstreamTable();
    int msg1_total = 0;
    srand(time(0));
    string leaderName = conn.run("getStreamingLeader("+raftsGroup+")")->getString();

    DBConnectionSP conn_ctl(new DBConnection(false, false));
    conn_ctl->connect(hostName, ctl_port, "admin", "123456");

    TableSP res_tradeHA = conn.run("colNames = `timestamp`sym`qty`price;\
                                    colTypes = [TIMESTAMP,SYMBOL,INT,DOUBLE];\
                                    table(1:0,colNames,colTypes);");
    int listenport = GetParam();
    cout << "current listenport is " << listenport << endl;

    PollingClient client = listenport == -1? PollingClient() : PollingClient(listenport);
    if (!isNewServer(conn, 2, 0, 8) && listenport == 0)
    {
        EXPECT_ANY_THROW(client.subscribe(hostName, port, HAtab, "test_backupSites", 0, true, nullptr, false, false, "admin", "123456", nullptr, backupSites));
    }
    else
    {
        thread th1 = thread(insert_task, hostName, HAtab, port, insert_total_rows/2);
        auto queue = client.subscribe(hostName, port, HAtab, "test_backupSites", 0, true, nullptr, false, false, "admin", "123456", nullptr, backupSites, 500, true);
        Message msg;
        auto handler = [&]{
        while (true)
        {
            queue->pop(msg);
            if(msg.isNull()) {break;}
            else{
                msg1_total += 1;
                vector<ConstantSP> rowData(msg->size());
                INDEX insertedrows = 0;
                string errMsg;
                for (int i = 0; i < msg->size(); i++)
                {
                    rowData[i] = msg->get(i);
                }
                if (!res_tradeHA->append(rowData, insertedrows, errMsg))
                {
                    cout << "append data failed with err: " << errMsg << endl;
                    break;
                }
                // if (msg1_total % 100 == 0)
                //     cout << msg1_total << endl;
                // cout << msg->getString() << endl;
                if (msg1_total == insert_total_rows)
                {
                    break;
                }
            }
        } };
        ThreadSP th_getData = new Thread(new Executor(handler));

        th1.join();
        Util::sleep(1000);
        conn_ctl->run("try{stopDataNode(`" + leaderName + ")}catch(ex){};go;"
                        "do{sleep(1000);}while((exec state from getClusterPerf() where name = `" + leaderName + ")[0] != 0);"
                        "print('stop leader DataNode successfully!');");

        conn.run("do{"
                        "try{"
                            "res = getStreamingLeader("+raftsGroup+");"
                            "break;"
                        "}catch(ex){};"
                    "}while(true);"
                    "print('current leader is: '+res)");
        thread th2 = thread(insert_task, hostName, HAtab, port, insert_total_rows/2);
        th2.join();

        cout << "unsubscribe the streamTable..." << endl;
        client.unsubscribe(hostName, port, HAtab, "test_backupSites");

        th_getData->start();
        th_getData->join();
        Util::sleep(100);
        conn.upload("res_tradeHA", {res_tradeHA});
        auto res = conn.run("res = select * from res_tradeHA order by timestamp;ex = select * from "+HAtab+" order by timestamp;"
                            "each(eqObj, ex.values(), res.values())")->getString();
        EXPECT_EQ(res, "[1,1,1,1]");

        conn_ctl->run("startDataNode(`" + leaderName +");sleep(1000);go;"
                      "do{sleep(1000);}while(all(exec state from getClusterPerf() where name =`" + leaderName + ") != 1);");
    }

    usedPorts.insert(listenport);
    conn.run("try{dropStreamTable(`"+HAtab+");}catch(ex){};go;");
    conn_ctl->close();
}