class DBConnectReconnectTest:public testing::Test
{
protected:
    //Suite
    static void SetUpTestCase() {
        //DBConnection conn;
        conn.initialize();
        bool ret = conn.connect(hostName, port, "admin", "123456", "", false, vector<string>(), 7200, true);
        if (!ret) {
            cout << "Failed to connect to the server" << endl;
        }
        else {
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
            
		}
    }
    virtual void TearDown()
    {
        pass;
    }
};

void shellStopCurNode(vector<string> nodes, string cur_node){
    DBConnection conn1(false,false);
    conn1.connect(hostName, ctl_port, "admin", "123456");

    conn1.run("try{stopDataNode(\"" + cur_node + "\")}catch(ex){};");
    cout<< cur_node + " has stopped..."<<endl;
    std::this_thread::sleep_for(std::chrono::seconds(5));
    // std::this_thread::yield();
    conn1.run("try{startDataNode(\"" + cur_node + "\")}catch(ex){};");
    cout<< "restart the datanode: "+cur_node +" succeed..."<<endl;
    conn1.close();
}

bool assertUnConnect(){
    Util::sleep(2000);

    DBConnection conn2(false,false);
    cout <<"check if unconnected..."<<endl;
    try{
        testing::internal::CaptureStdout();
        conn2.connect(hostName, port, "admin", "123456");
        std::string output = testing::internal::GetCapturedStdout();
        string ex_out = "Failed to connect to host = ";
        ex_out += hostName ;
        ex_out +=" port = ";
        ex_out += to_string(port);
        ex_out += " with error code 111\n";
        // cout<< ex_out;
        return (output == ex_out);
    }
    catch(exception& ex){
        cout<< ex.what()<<endl;
        return false;
    }
    
}

// void job(){
//     conn.connect(hostName, port, "admin", "123456", "", false, vector<string>(), 7200, true);
//     while (true)
//     {
//         cout<<conn.run("version()")->getString()<<endl;
//         Util::sleep(1000);
//     }
    
// }

TEST_F(DBConnectReconnectTest,test_NotHaReconnect){
    bool res;
    string cur_node = conn.run("getNodeAlias()")->getString();

    std::thread t1= std::thread(shellStopCurNode, nodeNames, cur_node);
    std::thread t2= std::thread([&]{res = assertUnConnect();});

    t1.join();
    t2.join();

    Util::sleep(10000);

    EXPECT_EQ(res,true);
    cout <<"check if reconnected..."<<endl;
    EXPECT_EQ(conn.run("1+1")->getInt(), 2);
    cout <<"check passed..."<<endl;
    // std::thread t1= std::thread(job);
    // t1.join();

}
