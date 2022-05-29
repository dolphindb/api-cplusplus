void pubTables2(const vector<string> tables){
	for(unsigned int i = 0 ;i < tables.size(); i++){
		string t = tables[i];
		string script =
				"share streamTable(100:0,`id`num,[LONG,LONG]) as " + t + ";\
				enableTablePersistence(" + t + ",true,true,10000000);\
				def writeData(t){\
					index = 0;\
					do{\
						num = rand(3..6,1)[0];\
						insert into " + t + " values(index..(index + num - 1) , 1..num );\
						sleep(num * 100);\
						index += num;\
					}while(true);\
				};\
				submitJob('ss','ss',writeData{" + t + "})";
		conn.run(script);
	}
}

int getStreamingOffset(DBConnection * conn, const string& tableName){
	string script = "getStreamingStat().pubTables2";
	TableSP t = conn->run(script);
	if(t->size() <= 0)
		return -1;
	VectorSP names = t->getColumn("tableName");
	VectorSP offset = t->getColumn("msgOffset");

	for( int i = 0 ;i < names->size(); i++){
		if(names->getString(i) == tableName)
			return offset->getLong(i);
	}
	return -1;
}


int subScriteTableTest3(const string& host, int port, int listenPort, const string& tableName) {
	DBConnection connSelf;
    bool ret = connSelf.connect(host,port);
    if(!ret){
		cout<<"Failed to connect to the server"<<endl;
		return 0;
	}
    std::atomic<int> total(0);
    auto handler = [&](Message msg) {
        total += msg->get(0)->getInt();
    };
    int threadNum = 10;
    ThreadPooledClient client(listenPort, threadNum);
    for(int i = 0 ;i < 100; i++){
        auto vec = client.subscribe(host, port, handler, tableName, tableName, 0);
        long long offset = getStreamingOffset(&connSelf, tableName);
        Util::sleep(50);
        client.unsubscribe(host, port, tableName, tableName);
        offset = getStreamingOffset(&connSelf, tableName);
        for (auto& t : vec) {
            t->join();
        }
    }
    return 0;
}


TEST(streamingMultithreadSubUnsub,test_streamingMultithreadSubUnsub){
    vector<string> tables = {"s1","s2","s3","s4","s5","s6","s7","s8","s9","s10"};
    pubTables2(tables);
    vector<std::thread> ts;
    for(unsigned int i = 0; i < tables.size(); i++){
    	ts.push_back(std::thread(subScriteTableTest3,hostName, port, listenPorts[i],  tables[i]));
    }

    for(auto &t : ts)
    	t.join();
}