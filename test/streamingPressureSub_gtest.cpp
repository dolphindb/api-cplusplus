class Count_PressureSub{
public:
	Count_PressureSub(long long  x = 0): c(0){};
	~Count_PressureSub(){
		cout<<"destructing the Count"<<endl;
	}
	void increase(long long  x){
		m.lock();
		c += x;
		m.unlock();
	}

	long long  getNum(){
		return c;
	}
private:
	Mutex m;
	long long  c;
};

void pubTables3(const vector<string> tables){
	for(unsigned int i = 0 ;i < tables.size(); i++){
		string t = tables[i];
		string script =
				"share streamTable(100:0,`id`num,[LONG,LONG]) as " + t + ";\
				enableTablePersistence(" + t + ",true,true,10000000);\
				def writeData(t){\
					index = 0;\
					for(i in 1..10000){\
						num = 1000;\
						insert into " + t + " values(index..(index + num - 1) , 1..num );\
						index += num;\
					}\
				};\
				submitJob('ss','ss',writeData{" + t + "})";
		conn.run(script);
	}
}

int getStreamingOffset1(DBConnection * conn, const string& tableName){
	string script = "getStreamingStat().pubTables3";
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

int subScriteTableTest4(const string& host, int port, int listenPort, const vector<string>& tables) {
	DBConnection connSelf;
    bool ret = connSelf.connect(host,port);
    if(!ret){
		cout<<"Failed to connect to the server"<<endl;
		return 0;
	}

    Count_PressureSub total;
    auto handler = [&](Message msg) {
    	int size = msg->get(0)->size();
        total.increase(size);

    };
    int threadNum = rand()%10;
    ThreadPooledClient client(listenPort, threadNum);

	vector<vector<ThreadSP>> vs;
	for(auto & table : tables){
		 auto vec = client.subscribe(host, port, handler, table, table, 0);
		 vs.push_back(vec);
	}
	Util::sleep(50);

	for(int i = 0 ;i < 10000000;i ++){
		cout<<"the total number is : "<<total.getNum()<<endl;
		// when sub finshed the last total number is 10000000
		Util::sleep(1000);
	}



	for(auto & table : tables){
		client.unsubscribe(host, port, table, table);
	}
	for (auto& vec : vs) {
		for(auto & t : vec)
			t->join();
	}

    return 0;
}

TEST(streamingPressureSub,test_streamingPressureSub){
    vector<string> tables = {"s1","s2","s3","s4","s5","s6","s7","s8","s9","s10"};
    pubTables3(tables);
    vector<std::thread> ts;
    for(unsigned int i = 0; i < tables.size(); i++){
    	ts.push_back(std::thread(subScriteTableTest4,hostName, port, listenPorts[i], tables));
    }

    for(auto &t : ts)
    	t.join();

}
