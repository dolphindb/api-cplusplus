class Count{
public:
	Count(long long  x = 0): c(0){};
	~Count(){
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
	void clear(){c = 0;}
private:
	Mutex m;
	long long  c;
};

extern void pubTables(const vector<string> tables) {
	for (unsigned int i = 0; i < tables.size(); i++) {
		string t = tables[i];
		string script =
			"share streamTable(100:0,`id`num,[LONG,LONG]) as " + t + ";\
				enableTablePersistence(" + t + ",true,true,10000000);\
				def writeData(t){\
					index = 0;\
					for(i in 1..1000){\
						num = 100;\
						insert into " + t + " values(index..(index + num - 1) , 1..num );\
						index += num;\
					}\
				};\
				submitJob('ss','ss',writeData{" + t + "})";
		conn.run(script);
	}
}


int subScriteTableTest1(const string& host, int port, int listenPort, const vector<string>& tables) {
	DBConnection connSelf;
    bool ret = connSelf.connect(host,port);
    if(!ret){
		cout<<"Failed to connect to the server"<<endl;
		return 0;
	}

    Count total;
    auto handler = [&](Message msg) {
    	int size = msg->get(0)->size();
        total.increase(size);

    };
    int threadNum = rand()%10;
    ThreadPooledClient client(listenPort, threadNum);
    for(int x = 0 ; x < 100000 ; x ++){
		vector<vector<ThreadSP>> vs;
		total.clear();
		for(auto & table : tables){
			 auto vec = client.subscribe(host, port, handler, table, table, 0);
			 vs.push_back(vec);
		}
		Util::sleep(5);

		long long sum =   total.getNum();
		cout<<"the total is "<< sum<<endl;

		for(auto & table : tables){
			client.unsubscribe(host, port, table, table);
		}
		for (auto& vec : vs) {
			for(auto & t : vec)
				t->join();
		}
    }


    return 0;
}


TEST(streamingCheckNum,test_streamingCheckNum){
	vector<string> tables = {"s1","s2","s3","s4","s5","s6","s7","s8","s9","s10"};
    pubTables(tables);
    vector<std::thread> ts;
    for(unsigned int i = 0; i < tables.size(); i++){
    	ts.push_back(std::thread(subScriteTableTest1,hostName, port, listenPorts[i], tables));
    }

    for(auto &t : ts)
    	t.join();
}
