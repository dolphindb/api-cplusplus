class Count_MultiThread{
public:
	Count_MultiThread(int x = 0): c(0){};
	~Count_MultiThread(){
		cout<<"destructing the Count"<<endl;
	}
	void increase(int x){
		m.lock();
		c += x;
		m.unlock();
	}
private:
	Mutex m;
	int c;
};

void pubTables1(const vector<string> tables){
	for(unsigned int i = 0 ;i < tables.size(); i++){
		string t = tables[i];
		string script =
				"share streamTable(100:0,`id`num,[LONG,LONG]) as " + t + ";\
				enableTablePersistence(" + t + ",true,true,10000000);\
				def writeData(t){\
					index = 0;\
					do{\
						num = rand(6..10,1)[0];\
						insert into " + t + " values(index..(index + num - 1) , 1..num );\
						sleep(num * 100);\
						index += num;\
					}while(true);\
				};\
				submitJob('ss','ss',writeData{" + t + "})";
		conn.run(script);
	}
}
int subScriteTableTest2(const string& host, int port, int listenPort, const vector<string>& tables) {
	DBConnection connSelf;
    bool ret = connSelf.connect(host,port);
    if(!ret){
		cout<<"Failed to connect to the server"<<endl;
		return 0;
	}

    Count_MultiThread total;
    auto handler = [&](Message msg) {
    	int size = msg->get(0)->getInt();
        total.increase(size);
        long long max = 0;
        for(int i = 0 ;i < size; i++){
        	max +=  msg->get(0)->getLong(i);
        }
    };
    int threadNum = rand() % 10;
    ThreadPooledClient client(listenPort, threadNum);
    for(int i = 0 ;i < 100; i++){
    	vector<vector<ThreadSP>> vs;
    	for(auto & table : tables){
    		 auto vec = client.subscribe(host, port, handler, table, table, 0);
    		 vs.push_back(vec);
    	}
    	Util::sleep(50);

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


TEST(streamingMulThreadMoreTables,test_streamingMulThreadMoreTables){
    vector<string> tables = {"s1","s2","s3","s4","s5","s6","s7","s8","s9","s10"};
    pubTables1(tables);
    vector<std::thread> ts;
    for(unsigned int i = 0; i < tables.size(); i++){
    	ts.push_back(std::thread(subScriteTableTest2,hostName, port, listenPorts[i], tables));
    }

    for(auto &t : ts)
    	t.join();
}

