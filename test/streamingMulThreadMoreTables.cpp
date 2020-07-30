#include "config.h"
void pubTables(const vector<string> tables){
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

int getStreamingOffset(DBConnection * conn, const string& tableName){
	string script = "getStreamingStat().pubTables";
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
class Count{
public:
	Count(int x = 0): c(0){};
	~Count(){
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
int subScriteTableTest(const string& host, int port, int listenPort, const vector<string>& tables) {
	DBConnection connSelf;
    bool ret = connSelf.connect(host,port);
    if(!ret){
		cout<<"Failed to connect to the server"<<endl;
		return 0;
	}

    Count total;
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

int main(){
    DBConnection::initialize();
    bool ret = conn.connect(hostName,port);
    if(!ret){
		cout<<"Failed to connect to the server"<<endl;
		return 0;
	}
    vector<string> tables = {"s1","s2","s3","s4","s5","s6","s7","s8","s9","s10"};
    pubTables(tables);
    vector<std::thread> ts;
    for(unsigned int i = 0; i < tables.size(); i++){
    	ts.push_back(std::thread(subScriteTableTest,hostName, port, listenPorts[i], tables));
    }

    for(auto &t : ts)
    	t.join();
    return 0;
}
