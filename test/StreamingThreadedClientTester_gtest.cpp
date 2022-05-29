#include <assert.h>
#include <iostream>

class StreamingThreadedClientTester:public testing::Test
{
protected:
    static void SetUpTestCase()
    {
        conn.connect(hostName,port);
        cout << "run script...";    
        string script="n=2000;\
        share streamTable(n:0,`time`sym`qty`price`exch`index,[TIMESTAMP,SYMBOL,INT,DOUBLE,SYMBOL,LONG]) as trades;\
        rows = 1;\
        timev = take(now(), rows);\
        symv = take(`MKFT, rows);\
        qtyv = take(112, rows);\
        pricev = take(53.75, rows);\
        exchv = take(`N, rows);\
        for (x in 0:2000) {\
            insert into trades values(timev, symv, qtyv, pricev, exchv,x);\
        }";
        conn.run(script);
        cout << "OK" << endl;   
    }
    static void TearDownTestCase()
    {
        pass;
    }
    // Some expensive resource shared by all tests.
};

// class Executor : public dolphindb::Runnable {
//     using Func = std::function<void()>;

// public:
//     explicit Executor(Func f) : func_(std::move(f)){};
//     void run() override { func_(); };

// private:
//     Func func_;
// };

TEST_F(StreamingThreadedClientTester,test_basic){
    int count=0;
    auto handler =[&](Message msg){
        ConstantSP res = msg->get(5);
        size_t len = res->size();
        for (int i=0; i<len; i++){
            ConstantSP row = res->getRow(i);
            long long value = row->getLong();
            count++;
            cout<< "Index:"+to_string(value)<<";";
        }
    };

    srand(time(0));
    int listenport = rand() % 1000 + 50000;
    ThreadedClient client(listenport);
    auto t = client.subscribe(hostName, port, handler, table, DEFAULT_ACTION_NAME,0);
    // auto t1 = client.subscribe(hostName, port, handler, table, DEFAULT_ACTION_NAME, 0);//重复订阅没报错但test程序无法自动结束
    Util::sleep(1);
    client.unsubscribe(hostName, port, table);//取消订阅成功
    t->join();
    // auto t1 = client.subscribe(hostName, port, handler, "Not exsit", DEFAULT_ACTION_NAME, 0); // host不存在或者table不存在成功，会显示不存在这个表或无法连接到这个服务器。
    // client.unsubscribe(hostName, port, table);
    // t1->join();
    
    assert(count==2000);
}

TEST_F(StreamingThreadedClientTester,test_batchSize){
    MessageBatchHandler handler =[&](vector<Message> msg){

        for (int j=0;j<msg.size();++j){
            
            ConstantSP res = msg[j]->get(5);
            size_t len = res->size();
            for (int i=0; i<len; i++){
                ConstantSP row = res->getRow(i);
                long long value = row->getLong();
                
            }
            
        }
        
        assert(msg.size()==50);//确定每一次处理数量为50
    };

    srand(time(0));
    int listenport = rand() % 1000 + 50000;
    ThreadedClient client(listenport);
    auto t = client.subscribe(hostName, port, handler, table, DEFAULT_ACTION_NAME, 0, true, nullptr, false,50, 5);
    Util::sleep(10);
    client.unsubscribe(hostName, port, table);
    t->join();
}

TEST_F(StreamingThreadedClientTester,test_throttle){
    auto Time0 = time(NULL);
    
    MessageBatchHandler handler =[&](vector<Message> msg){

        assert((time(NULL)-Time0)==5);//因为batchsize大于总msg数，确定需要等throttle的5秒之后再进行处理

        for (int j=0;j<msg.size();++j){
            
            ConstantSP res = msg[j]->get(5);
            size_t len = res->size();
            for (int i=0; i<len; i++){
                ConstantSP row = res->getRow(i);
                long long value = row->getLong();
                
            }
        }
       
    };

    srand(time(0));
    int listenport = rand() % 1000 + 50000;
    ThreadedClient client(listenport);
   
    auto t = client.subscribe(hostName, port, handler, table, DEFAULT_ACTION_NAME, 0, true, nullptr, false,5000, 5);
    Util::sleep(10);
    client.unsubscribe(hostName, port, table);
    t->join();
}

