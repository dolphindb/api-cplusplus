#include <unistd.h>
#include <chrono>
#include <iostream>


TEST(StreamingThreadPooledClientTester,test_StreamingThreadPooledClientTester){
    atomic_int cnt;
    cnt = 0;
    atomic<unsigned long long> total;
    total = 0;

    auto then = system_clock::now();
    auto handler = [&](Message msg) {
        ConstantSP res = msg->get(5);
        //printf("%d",res->size());
        size_t len = res->size();
        for (int i=0; i<len; i++){
            ConstantSP row = res->getRow(i);
            //long long value = row->getLong();
            //Constant* time = Util::createTimestamp(value);
            //string value = row->getString(i);
            //Constant* symv = Util::createString(value);
            //int value = row->getInt(i);
            //Constant* qtyv = Util::createInt(value);
            //double value = row->getDouble(i);
            //Constant* pricev = Util::createDouble(value);
            //string value = row->getString(i);
            //Constant* exchv = Util::createString(value);
            long long value = row->getLong();
            Constant* x = Util::createLong(value); //收到的数据符合预期
            cout<<x->getLong()<<endl;
        }

        if (cnt == 0) then = system_clock::now();
        cnt += msg->get(0)->size();
        total += msg->get(0)->size();

        if (cnt >= 2000) {
            auto now = system_clock::now();
            auto dur = now - then;
            auto ms = duration_cast<milliseconds>(dur).count();
            cout << cnt << " messages took " << ms << " ms, throughtput: " << 1.0 * cnt / ms * 1000.0 << " messages/s"
                 << endl;
            cnt = 0;
        }
    };

    srand(time(0));
    int listenport = rand() % 8000 + 1025;
    ThreadPooledClient client(listenport, 10);

//    while (true) {
        auto vec = client.subscribe(hostName, port, handler, table, DEFAULT_ACTION_NAME, 0);
        //auto vec = client.subscribe(host, port, handler, table1, DEFAULT_ACTION_NAME, 0);//host不存在或者table不存在成功，会显示不存在这个表或无法连接到这个服务器。
        //auto vec1 = client.subscribe(host, port, handler, table, DEFAULT_ACTION_NAME, 0);//重复订阅没有报错，服务器端仍然只显示1个订阅者
        sleep(1);
        client.unsubscribe(hostName, port, table); //反订阅成功
        for (auto& t : vec) {
            t->join();
        }
        /*for (auto& t : vec1) {
            t->join();
        }*/
        cout << "total: " << total << endl;
   // }
}
