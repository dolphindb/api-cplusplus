#include <iostream>
#include "config.h"
#include "Streaming.h"
using namespace std;
using namespace dolphindb;
using namespace std::chrono;
#ifdef WINDOWS
#define sleep(x) Sleep(x)
#endif



int main() {
    int cnt = 0;
    int n = 10;
    unsigned long long total = 0;
    auto then = system_clock::now();
    auto handler = [&](Message msg) {
        ConstantSP res = msg->get(0);
        //printf("%d",res->size());
        size_t len = res->size();
        for (int i=0; i<len; i++){
            ConstantSP row = res->getRow(i);
            long long value = row->getLong();
            Constant* time = Util::createTimestamp(value);
            //string value = row->getString(i);
            //Constant* symv = Util::createString(value);
            //int value = row->getInt(i);
            //Constant* qtyv = Util::createInt(value);
            //double value = row->getDouble(i);
            //Constant* pricev = Util::createDouble(value);
            //string value = row->getString(i);
            //Constant* exchv = Util::createString(value);
            //long long value = row->getLong();
            //Constant* x = Util::createLong(value); //收到的数据符合预期
            cout<<time->getString()<<endl;
        }
        if (cnt == 0) then = system_clock::now();
        ++cnt, ++total;
        if (cnt >= 20000) {
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
    PollingClient client(listenport);

    //auto queue = client.subscribe(hostName, port, "trades", DEFAULT_ACTION_NAME, 0);
    auto queue = client.subscribe(hostName, port,table, DEFAULT_ACTION_NAME, 0);
    Message msg;
    while (true) {
        if (queue->poll(msg, 1000)) {
            handler(msg);
        }
        /*srand(time(0));
        sleep(rand() % n + 1); //反订阅成功
        printf("wakeup!\n");
        client.unsubscribe(hostName, port, table,DEFAULT_ACTION_NAME);*/
    }

}
