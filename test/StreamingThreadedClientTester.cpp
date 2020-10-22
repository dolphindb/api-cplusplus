#include <iostream>
#include <thread>
#include "config.h"
#include "Streaming.h"
using namespace std;
using namespace dolphindb;
using namespace std::chrono;
#ifdef WINDOWS
#define sleep(x) Sleep((x)*1000)
#define usleep(x) Sleep(x)
#endif

class Executor : public dolphindb::Runnable {
    using Func = std::function<void()>;

public:
    explicit Executor(Func f) : func_(std::move(f)){};
    void run() override { func_(); };

private:
    Func func_;
};


/* test-suite
n=20000
share streamTable(n:0,`time`sym`qty`price`exch`index,[TIMESTAMP,SYMBOL,INT,DOUBLE,SYMBOL,LONG]) as trades

rows = 1
timev = take(now(), rows)
symv = take(`MKFT, rows)
qtyv = take(112, rows)
pricev = take(53.75, rows)
exchv = take(`N, rows)

do {
for (x in 0:200000) {
    insert into trades values(timev, symv, qtyv, pricev, exchv,x)
}
sleep(1000)
} while(1)

*/




int main() {
    srand(time(0));
    int listenport = rand() % 1000 + 50000;
    ThreadedClient client(listenport);

    int n = 10;
    int m = 20000;

    vector<ThreadSP> tmp;
    for (int i = 0; i < n; ++i) {
        ThreadSP t = new Thread(new Executor([=, &tmp, &client]() {
            srand(time(0));
            sleep(rand() % n + 1);
            int cnt = 0;
            long long total = 0;
            auto then = system_clock::now();
            auto handler = [&](Message msg) {
                ConstantSP res = msg->get(0); //msg输出的是元祖，这里res返回的是指定的列
                //printf("%d\n", res->size());
                size_t len = res->size();
                //printf("%d\n", len);

                //输出msg的res指定列的值
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
                    cout<<time->getString()<<endl; //测试结果显示收到的数据符合预期
                }


                if (cnt == 0) then = system_clock::now();
                ++cnt, ++total;
                if (cnt == m) {
                    auto now = system_clock::now();
                    auto dur = now - then;
                    auto ms = duration_cast<milliseconds>(dur).count();
                    printf("[Thread %3d] %5d messages took %5ld ms, throughput: %10.1lf messages/s, total: %lld\n", i, m,
                           ms, 1.0 * cnt / ms * 1000.0, total);
                    cnt = 0;
                }
            };

//            for (int j = 0;; ++j) {
                auto t = client.subscribe(hostName, port, handler, table, "thread" + to_string(i), 0); //正常订阅成功
                auto t1 = client.subscribe(hostName, port, handler, table, "thread" + to_string(i), 0);//重复订阅没有报错，服务器端仍然只显示1个订阅者
                //auto t = client.subscribe(host1, port, handler, "test", "thread" + to_string(i), 0); //host不存在或者table不存在成功，会显示不存在这个表或无法连接到这个服务器
                srand(time(0));
                sleep(rand() % n + 1);
                client.unsubscribe(hostName, port, table, "thread" + to_string(i)); //反订阅成功
//                client.unsubscribe(host, port, table, "thread" + to_string(i));
                t->join();
                t1->join();
//            }
        }));
        t->start();
        tmp.emplace_back(t);
    }

    for (auto t : tmp) {
        t->join();
    }
}
