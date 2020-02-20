#include <iostream>
#include <thread>
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

const string host = "192.168.1.124";
const int serverport = 8922;

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

            for (int j = 0;; ++j) {
                auto t = client.subscribe(host, serverport, handler, "trades", "thread" + to_string(i), 0);
                srand(time(0));
                sleep(rand() % n + 1);
                client.unsubscribe(host, serverport, "trades", "thread" + to_string(i));
                t->join();
            }
        }));
        t->start();
        tmp.emplace_back(t);
    }

    for (auto t : tmp) {
        t->join();
    }
}
