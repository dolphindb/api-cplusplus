#include <unistd.h>
#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>
#include "Streaming.h"
using namespace std;
using namespace dolphindb;
using namespace std::chrono;

#ifdef WINDOWS
#define sleep(x) Sleep(x)
#endif

const string host = "192.168.1.124";
const int port = 8922;
const auto table = "trades";

int main() {
    atomic_int cnt;
    cnt = 0;
    atomic<unsigned long long> total;
    total = 0;

    auto then = system_clock::now();
    auto handler = [&](Message msg) {
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

    while (true) {
        auto vec = client.subscribe(host, port, handler, table, DEFAULT_ACTION_NAME, 0);
        sleep(1);
        client.unsubscribe(host, port, table);
        for (auto& t : vec) {
            t->join();
        }
        cout << "total: " << total << endl;
    }
}
