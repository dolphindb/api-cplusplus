#include <iostream>
#include "Streaming.h"
using namespace std;
using namespace dolphindb;
using namespace std::chrono;
#ifdef WINDOWS
#define sleep(x) Sleep(x)
#endif

const string host = "192.168.1.124";
const int port = 8922;

int main() {
    int cnt = 0;
    unsigned long long total = 0;
    auto then = system_clock::now();
    auto handler = [&](Message msg) {
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

    auto queue = client.subscribe(host, port, "trades", DEFAULT_ACTION_NAME, 0);
    Message msg;
    while (true) {
        if (queue->poll(msg, 1000)) {
            handler(msg);
        }
    }
}
