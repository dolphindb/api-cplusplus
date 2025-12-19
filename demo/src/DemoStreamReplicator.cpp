#include "StreamReplicator.h"
using namespace dolphindb;
using namespace std;

int main()
{
    std::vector<HostInfo> hosts{{"192.168.0.104", 8848, "admin", "123456"}, {"192.168.0.104", 8900, "admin", "123456"}};
    StreamReplicator rep(hosts, "testTable");
    rep.insert(1, 100.0);
    rep.insert(2, 200.0);
    return 0;
}
