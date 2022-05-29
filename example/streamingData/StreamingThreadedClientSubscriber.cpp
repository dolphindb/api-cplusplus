#include "Streaming.h"
#include <iostream>
#include <thread>
using namespace std;
using namespace dolphindb;
using namespace std::chrono;

class Executor : public dolphindb::Runnable {
  using Func = std::function<void()>;

public:
  explicit Executor(Func f) : func_(std::move(f)){};
  void run() override { func_(); };

private:
  Func func_;
};

const string host = "192.168.1.102";
const int serverport = 8899;

int main() {
  srand(time(0));
  int listenport = rand() % 1000 + 50000;
  ThreadedClient client(listenport);

  vector<ThreadSP> tmp;
  ThreadSP t = new Thread(new Executor([=, &tmp, &client]() {
    DBConnection conn;
    try {
      bool ret = conn.connect(host, serverport, "admin", "123456");
      cout << "connected to the server" << endl;
      if (!ret) {
        cout << "Failed to connect to the server " << host << endl;
        return;
      }
    } catch (exception &ex) {
      cout << "Failed to  connect  with error: " << ex.what();
      return;
    }

    auto handler = [&](Message msg) {
      TableSP t = (TableSP)msg;
      cout << "received message size is " << t->size()
           << "msg type:" << msg->getType() << endl;
      cout << t->getColumn(0)->getLong(0) << endl;
      cout << t->getColumn(17)->getString(0) << endl;
      cout << t->getColumn(18)->getString(0) << endl;
      cout << t->getColumn(19)->getString(0) << endl;

      try {
        vector<ConstantSP> args;
        args.push_back(t);
        conn.run("tableInsert{objByName(`sub1)}", args);
        // conn.run("tableInsert{loadTable('dfs://dolphindb', `demoTable)}",
        // args);
      } catch (exception &ex) {
        cout << "Failed to  run  with error: " << ex.what() << endl;
      }
    };

    auto t1 = client.subscribe(host, serverport, handler, "st1",
                               "threadedClientSub", 0, true, nullptr,true);
    t1->join();
  }));
  t->start();
  tmp.emplace_back(t);

  for (auto t : tmp) {
    t->join();
  }
}
