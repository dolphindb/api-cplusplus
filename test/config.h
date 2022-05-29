#include "../include/DolphinDB.h"
#include "../include/Util.h"
#include "../include/BatchTableWriter.h"
#include "../include/MultithreadedTableWriter.h"
#include "Streaming.h"
#include <vector>
#include <string>
#include <climits>
#include <thread>
#include <atomic>
#include <cstdio>
#include <random>
//#include <sys/time.h>
//#include <bits/stl_vector.h>
#include "ctime"

using namespace dolphindb;
using namespace std;
using std::endl;
using std::cout;
using std::atomic_long;

extern string hostName;
extern string host1;
extern string errCode;
extern int port;
extern string table;
extern vector<int> listenPorts;
extern string alphas;
extern int pass, fail;
extern bool assertObj;
extern int vecSize;


extern int const INDEX_MAX_1;
extern int const INDEX_MIN_2;

using namespace std::chrono;


string hostName = "192.168.1.30";
string host1 = hostName;
string errCode = "0";
int port = 8902;
string table = "trades";
vector<int> listenPorts = { 18901,18902,18903,18904,18905,18906,18907,18908,18909,18910 };
string alphas = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
int pass, fail;
bool assertObj = true;
int vecSize = 20;

int const INDEX_MAX_1=1;
int const INDEX_MIN_2=-1;

static DBConnection conn(false, false);
static DBConnection conn_compress(false, false, 7200, true);
static DBConnectionPool pool(hostName, port, 10, "admin", "123456");
