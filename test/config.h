#include "../include/DolphinDB.h"
#include "../include/Util.h"
#include "Streaming.h"
#include <vector>
#include <limits.h>
#include <thread>
#include <atomic>
#include <stdio.h>
using namespace dolphindb;
using std::endl;
using std::cout;
using std::atomic_long;
static string hostName = "127.0.0.1";
static string host1 = "192.168.1.200";
static int port = 8848;
static auto table = "trades";
static vector<int> listenPorts = {18901,18902,18903,18904,18905,18906,18907,18908,18909,18910};
static string alphas = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
static int pass,fail;
DBConnection conn;