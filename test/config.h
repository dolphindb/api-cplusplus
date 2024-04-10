#pragma once
#include "gtest/gtest.h"
#include "DolphinDB.h"
#include "Util.h"
#include "BatchTableWriter.h"
#include "MultithreadedTableWriter.h"
#include "ConstantImp.h"
#include "Streaming.h"
#include "ConstantMarshall.h"
#include "TableImp.h"
#include "ConstantFactory.h"
#include "Format.h"
#include "DFSChunkMeta.h"
#include "Logger.h"
// #include "Database.h"
// #include "Utility.h"
// #include "DBTable.h"
// #include "QueryWrapper.h"
#include <vector>
#include <string>
#include <climits>
#include <thread>
#include <atomic>
#include <cstdio>
#include <random>
#include <fstream>
#include "ctime"
#include <cstdlib>
#include <stdexcept>

using namespace dolphindb;
using namespace std;
using std::atomic_long;
using std::cout;
using std::endl;

extern string hostName;
extern string errCode;
extern int port, ctl_port;
extern string table;
extern vector<int> listenPorts;
extern string alphas;
extern int pass, fail;
extern bool assertObj;
extern int vecSize;
extern unordered_set<int> usedPorts;

extern int const INDEX_MAX_1;
extern int const INDEX_MIN_2;

extern vector<string> sites;
extern vector<string> backupSites;
extern string raftsGroup;
extern vector<string> nodeNames;

extern DBConnection conn;
extern DBConnection connReconn;
extern DBConnection conn_compress;
extern DBConnectionSP connsp;

// check server version
bool isNewServer(DBConnection &conn, const int &major, const int &minor, const int &revision);

void checkAPIVersion();
string getRandString(int len);
