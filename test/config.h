// #pragma once

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
#include <vector>
#include <string>
#include <climits>
#include <thread>
#include <atomic>
#include <cstdio>
#include <random>
// #include <sys/time.h>
// #include <bits/stl_vector.h>
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
extern vector<int> usedPorts;

extern int const INDEX_MAX_1;
extern int const INDEX_MIN_2;

extern vector<string> sites;
extern string raftsGroup;
extern vector<string> nodeNames;

string hostName = "192.168.0.200";
string errCode = "0";
int port = 20003;
int ctl_port = 20000;
string table = "trades";
vector<int> listenPorts = {18901, 18902, 18903, 18904, 18905, 18906, 18907, 18908, 18909, 18910};
vector<int> usedPorts = {};
string alphas = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
int pass, fail;
bool assertObj = true;
int vecSize = 20;

int const INDEX_MAX_1 = 1;
int const INDEX_MIN_2 = -1;

vector<string> sites = {"192.168.0.200:20002:P1_node1", "192.168.0.200:20012:P2_node1", "192.168.0.200:20022:P3_node1"};
string raftsGroup = "11";
vector<string> nodeNames = {"P1_node1", "P2_node1", "P3_node1"};

static DBConnection conn(false, false);
static DBConnection connReconn(false, false);
static DBConnection conn_compress(false, false, 7200, true);

// check server version
bool isNewVersion = true;

void checkAPIVersion()
{
    const char* api_version = std::getenv("API_VERSION");
    if(api_version == nullptr){
        cout<<"API_VERSION is not set, skip version check\n";
        return;
    }
    
    if (string(api_version) == ""){
        cout << "API_VERSION is not set, skip version check\n";
        return;
    }

    string actual_version = Util::VER;
    if (string(api_version) == actual_version)
    {
        cout << "version check successfully" << endl;
    }
    else{
        throw std::runtime_error("API_VERSION is not match to the env");
    }
}