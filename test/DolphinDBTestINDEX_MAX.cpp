#include "DolphinDBTestINDEX_MAX.h"
#include "../include/DolphinDB.h"
#include "../include/Util.h"
#include "Streaming.h"
#include <vector>
#include <limits.h>
#include <thread>
#include <atomic>
#include <stdio.h>
using std::endl;
using std::cout;
static string hostName = "115.239.209.223";
static int port = 9959;
static int pass = 0;
static int fail = 0;
dolphindb::DBConnection conn(false, false);
template<typename T>
void ASSERTION(const string test, const T& ret, const T& expect) {
    if(ret != expect){
        std::cout<<"ASSERT FAIL--" << test<< "-- expect return --"<< (T)expect <<", real return--"<<(T)ret<< std::endl;
        fail++;
    }
    else    
        pass++;
}
void printTestResults(const string& fileName){
    FILE * fp = fopen(fileName.c_str(),"ab+");
    if(fp == NULL)
    	throw dolphindb::RuntimeException("Failed to open file [" + fileName + "].");
    vector<string> result;
    result.push_back("test INDEX_MAX Conflict total:" + std::to_string(pass + fail) + "\n") ;
    result.push_back("pass:" + std::to_string(pass) + "\n");
    result.push_back("fail:" + std::to_string(fail) + "\n");
    for(unsigned int i = 0 ;i < result.size(); i++)
    	fwrite(result[i].c_str(), 1, result[i].size(),fp);
}

int main(int argc, char ** argv){
    dolphindb::DBConnection::initialize();
    int dolphindbINDEX_MAX = dolphindb::INDEX_MAX;
    int dolphindbINDEX_MIN = dolphindb::INDEX_MIN;
    int constINDEX_MAX = INDEX_MAX;
    int constINDEX_MIN = INDEX_MIN;
    ASSERTION("testINDEX_MAXConflict", dolphindbINDEX_MAX, INT_MAX);
    ASSERTION("testINDEX_MAXConflict", dolphindbINDEX_MIN, INT_MIN);
    ASSERTION("testINDEX_MAXConflict", constINDEX_MAX, (int)1);
    ASSERTION("testINDEX_MAXConflict", constINDEX_MIN, (int)-1);
    bool ret = conn.connect(hostName,port);
    if(!ret){
		cout<<"Failed to connect to the server"<<endl;
		return 0;
	}
    
    if (argc >= 2) {
        string fileName(argv[1]);
        printTestResults(fileName);
    }


    return 0;
}
