#pragma once
#include <string>
#include <vector>
#include "Logger.h"
#include "DolphinDB.h"

// single 
extern const std::string HOST, USER, PASSWD;
extern const int PORT;

// cluster 
extern const std::string HOST_CLUSTER, USER_CLUSTER, PASSWD_CLUSTER;
extern const int PORT_CONTROLLER, PORT_AGENT, PORT_DNODE1, PORT_DNODE2, PORT_DNODE3, PORT_CNODE1;

extern std::vector<std::string> sites;
extern std::vector<std::string> nodeNames;
extern dolphindb::DLogger::Level default_level;

std::string getRandString(int len);
std::string getCaseName();
std::string getCaseNameHash();
dolphindb::TableSP AnyVectorToTable(dolphindb::VectorSP vec);
