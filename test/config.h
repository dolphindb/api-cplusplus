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

// clear all memory variables and database in DolphinDB
// #define CLEAR_ENV(_session) \
//     _session.run("\
//         def clear_env(){\
//             for (name in (exec name from objs(true) where shared=true))\
//             {\
//                 try{dropStreamTable(name, true)}catch(ex){};go;\
//                 try{undef(name, SHARED)}catch(ex){};go;\
//             };\
//         };\
//         pnodeRun(clear_env);\
//         for(db in getClusterDFSDatabases())\
//         {\
//             try{dropDatabase(db)}catch(ex){};go;\
//         };\
//         undef all;go");

// #define CLEAR_ENV2(_sessionsp) \
//     _sessionsp->run("\
//         def clear_env(){\
//             for (name in (exec name from objs(true) where shared=true))\
//             {\
//                 try{dropStreamTable(name, true)}catch(ex){};go;\
//                 try{undef(name, SHARED)}catch(ex){};go;\
//             };\
//         };\
//         pnodeRun(clear_env);\
//         for(db in getClusterDFSDatabases())\
//         {\
//             try{dropDatabase(db)}catch(ex){};go;\
//         };\
//         undef all;go");
