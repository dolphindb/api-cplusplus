#include "DolphinDB.h"
#include "Streaming.h"
#include "stdio.h"
#include "Util.h"
#include <thread>
#include <string>

using namespace dolphindb;
using namespace std;


void PollingClient_sub(string hostName, int port, string table, string action, string target_dbName, string target_dfs_tableName)
{
    PollingClient client(0);
    srand(time(NULL));
    auto queue = client.subscribe(hostName, port, table, action, 0, true, nullptr, true);
    Message msg;
    ThreadSP th1 = new Thread(new Executor([&]
                                           {
    while (true)
    {
        queue->pop(msg);
        if(msg.isNull()) {break;}
        else{
            DBConnection conn(false, false);
            conn.connect(hostName, port, "admin", "123456");
            AutoFitTableAppender appender(target_dbName, target_dfs_tableName, conn);
            bool succeeded = false; 
            while(!succeeded){
                try
                {
                    appender.append(msg);
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    std::cerr << "[PollingClient]: throwed a exception, which is \n" << e.what() << '\n';
                    cout<< "retrying..."<<endl;
                    Util::sleep(100);
                }
            }

            int rows = conn.run("(exec count(*) from loadTable('"+target_dbName+"','"+target_dfs_tableName+"'))[0]")->getInt();
            cout<<"[PollingClient]: total saved rows from subscription: " << rows <<endl;
            conn.close();
        }
    } }));

    th1->start();
    th1->join();
}

void ThreadedClient_sub(string hostName, int port, string table, string action, string target_dbName, string target_dfs_tableName)
{
    ThreadedClient threadedClient(0);

    auto onehandler = [&](Message msg)
    {
        DBConnection conn(false, false);
        conn.connect(hostName, port, "admin", "123456");
        AutoFitTableAppender appender(target_dbName, target_dfs_tableName, conn);
        bool succeeded = false; 
        while(!succeeded){
            try
            {
                appender.append(msg);
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                std::cerr << "[ThreadedClient]: throwed a exception, which is \n" << e.what() << '\n';
                cout<< "retrying..."<<endl;
                Util::sleep(100);
            }
        }
        int rows = conn.run("(exec count(*) from loadTable('" + target_dbName + "','" + target_dfs_tableName + "'))[0]")->getInt();
        cout << "[ThreadedClient]: total saved rows from subscription: " << rows << endl;
        conn.close();
    };

    auto thread = threadedClient.subscribe(hostName, port, onehandler, table, action, 0, true, nullptr, true);
    thread->start();
    thread->join();
}

void ThreadPooledClient_sub(string hostName, int port, string table, string action, string target_dbName, string target_dfs_tableName)
{
    ThreadPooledClient client(0, 10);

    auto onehandler = [&](Message msg)
    {
        DBConnection conn(false, false);
        conn.connect(hostName, port, "admin", "123456");
        AutoFitTableAppender appender(target_dbName, target_dfs_tableName, conn);
        bool succeeded = false; 
        while(!succeeded){
            try
            {
                appender.append(msg);
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                std::cerr << "[ThreadPooledClient]: throwed a exception, which is \n" << e.what() << '\n';
                cout<< "retrying..."<<endl;
                Util::sleep(100);
            }
        }
        int rows = conn.run("(exec count(*) from loadTable('" + target_dbName + "','" + target_dfs_tableName + "'))[0]")->getInt();
        cout << "[ThreadPooledClient]: total saved rows from subscription: " << rows << endl;
        conn.close();
    };

    auto threadV = client.subscribe(hostName, port, onehandler, table, action, 0, true, nullptr, true);
    for (auto &thread : threadV)
    {
        thread->start();
        thread->join();
    }
}


void ThreadedClient_sub_batch(string hostName, int port, string table, string action, string target_dbName, string target_dfs_tableName)
{
    ThreadedClient threadedClient(0);

    auto batchhandler = [&](vector<Message> msgs)
    {
        for (auto &msg : msgs){
            DBConnection conn(false, false);
            conn.connect(hostName, port, "admin", "123456");
            AutoFitTableAppender appender(target_dbName, target_dfs_tableName, conn);
            bool succeeded = false; 
            while(!succeeded){
                try
                {
                    appender.append(msg);
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    std::cerr << "[ThreadedClient_batch]: throwed a exception, which is \n" << e.what() << '\n';
                    cout<< "retrying..."<<endl;
                    Util::sleep(100);
                }
            }
            int rows = conn.run("(exec count(*) from loadTable('" + target_dbName + "','" + target_dfs_tableName + "'))[0]")->getInt();
            cout << "[ThreadedClient_batch]: total saved rows from subscription: " << rows << endl;
            conn.close();
        }
        
    };

    auto thread = threadedClient.subscribe(hostName, port, batchhandler, table, action, 0, true, nullptr,false, 1, 1, true);
    thread->start();
    thread->join();
}


TableSP AnyVectorToTable(VectorSP vec)
{
    vector<DATA_TYPE> colTypes;
    vector<string> colNames;
    auto col_count = vec->size();
    for(auto i=0;i<col_count;i++){
        colTypes.emplace_back(vec->get(i)->getType());
        colNames.emplace_back("col"+ to_string(i));
    }
    TableSP tmp = Util::createTable(colNames, colTypes, 0, 1);
	vector<VectorSP> columnVecs;
    INDEX row = 1;
    string err;
    vector<ConstantSP> row_vals;
    for(auto i=0;i<col_count;i++){
        row_vals.emplace_back(vec->get(i));
    }
    bool succeeded = tmp->append(row_vals, row, err);
    if(!succeeded){
        throw RuntimeException("create table failed");
    }
    return tmp;
}

void PollingClient_sub_d(string hostName, int port, string table, string action, vector<string> target_dbNames, vector<string> target_dfs_tableNames, StreamDeserializerSP sdsp)
{
    PollingClient client(0);
    srand(time(NULL));
    auto queue = client.subscribe(hostName, port, table, action, 0, true, nullptr, false, false, "admin", "123456", sdsp);
    string db1 = target_dbNames[0];
    string db2 = target_dbNames[1];
    string dfstable1 = target_dfs_tableNames[0];
    string dfstable2 = target_dfs_tableNames[1];
    Message msg;
    ThreadSP th1 = new Thread(new Executor([&]
                                           {
    while (true)
    {
        queue->pop(msg);
        if(msg.isNull()) {break;}
        else{
            string symbolflag = msg.getSymbol();
            DBConnection conn(false, false);
            conn.connect(hostName, port, "admin", "123456");
            string db = "";
            string table = "";
            if(symbolflag == "msg1"){
                db = db1;
                table = dfstable1;
            }
            else if (symbolflag == "msg2"){
                db = db2;
                table = dfstable2;
            }
            AutoFitTableAppender appender(db, table, conn);
            bool succeeded = false; 
            TableSP tab = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    appender.append(tab);
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    std::cerr << "[PollingClient]: throwed a exception, which is \n" << e.what() << '\n';
                    cout<< "retrying..."<<endl;
                    Util::sleep(100);
                }
            }

            int rows = conn.run("(exec count(*) from loadTable('"+db+"','"+table+"'))[0]")->getInt();
            cout<<"[PollingClient]: total saved rows from subscription: " << rows <<endl;
            conn.close();
        }
    } }));

    th1->start();
    th1->join();
}

void ThreadedClient_sub_d(string hostName, int port, string table, string action, vector<string> target_dbNames, vector<string> target_dfs_tableNames, StreamDeserializerSP sdsp)
{
    ThreadedClient threadedClient(0);
    string db1 = target_dbNames[0];
    string db2 = target_dbNames[1];
    string dfstable1 = target_dfs_tableNames[0];
    string dfstable2 = target_dfs_tableNames[1];
    auto onehandler = [&](Message msg)
    {
        string symbolflag = msg.getSymbol();
        DBConnection conn(false, false);
        conn.connect(hostName, port, "admin", "123456");
        string db = "";
        string table = "";
        if(symbolflag == "msg1"){
            db = db1;
            table = dfstable1;
        }
        else if (symbolflag == "msg2"){
            db = db2;
            table = dfstable2;
        }
        AutoFitTableAppender appender(db, table, conn);
        bool succeeded = false; 
        TableSP tab = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                appender.append(tab);
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                std::cerr << "[ThreadedClient]: throwed a exception, which is \n" << e.what() << '\n';
                cout<< "retrying..."<<endl;
                Util::sleep(100);
            }
        }

        int rows = conn.run("(exec count(*) from loadTable('"+db+"','"+table+"'))[0]")->getInt();
        cout<<"[ThreadedClient]: total saved rows from subscription: " << rows <<endl;
        conn.close();
    };

    auto thread = threadedClient.subscribe(hostName, port, onehandler, table, action, 0, true, nullptr, false, false, "admin", "123456", sdsp);
    thread->start();
    thread->join();
}

void ThreadPooledClient_sub_d(string hostName, int port, string table, string action, vector<string> target_dbNames, vector<string> target_dfs_tableNames, StreamDeserializerSP sdsp)
{
    ThreadPooledClient client(0, 10);
    string db1 = target_dbNames[0];
    string db2 = target_dbNames[1];
    string dfstable1 = target_dfs_tableNames[0];
    string dfstable2 = target_dfs_tableNames[1];
    auto onehandler = [&](Message msg)
    {
        string symbolflag = msg.getSymbol();
        DBConnection conn(false, false);
        conn.connect(hostName, port, "admin", "123456");
        string db = "";
        string table = "";
        if(symbolflag == "msg1"){
            db = db1;
            table = dfstable1;
        }
        else if (symbolflag == "msg2"){
            db = db2;
            table = dfstable2;
        }
        AutoFitTableAppender appender(db, table, conn);
        bool succeeded = false; 
        TableSP tab = AnyVectorToTable(msg);
        while(!succeeded){
            try
            {
                appender.append(tab);
                succeeded = true;
            }
            catch(const std::exception& e)
            {
                std::cerr << "[ThreadPooledClient]: throwed a exception, which is \n" << e.what() << '\n';
                cout<< "retrying..."<<endl;
                Util::sleep(100);
            }
        }

        int rows = conn.run("(exec count(*) from loadTable('"+db+"','"+table+"'))[0]")->getInt();
        cout<<"[ThreadPooledClient]: total saved rows from subscription: " << rows <<endl;
        conn.close();
    };

    auto threadV = client.subscribe(hostName, port, onehandler, table, action, 0, true, nullptr, false, false, "admin", "123456", sdsp);
    for (auto &thread : threadV)
    {
        thread->start();
        thread->join();
    }
}

void ThreadedClient_sub_batch_d(string hostName, int port, string table, string action, vector<string> target_dbNames, vector<string> target_dfs_tableNames, StreamDeserializerSP sdsp)
{
    ThreadedClient threadedClient(0);
    string db1 = target_dbNames[0];
    string db2 = target_dbNames[1];
    string dfstable1 = target_dfs_tableNames[0];
    string dfstable2 = target_dfs_tableNames[1];
    auto batchhandler = [&](vector<Message> msgs)
    {
        for (auto &msg : msgs)
        {
            string symbolflag = msg.getSymbol();
            DBConnection conn(false, false);
            conn.connect(hostName, port, "admin", "123456");
            string db = "";
            string table = "";
            if(symbolflag == "msg1"){
                db = db1;
                table = dfstable1;
            }
            else if (symbolflag == "msg2"){
                db = db2;
                table = dfstable2;
            }
            AutoFitTableAppender appender(db, table, conn);
            bool succeeded = false; 
            TableSP tab = AnyVectorToTable(msg);
            while(!succeeded){
                try
                {
                    appender.append(tab);
                    succeeded = true;
                }
                catch(const std::exception& e)
                {
                    std::cerr << "[ThreadedClient_batch]: throwed a exception, which is \n" << e.what() << '\n';
                    cout<< "retrying..."<<endl;
                    Util::sleep(100);
                }
            }

            int rows = conn.run("(exec count(*) from loadTable('"+db+"','"+table+"'))[0]")->getInt();
            cout<<"[ThreadedClient_batch]: total saved rows from subscription: " << rows <<endl;
            conn.close();
        }
    };

    auto thread = threadedClient.subscribe(hostName, port, batchhandler, table, action, 0, true, nullptr, false, 1, 1, false, "admin", "123456", sdsp);
    thread->start();
    thread->join();
}