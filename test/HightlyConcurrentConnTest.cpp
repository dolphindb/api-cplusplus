
#include "DolphinDB.h"
#include "Util.h"
#include "Streaming.h"
#include <vector>
#include <string>
#include <thread>

using namespace dolphindb;
using namespace std;

const string hostName = "192.168.0.16";
const int port = 9005;

bool createPartitionedTable(DBConnection &conn, string dbName, string tableName, int partitionNums){
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \""+dbName+"\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,VALUE,date(1.."+to_string(partitionNums)+"));";
	script += "dummy = table(1:0, `id`sym`date`value, [INT, SYMBOL, DATE, DOUBLE]);";
	script += ""+tableName+" = db.createPartitionedTable(dummy,`"+tableName+",`date);";
    conn.run(script);
    return true;
}

void job_insertToPartitionedTable(string dbName, string tableName, int partition){
    DBConnection conn(false,false);
    conn.connect(hostName, port, "admin", "123456");
    string str = "dummy = table(1:0, `id`sym`date`value, [INT, SYMBOL, DATE, DOUBLE]);"
                "tableInsert(dummy,rand(10000,1),rand(`a`b`c`d`e`f`g`h`i`j`k`l`m`n,1),date("+to_string(partition)+"),rand(10000.00,1));"
                "loadTable(\""+dbName+"\",`"+tableName+").append!(dummy)";
    conn.run(str);
    Util::sleep(1000);
    conn.close();
    // cout<<"insert finished! "<<endl;
}

int selectFromPartitionedTable(DBConnection &conn, string dbName, string tableName){
    string str = "exec count(*) from loadTable(\""+dbName+"\",`"+tableName+")";
    return conn.run(str)->getInt();
}

void test_HightlyConcurrentConn_insertToPartitionedTable(int size) {
    DBConnection conn(false,false);
    conn.connect(hostName, port, "admin", "123456");
    string db = "dfs://test_dfsDemo";
    string tab = "dfs_table";

    if(createPartitionedTable(conn,db, tab, size))
        cout<<"create database and partitionedtable successfully"<<endl;
	vector<thread> th(size);
    int sum=0;
	for (int i = 0; i < size; i++){
		th[i] = std::thread(job_insertToPartitionedTable, db, tab, i);
	}
	for (int i = 0; i < size; i++){
		th[i].join();
        sum+=1;
	}
    assert(sum==size);
    assert(selectFromPartitionedTable(conn, db, tab) == size);
    cout<<"test_HightlyConcurrentConn insert to partitionedtable passed! "<<endl;

    conn.run("undef all;");
    conn.close();
}

// in-memory table

bool createinMemoryTable(DBConnection &conn, string tableName){
	string script = "colName =  `cint`csymbol`cdate`cdouble;"
                    "colType = [INT,SYMBOL,DATE,DOUBLE];"
                    "tab1=table(1:0,colName,colType);"
                    "share tab1 as "+tableName+"";
    conn.run(script);
    return true;
}

void job_insertinMemoryTable(string tableName){
    DBConnection conn(false,false);
    conn.connect(hostName, port, "admin", "123456");
    string str = "tableInsert("+tableName+",rand(10000,1),rand(`a`b`c`d`e`f`g`h`i`j`k`l`m`n,1),rand(2000.01.01..2021.12.20,1),rand(10000.00,1));";
    conn.run(str);
    conn.close();
    // cout<<"insert finished! "<<endl;
}

int selectFrominMemoryTable(DBConnection &conn, string tableName){
    string str = "exec count(*) from "+tableName+"";
    return conn.run(str)->getInt();
}


void test_HightlyConcurrentConn_insertToinMemoryTable(int size) {
    DBConnection conn(false,false);
    conn.connect(hostName, port, "admin", "123456");
    string tab = "tmp";

    if(createinMemoryTable(conn, tab))
        cout<<"create in-memory table successfully"<<endl;
	vector<thread> th(size);
    int sum=0;
	for (int i = 0; i < size; i++){
		th[i] = std::thread(job_insertinMemoryTable, tab);
	}
	for (int i = 0; i < size; i++){
		th[i].join();
        sum+=1;
	}
    assert(sum==size);
    assert(selectFrominMemoryTable(conn, tab) == size);
    cout<<"test_HightlyConcurrentConn insert to in-memory table passed! "<<endl;

    conn.run("undef all;");
    conn.close();
}


int main(){
    try{
        test_HightlyConcurrentConn_insertToPartitionedTable(500);
        test_HightlyConcurrentConn_insertToinMemoryTable(500);
    }
    catch(const std::exception& e){
        cout<<"Exception was throwed out: "<<endl<<'\t'<<e.what()<<endl;
    }

    return 0;
}