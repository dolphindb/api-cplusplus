#include "../include/DolphinDB.h"
#include "../include/MultithreadedTableWriter.h"
#include "../include/BatchTableWriter.h"
#include "assert.h"
#include <iostream>
#include <fstream>

using namespace dolphindb;
using namespace std;

static string hostName;
static int port;

#ifdef WINDOWS
    static string DATA_FIRE="D:/DATA/5m.csv";
#else
    static string DATA_FIRE="/home/datas/test_data.csv";
#endif

double_t getTimeStampMs() {
	return Util::getEpochTime();
}

static double test_MultithreadedTableWriter_insert_threadCount_1(DBConnection &conn, vector<COMPRESS_METHOD> *pcompress=nullptr)
{
    cout <<"MTW_insert_threadCount_1"<< ",";
    double_t tm_start, tm_end, runTime=0;

    string script1;
    string dbName = "dfs://test_MultithreadedTableWriter";
    script1 += "dbName = \"dfs://test_MultithreadedTableWriter\"\n";
    script1 += "tableName=\"pt\"\n";
    script1 += "login(\"admin\",\"123456\")\n";
    script1 += "DATA_FIRE=\"";
    script1 += DATA_FIRE;
    script1 += "\"\n";
    script1 += "if(existsDatabase(dbName)){\n";
    script1 += " dropDatabase(dbName)\n";
    script1 += "}\n";
    script1 += "db=database(dbName,RANGE,month(1990.01.01) +((0..52)*6))\n";
    script1 += "schema=extractTextSchema(DATA_FIRE)\n";
    script1 += "table1= table(1:0,schema[`name],schema[`type])\n";
    script1 += "pt = db.createPartitionedTable(table1, `pt, `date)";
    //cout<<script1<<endl;
    conn.run(script1);
    SmartPointer<MultithreadedTableWriter> mulwrite;
    ErrorCodeInfo pErrorInfo;
    mulwrite =new MultithreadedTableWriter(hostName,port, "admin","123456",dbName,"pt",NULL,false,NULL,1000,1,1,"date",pcompress);
    TableSP bt = conn.run("t0 = loadText('"+DATA_FIRE+"');t0");
    MultithreadedTableWriter::Status status;
    vector<ConstantSP> datas;
    datas.reserve(bt->rows() * 21);
    for(int i = 0; i < bt->rows(); ++i){
        for(int j = 0; j < 21; ++j)
       datas.emplace_back(bt->getColumn(j)->get(i));
    }
    tm_start=getTimeStampMs();
    for(int i=0;i < bt->rows();i++){
        mulwrite->insert(pErrorInfo,datas[i * 21 + 0],datas[i * 21 + 1],datas[i * 21 + 2],datas[i * 21 + 3],
        datas[i * 21 + 4],datas[i * 21 + 5],datas[i * 21 + 6],datas[i * 21 + 7],datas[i * 21 + 8],
        datas[i * 21 + 9],datas[i * 21 + 10],datas[i * 21 + 11],datas[i * 21 + 12],datas[i * 21 + 13],datas[i * 21 + 14],
        datas[i * 21 + 15],datas[i * 21 + 16],datas[i * 21 + 17],datas[i * 21 + 18],datas[i * 21 + 19],datas[i * 21 + 20]);
        // (datas[i * 21 + 0],datas[i * 21 + 1],datas[i * 21 + 2],datas[i * 21 + 3],
        // datas[i * 21 + 4],datas[i * 21 + 5],datas[i * 21 + 6],datas[i * 21 + 7],datas[i * 21 + 8],
        // datas[i * 21 + 9],datas[i * 21 + 10],datas[i * 21 + 11],datas[i * 21 + 12],datas[i * 21 + 13],datas[i * 21 + 14],
        // datas[i * 21 + 15],datas[i * 21 + 16],datas[i * 21 + 17],datas[i * 21 + 18],datas[i * 21 + 19],datas[i * 21 + 20]);
       
    }
    // string script2="a= select * from pt;saveText(a,'D:/DATA/5m.csv')";
    // conn.run(script2);

    do{
        mulwrite->getStatus(status);
        // usleep(100000);
        if(status.sentRows ==  bt->rows()){
            tm_end=getTimeStampMs();
            VectorSP resVec = conn.run("pt0=exec * from pt;each(eqObj,pt0.values(),t0.values())");
            for(unsigned int i=0;i<resVec->size();i++)
                assert(resVec->get(i)->getBool());
            cout<<Util::createTimestamp(tm_start)->castTemporal(DT_DATE)->getString()<<",";
            runTime = (tm_end - tm_start)/(double)1000;
            // cout<<"test_MultithreadedTableWriter_insert_threadCount_1-> Time expended: " << runTime << "s" << endl;
            cout << runTime<<","+Util::VER<< endl;
            // double TTL = re_rows->getDouble()/runTime;
            // cout<<"TTL: "<< fixed << TTL << " rows/s"<<endl << endl;
            // cout<< fixed <<TTL<<endl;
            break;
        }
    }while (true);
    conn.run("undef all;dropDatabase(\""+dbName+"\")");
    return runTime;
}

static double test_MultithreadedTableWriter_insert_threadCount_2(DBConnection &conn, vector<COMPRESS_METHOD> *pcompress=nullptr)
{
    cout <<"MTW_insert_threadCount_2"<< ",";
    double_t tm_start, tm_end, runTime=0;

    string script1;

    string dbName = "dfs://test_MultithreadedTableWriter";
    script1 += "dbName = \"dfs://test_MultithreadedTableWriter\"\n";
    script1 += "tableName=\"pt\"\n";
    script1 += "login(\"admin\",\"123456\")\n";
    script1 += "DATA_FIRE=\"";
    script1 += DATA_FIRE;
    script1 += "\"\n";
    script1 += "if(existsDatabase(dbName)){\n";
    script1 += " dropDatabase(dbName)\n";
    script1 += "}\n";
    script1 += "db=database(dbName,RANGE,month(1990.01.01) +((0..52)*6))\n";
    script1 += "schema=extractTextSchema(DATA_FIRE)\n";
    script1 += "table1= table(1:0,schema[`name],schema[`type])\n";
    script1 += "pt = db.createPartitionedTable(table1, `pt, `date)";
    //cout<<script1<<endl;
    conn.run(script1);
    SmartPointer<MultithreadedTableWriter> mulwrite;
    ErrorCodeInfo pErrorInfo;
    mulwrite =new MultithreadedTableWriter(hostName,port, "admin","123456",dbName,"pt",NULL,false,NULL,1000,1,2,"date",pcompress);
    TableSP bt = conn.run("t0 = loadText('"+DATA_FIRE+"');t0");
    MultithreadedTableWriter::Status status;
    vector<ConstantSP> datas;
    datas.reserve(bt->rows() * 21);
    for(int i = 0; i < bt->rows(); ++i){
        for(int j = 0; j < 21; ++j)
       datas.emplace_back(bt->getColumn(j)->get(i));
    }
    tm_start=getTimeStampMs();
    for(int i=0;i < bt->rows();i++){
        mulwrite->insert(pErrorInfo,datas[i * 21 + 0],datas[i * 21 + 1],datas[i * 21 + 2],datas[i * 21 + 3],
        datas[i * 21 + 4],datas[i * 21 + 5],datas[i * 21 + 6],datas[i * 21 + 7],datas[i * 21 + 8],
        datas[i * 21 + 9],datas[i * 21 + 10],datas[i * 21 + 11],datas[i * 21 + 12],datas[i * 21 + 13],datas[i * 21 + 14],
        datas[i * 21 + 15],datas[i * 21 + 16],datas[i * 21 + 17],datas[i * 21 + 18],datas[i * 21 + 19],datas[i * 21 + 20]);
        // (datas[i * 21 + 0],datas[i * 21 + 1],datas[i * 21 + 2],datas[i * 21 + 3],
        // datas[i * 21 + 4],datas[i * 21 + 5],datas[i * 21 + 6],datas[i * 21 + 7],datas[i * 21 + 8],
        // datas[i * 21 + 9],datas[i * 21 + 10],datas[i * 21 + 11],datas[i * 21 + 12],datas[i * 21 + 13],datas[i * 21 + 14],
        // datas[i * 21 + 15],datas[i * 21 + 16],datas[i * 21 + 17],datas[i * 21 + 18],datas[i * 21 + 19],datas[i * 21 + 20]);
       
    }
    // string script2="a= select * from pt;saveText(a,'D:/DATA/5m.csv')";
    // conn.run(script2);

    do{
        mulwrite->getStatus(status);
        // usleep(100000);
        if(status.sentRows ==  bt->rows()){
            tm_end=getTimeStampMs();
            VectorSP resVec = conn.run("pt0=exec * from pt;each(eqObj,pt0.values(),t0.values())");
            for(unsigned int i=0;i<resVec->size();i++)
                assert(resVec->get(i)->getBool());
            cout<<Util::createTimestamp(tm_start)->castTemporal(DT_DATE)->getString()<<",";
            runTime = (tm_end - tm_start)/(double)1000;
            // cout<<"test_MultithreadedTableWriter_insert_threadCount_1-> Time expended: " << runTime << "s" << endl;
            cout << runTime<<","+Util::VER<< endl;
            // double TTL = re_rows->getDouble()/runTime;
            // cout<<"TTL: "<< fixed << TTL << " rows/s"<<endl << endl;
            // cout<< fixed <<TTL<<endl;
            break;
        }
    }while (true);
    conn.run("undef all;dropDatabase(\""+dbName+"\")");
    return runTime;
}


static double test_MultithreadedTableWriter_insert_threadCount_4(DBConnection &conn, vector<COMPRESS_METHOD> *pcompress=nullptr)
{
    cout <<"MTW_insert_threadCount_4"<< ",";
    double_t tm_start, tm_end, runTime=0;

    string script1;

    string dbName = "dfs://test_MultithreadedTableWriter";
    script1 += "dbName = \"dfs://test_MultithreadedTableWriter\"\n";
    script1 += "tableName=\"pt\"\n";
    script1 += "login(\"admin\",\"123456\")\n";
    script1 += "DATA_FIRE=\"";
    script1 += DATA_FIRE;
    script1 += "\"\n";
    script1 += "if(existsDatabase(dbName)){\n";
    script1 += " dropDatabase(dbName)\n";
    script1 += "}\n";
    script1 += "db=database(dbName,RANGE,month(1990.01.01) +((0..52)*6))\n";
    script1 += "schema=extractTextSchema(DATA_FIRE)\n";
    script1 += "table1= table(1:0,schema[`name],schema[`type])\n";
    script1 += "pt = db.createPartitionedTable(table1, `pt, `date)";
    //cout<<script1<<endl;
    conn.run(script1);
    SmartPointer<MultithreadedTableWriter> mulwrite;
    ErrorCodeInfo pErrorInfo;
    mulwrite =new MultithreadedTableWriter(hostName,port, "admin","123456",dbName,"pt",NULL,false,NULL,1000,1,4,"date",pcompress);
    TableSP bt = conn.run("t0 = loadText('"+DATA_FIRE+"');t0");
    MultithreadedTableWriter::Status status;
    vector<ConstantSP> datas;
    datas.reserve(bt->rows() * 21);
    for(int i = 0; i < bt->rows(); ++i){
        for(int j = 0; j < 21; ++j)
       datas.emplace_back(bt->getColumn(j)->get(i));
    }
    tm_start=getTimeStampMs();
    for(int i=0;i < bt->rows();i++){
        mulwrite->insert(pErrorInfo,datas[i * 21 + 0],datas[i * 21 + 1],datas[i * 21 + 2],datas[i * 21 + 3],
        datas[i * 21 + 4],datas[i * 21 + 5],datas[i * 21 + 6],datas[i * 21 + 7],datas[i * 21 + 8],
        datas[i * 21 + 9],datas[i * 21 + 10],datas[i * 21 + 11],datas[i * 21 + 12],datas[i * 21 + 13],datas[i * 21 + 14],
        datas[i * 21 + 15],datas[i * 21 + 16],datas[i * 21 + 17],datas[i * 21 + 18],datas[i * 21 + 19],datas[i * 21 + 20]);
        // (datas[i * 21 + 0],datas[i * 21 + 1],datas[i * 21 + 2],datas[i * 21 + 3],
        // datas[i * 21 + 4],datas[i * 21 + 5],datas[i * 21 + 6],datas[i * 21 + 7],datas[i * 21 + 8],
        // datas[i * 21 + 9],datas[i * 21 + 10],datas[i * 21 + 11],datas[i * 21 + 12],datas[i * 21 + 13],datas[i * 21 + 14],
        // datas[i * 21 + 15],datas[i * 21 + 16],datas[i * 21 + 17],datas[i * 21 + 18],datas[i * 21 + 19],datas[i * 21 + 20]);
       
    }
    // string script2="a= select * from pt;saveText(a,'D:/DATA/5m.csv')";
    // conn.run(script2);

    do{
        mulwrite->getStatus(status);
        // usleep(100000);
        if(status.sentRows ==  bt->rows()){
            tm_end=getTimeStampMs();
            VectorSP resVec = conn.run("pt0=exec * from pt;each(eqObj,pt0.values(),t0.values())");
            for(unsigned int i=0;i<resVec->size();i++)
                assert(resVec->get(i)->getBool());
            cout<<Util::createTimestamp(tm_start)->castTemporal(DT_DATE)->getString()<<",";
            runTime = (tm_end - tm_start)/(double)1000;
            // cout<<"test_MultithreadedTableWriter_insert_threadCount_1-> Time expended: " << runTime << "s" << endl;
            cout << runTime<<","+Util::VER<< endl;
            // double TTL = re_rows->getDouble()/runTime;
            // cout<<"TTL: "<< fixed << TTL << " rows/s"<<endl << endl;
            // cout<< fixed <<TTL<<endl;
            break;
        }
    }while (true);
    conn.run("undef all;dropDatabase(\""+dbName+"\")");
    return runTime;
}

static double test_MultithreadedTableWriter_insert_threadCount_8(DBConnection &conn, vector<COMPRESS_METHOD> *pcompress=nullptr)
{
    cout <<"MTW_insert_threadCount_8"<< ",";
    double_t tm_start, tm_end, runTime=0;

    string script1;

    string dbName = "dfs://test_MultithreadedTableWriter";
    script1 += "dbName = \"dfs://test_MultithreadedTableWriter\"\n";
    script1 += "tableName=\"pt\"\n";
    script1 += "login(\"admin\",\"123456\")\n";
    script1 += "DATA_FIRE=\"";
    script1 += DATA_FIRE;
    script1 += "\"\n";
    script1 += "if(existsDatabase(dbName)){\n";
    script1 += " dropDatabase(dbName)\n";
    script1 += "}\n";
    script1 += "db=database(dbName,RANGE,month(1990.01.01) +((0..52)*6))\n";
    script1 += "schema=extractTextSchema(DATA_FIRE)\n";
    script1 += "table1= table(1:0,schema[`name],schema[`type])\n";
    script1 += "pt = db.createPartitionedTable(table1, `pt, `date)";
    //cout<<script1<<endl;
    conn.run(script1);
    SmartPointer<MultithreadedTableWriter> mulwrite;
    ErrorCodeInfo pErrorInfo;
    mulwrite =new MultithreadedTableWriter(hostName,port, "admin","123456",dbName,"pt",NULL,false,NULL,1000,1,8,"date",pcompress);
    TableSP bt = conn.run("t0 = loadText('"+DATA_FIRE+"');t0");
    MultithreadedTableWriter::Status status;
    vector<ConstantSP> datas;
    datas.reserve(bt->rows() * 21);
    for(int i = 0; i < bt->rows(); ++i){
        for(int j = 0; j < 21; ++j)
       datas.emplace_back(bt->getColumn(j)->get(i));
    }
    tm_start=getTimeStampMs();
    for(int i=0;i < bt->rows();i++){
        mulwrite->insert(pErrorInfo,datas[i * 21 + 0],datas[i * 21 + 1],datas[i * 21 + 2],datas[i * 21 + 3],
        datas[i * 21 + 4],datas[i * 21 + 5],datas[i * 21 + 6],datas[i * 21 + 7],datas[i * 21 + 8],
        datas[i * 21 + 9],datas[i * 21 + 10],datas[i * 21 + 11],datas[i * 21 + 12],datas[i * 21 + 13],datas[i * 21 + 14],
        datas[i * 21 + 15],datas[i * 21 + 16],datas[i * 21 + 17],datas[i * 21 + 18],datas[i * 21 + 19],datas[i * 21 + 20]);
        // (datas[i * 21 + 0],datas[i * 21 + 1],datas[i * 21 + 2],datas[i * 21 + 3],
        // datas[i * 21 + 4],datas[i * 21 + 5],datas[i * 21 + 6],datas[i * 21 + 7],datas[i * 21 + 8],
        // datas[i * 21 + 9],datas[i * 21 + 10],datas[i * 21 + 11],datas[i * 21 + 12],datas[i * 21 + 13],datas[i * 21 + 14],
        // datas[i * 21 + 15],datas[i * 21 + 16],datas[i * 21 + 17],datas[i * 21 + 18],datas[i * 21 + 19],datas[i * 21 + 20]);
       
    }
    // string script2="a= select * from pt;saveText(a,'D:/DATA/5m.csv')";
    // conn.run(script2);

    do{
        mulwrite->getStatus(status);
        // usleep(100000);
        if(status.sentRows ==  bt->rows()){
            tm_end=getTimeStampMs();
            VectorSP resVec = conn.run("pt0=exec * from pt;each(eqObj,pt0.values(),t0.values())");
            for(unsigned int i=0;i<resVec->size();i++)
                assert(resVec->get(i)->getBool());
            cout<<Util::createTimestamp(tm_start)->castTemporal(DT_DATE)->getString()<<",";
            runTime = (tm_end - tm_start)/(double)1000;
            // cout<<"test_MultithreadedTableWriter_insert_threadCount_1-> Time expended: " << runTime << "s" << endl;
            cout << runTime<<","+Util::VER<< endl;
            // double TTL = re_rows->getDouble()/runTime;
            // cout<<"TTL: "<< fixed << TTL << " rows/s"<<endl << endl;
            // cout<< fixed <<TTL<<endl;
            break;
        }
    }while (true);
    conn.run("undef all;dropDatabase(\""+dbName+"\")");
    return runTime;
}

static double test_MultithreadedTableWriter_insert_threadCount_16(DBConnection &conn, vector<COMPRESS_METHOD> *pcompress=nullptr)
{
    cout <<"MTW_insert_threadCount_16"<< ",";
    double_t tm_start, tm_end, runTime=0;

    string script1;

    string dbName = "dfs://test_MultithreadedTableWriter";
    script1 += "dbName = \"dfs://test_MultithreadedTableWriter\"\n";
    script1 += "tableName=\"pt\"\n";
    script1 += "login(\"admin\",\"123456\")\n";
    script1 += "DATA_FIRE=\"";
    script1 += DATA_FIRE;
    script1 += "\"\n";
    script1 += "if(existsDatabase(dbName)){\n";
    script1 += " dropDatabase(dbName)\n";
    script1 += "}\n";
    script1 += "db=database(dbName,RANGE,month(1990.01.01) +((0..52)*6))\n";
    script1 += "schema=extractTextSchema(DATA_FIRE)\n";
    script1 += "table1= table(1:0,schema[`name],schema[`type])\n";
    script1 += "pt = db.createPartitionedTable(table1, `pt, `date)";
    //cout<<script1<<endl;
    conn.run(script1);
    SmartPointer<MultithreadedTableWriter> mulwrite;
    ErrorCodeInfo pErrorInfo;
    mulwrite =new MultithreadedTableWriter(hostName,port, "admin","123456",dbName,"pt",NULL,false,NULL,1000,1,16,"date",pcompress);
    TableSP bt = conn.run("t0 = loadText('"+DATA_FIRE+"');t0");
    MultithreadedTableWriter::Status status;
    vector<ConstantSP> datas;
    datas.reserve(bt->rows() * 21);
    for(int i = 0; i < bt->rows(); ++i){
        for(int j = 0; j < 21; ++j)
       datas.emplace_back(bt->getColumn(j)->get(i));
    }
    tm_start=getTimeStampMs();
    for(int i=0;i < bt->rows();i++){
        mulwrite->insert(pErrorInfo,datas[i * 21 + 0],datas[i * 21 + 1],datas[i * 21 + 2],datas[i * 21 + 3],
        datas[i * 21 + 4],datas[i * 21 + 5],datas[i * 21 + 6],datas[i * 21 + 7],datas[i * 21 + 8],
        datas[i * 21 + 9],datas[i * 21 + 10],datas[i * 21 + 11],datas[i * 21 + 12],datas[i * 21 + 13],datas[i * 21 + 14],
        datas[i * 21 + 15],datas[i * 21 + 16],datas[i * 21 + 17],datas[i * 21 + 18],datas[i * 21 + 19],datas[i * 21 + 20]);
        // (datas[i * 21 + 0],datas[i * 21 + 1],datas[i * 21 + 2],datas[i * 21 + 3],
        // datas[i * 21 + 4],datas[i * 21 + 5],datas[i * 21 + 6],datas[i * 21 + 7],datas[i * 21 + 8],
        // datas[i * 21 + 9],datas[i * 21 + 10],datas[i * 21 + 11],datas[i * 21 + 12],datas[i * 21 + 13],datas[i * 21 + 14],
        // datas[i * 21 + 15],datas[i * 21 + 16],datas[i * 21 + 17],datas[i * 21 + 18],datas[i * 21 + 19],datas[i * 21 + 20]);
       
    }
    // string script2="a= select * from pt;saveText(a,'D:/DATA/5m.csv')";
    // conn.run(script2);

    do{
        mulwrite->getStatus(status);
        // usleep(100000);
        if(status.sentRows ==  bt->rows()){
            tm_end=getTimeStampMs();
            VectorSP resVec = conn.run("pt0=exec * from pt;each(eqObj,pt0.values(),t0.values())");
            for(unsigned int i=0;i<resVec->size();i++)
                assert(resVec->get(i)->getBool());
            cout<<Util::createTimestamp(tm_start)->castTemporal(DT_DATE)->getString()<<",";
            runTime = (tm_end - tm_start)/(double)1000;
            // cout<<"test_MultithreadedTableWriter_insert_threadCount_1-> Time expended: " << runTime << "s" << endl;
            cout << runTime<<","+Util::VER<< endl;
            // double TTL = re_rows->getDouble()/runTime;
            // cout<<"TTL: "<< fixed << TTL << " rows/s"<<endl << endl;
            // cout<< fixed <<TTL<<endl;
            break;
        }
    }while (true);
    conn.run("undef all;dropDatabase(\""+dbName+"\")");
    return runTime;
}

static double test_MultithreadedTableWriter_insert_threadCount_32(DBConnection &conn, vector<COMPRESS_METHOD> *pcompress=nullptr)
{
    cout <<"MTW_insert_threadCount_32"<< ",";
    double_t tm_start, tm_end, runTime=0;

    string script1;

    string dbName = "dfs://test_MultithreadedTableWriter";
    script1 += "dbName = \"dfs://test_MultithreadedTableWriter\"\n";
    script1 += "tableName=\"pt\"\n";
    script1 += "login(\"admin\",\"123456\")\n";
    script1 += "DATA_FIRE=\"";
    script1 += DATA_FIRE;
    script1 += "\"\n";
    script1 += "if(existsDatabase(dbName)){\n";
    script1 += " dropDatabase(dbName)\n";
    script1 += "}\n";
    script1 += "db=database(dbName,RANGE,month(1990.01.01) +((0..52)*6))\n";
    script1 += "schema=extractTextSchema(DATA_FIRE)\n";
    script1 += "table1= table(1:0,schema[`name],schema[`type])\n";
    script1 += "pt = db.createPartitionedTable(table1, `pt, `date)";
    //cout<<script1<<endl;
    conn.run(script1);
    SmartPointer<MultithreadedTableWriter> mulwrite;
    ErrorCodeInfo pErrorInfo;
    mulwrite =new MultithreadedTableWriter(hostName,port, "admin","123456",dbName,"pt",NULL,false,NULL,1000,1,32,"date",pcompress);
    TableSP bt = conn.run("t0 = loadText('"+DATA_FIRE+"');t0");
    MultithreadedTableWriter::Status status;
    vector<ConstantSP> datas;
    datas.reserve(bt->rows() * 21);
    for(int i = 0; i < bt->rows(); ++i){
        for(int j = 0; j < 21; ++j)
       datas.emplace_back(bt->getColumn(j)->get(i));
    }
    tm_start=getTimeStampMs();
    for(int i=0;i < bt->rows();i++){
        mulwrite->insert(pErrorInfo,datas[i * 21 + 0],datas[i * 21 + 1],datas[i * 21 + 2],datas[i * 21 + 3],
        datas[i * 21 + 4],datas[i * 21 + 5],datas[i * 21 + 6],datas[i * 21 + 7],datas[i * 21 + 8],
        datas[i * 21 + 9],datas[i * 21 + 10],datas[i * 21 + 11],datas[i * 21 + 12],datas[i * 21 + 13],datas[i * 21 + 14],
        datas[i * 21 + 15],datas[i * 21 + 16],datas[i * 21 + 17],datas[i * 21 + 18],datas[i * 21 + 19],datas[i * 21 + 20]);
        // (datas[i * 21 + 0],datas[i * 21 + 1],datas[i * 21 + 2],datas[i * 21 + 3],
        // datas[i * 21 + 4],datas[i * 21 + 5],datas[i * 21 + 6],datas[i * 21 + 7],datas[i * 21 + 8],
        // datas[i * 21 + 9],datas[i * 21 + 10],datas[i * 21 + 11],datas[i * 21 + 12],datas[i * 21 + 13],datas[i * 21 + 14],
        // datas[i * 21 + 15],datas[i * 21 + 16],datas[i * 21 + 17],datas[i * 21 + 18],datas[i * 21 + 19],datas[i * 21 + 20]);
       
    }
    // string script2="a= select * from pt;saveText(a,'D:/DATA/5m.csv')";
    // conn.run(script2);

    do{
        mulwrite->getStatus(status);
        // usleep(100000);
        if(status.sentRows ==  bt->rows()){
            tm_end=getTimeStampMs();
            VectorSP resVec = conn.run("pt0=exec * from pt;each(eqObj,pt0.values(),t0.values())");
            for(unsigned int i=0;i<resVec->size();i++)
                assert(resVec->get(i)->getBool());
            cout<<Util::createTimestamp(tm_start)->castTemporal(DT_DATE)->getString()<<",";
            runTime = (tm_end - tm_start)/(double)1000;
            // cout<<"test_MultithreadedTableWriter_insert_threadCount_1-> Time expended: " << runTime << "s" << endl;
            cout << runTime<<","+Util::VER<< endl;
            // double TTL = re_rows->getDouble()/runTime;
            // cout<<"TTL: "<< fixed << TTL << " rows/s"<<endl << endl;
            // cout<< fixed <<TTL<<endl;
            break;
        }
    }while (true);
    conn.run("undef all;dropDatabase(\""+dbName+"\")");
    return runTime;
}

static double test_MultithreadedTableWriter_insert_threadCount_64(DBConnection &conn, vector<COMPRESS_METHOD> *pcompress=nullptr)
{
    cout <<"MTW_insert_threadCount_64"<< ",";
    double_t tm_start, tm_end, runTime=0;

    string script1;

    string dbName = "dfs://test_MultithreadedTableWriter";
    script1 += "dbName = \"dfs://test_MultithreadedTableWriter\"\n";
    script1 += "tableName=\"pt\"\n";
    script1 += "login(\"admin\",\"123456\")\n";
    script1 += "DATA_FIRE=\"";
    script1 += DATA_FIRE;
    script1 += "\"\n";
    script1 += "if(existsDatabase(dbName)){\n";
    script1 += " dropDatabase(dbName)\n";
    script1 += "}\n";
    script1 += "db=database(dbName,RANGE,month(1990.01.01) +((0..52)*6))\n";
    script1 += "schema=extractTextSchema(DATA_FIRE)\n";
    script1 += "table1= table(1:0,schema[`name],schema[`type])\n";
    script1 += "pt = db.createPartitionedTable(table1, `pt, `date)";
    //cout<<script1<<endl;
    conn.run(script1);
    SmartPointer<MultithreadedTableWriter> mulwrite;
    ErrorCodeInfo pErrorInfo;
    mulwrite =new MultithreadedTableWriter(hostName,port, "admin","123456",dbName,"pt",NULL,false,NULL,1000,1,64,"date",pcompress);
    TableSP bt = conn.run("t0 = loadText('"+DATA_FIRE+"');t0");
    MultithreadedTableWriter::Status status;
    vector<ConstantSP> datas;
    datas.reserve(bt->rows() * 21);
    for(int i = 0; i < bt->rows(); ++i){
        for(int j = 0; j < 21; ++j)
       datas.emplace_back(bt->getColumn(j)->get(i));
    }
    tm_start=getTimeStampMs();
    for(int i=0;i < bt->rows();i++){
        mulwrite->insert(pErrorInfo,datas[i * 21 + 0],datas[i * 21 + 1],datas[i * 21 + 2],datas[i * 21 + 3],
        datas[i * 21 + 4],datas[i * 21 + 5],datas[i * 21 + 6],datas[i * 21 + 7],datas[i * 21 + 8],
        datas[i * 21 + 9],datas[i * 21 + 10],datas[i * 21 + 11],datas[i * 21 + 12],datas[i * 21 + 13],datas[i * 21 + 14],
        datas[i * 21 + 15],datas[i * 21 + 16],datas[i * 21 + 17],datas[i * 21 + 18],datas[i * 21 + 19],datas[i * 21 + 20]);
        // (datas[i * 21 + 0],datas[i * 21 + 1],datas[i * 21 + 2],datas[i * 21 + 3],
        // datas[i * 21 + 4],datas[i * 21 + 5],datas[i * 21 + 6],datas[i * 21 + 7],datas[i * 21 + 8],
        // datas[i * 21 + 9],datas[i * 21 + 10],datas[i * 21 + 11],datas[i * 21 + 12],datas[i * 21 + 13],datas[i * 21 + 14],
        // datas[i * 21 + 15],datas[i * 21 + 16],datas[i * 21 + 17],datas[i * 21 + 18],datas[i * 21 + 19],datas[i * 21 + 20]);
       
    }
    // string script2="a= select * from pt;saveText(a,'D:/DATA/5m.csv')";
    // conn.run(script2);

    do{
        mulwrite->getStatus(status);
        // usleep(100000);
        if(status.sentRows ==  bt->rows()){
            tm_end=getTimeStampMs();
            VectorSP resVec = conn.run("pt0=exec * from pt;each(eqObj,pt0.values(),t0.values())");
            for(unsigned int i=0;i<resVec->size();i++)
                assert(resVec->get(i)->getBool());
            cout<<Util::createTimestamp(tm_start)->castTemporal(DT_DATE)->getString()<<",";
            runTime = (tm_end - tm_start)/(double)1000;
            // cout<<"test_MultithreadedTableWriter_insert_threadCount_1-> Time expended: " << runTime << "s" << endl;
            cout << runTime<<","+Util::VER<< endl;
            // double TTL = re_rows->getDouble()/runTime;
            // cout<<"TTL: "<< fixed << TTL << " rows/s"<<endl << endl;
            // cout<< fixed <<TTL<<endl;
            break;
        }
    }while (true);
    conn.run("undef all;dropDatabase(\""+dbName+"\")");
    return runTime;
}


static double test_MultithreadedTableWriter_inertUnwriteData_threadCount_1(DBConnection &conn, vector<COMPRESS_METHOD> *pcompress=nullptr)
{
    cout <<"MTW_inertUnwriteData_threadCount_1"<< ",";
    double_t tm_start, tm_end, runTime=0;

    string script1;
    string dbName = "dfs://test_MultithreadedTableWriter";
    script1 += "dbName = \"dfs://test_MultithreadedTableWriter\"\n";
    script1 += "tableName=\"pt\"\n";
    script1 += "login(\"admin\",\"123456\")\n";
    script1 += "DATA_FIRE=\"";
    script1 += DATA_FIRE;
    script1 += "\"\n";
    script1 += "if(existsDatabase(dbName)){\n";
    script1 += " dropDatabase(dbName)\n";
    script1 += "}\n";
    script1 += "db=database(dbName,RANGE,month(1990.01.01) +((0..52)*6))\n";
    script1 += "schema=extractTextSchema(DATA_FIRE)\n";
    script1 += "table1= table(1:0,schema[`name],schema[`type])\n";
    script1 += "pt = db.createPartitionedTable(table1, `pt, `date)";
    //cout<<script1<<endl;
    conn.run(script1);
    SmartPointer<MultithreadedTableWriter> mulwrite;
    ErrorCodeInfo pErrorInfo;
    mulwrite =new MultithreadedTableWriter(hostName,port, "admin","123456",dbName,"pt",NULL,false,NULL,1000,1,1,"date",pcompress);
    TableSP bt = conn.run("t0 = loadText('"+DATA_FIRE+"');t0");
    MultithreadedTableWriter::Status status;
    vector<std::vector<dolphindb::ConstantSP> *> datas;
    for(int i = 0; i < bt->rows(); ++i){
        vector<ConstantSP> *prow = new vector<ConstantSP>;
        vector<ConstantSP> &row = *prow;
        for(int j = 0; j < 21; ++j){
            row.emplace_back(bt->getColumn(j)->getRow(i));
        }
        datas.push_back(prow);
    }
    
    tm_start=getTimeStampMs();
    mulwrite->insertUnwrittenData(datas,pErrorInfo);

    do{
        mulwrite->getStatus(status);
        // usleep(100000);
        if(status.sentRows ==  bt->rows()){
            tm_end=getTimeStampMs();
            VectorSP resVec = conn.run("pt0=exec * from pt;each(eqObj,pt0.values(),t0.values())");
            for(unsigned int i=0;i<resVec->size();i++)
                assert(resVec->get(i)->getBool());
            cout<<Util::createTimestamp(tm_start)->castTemporal(DT_DATE)->getString()<<",";
            runTime = (tm_end - tm_start)/(double)1000;
            // cout<<"test_MultithreadedTableWriter_insert_threadCount_1-> Time expended: " << runTime << "s" << endl;
            cout << runTime<<","+Util::VER<< endl;
            // double TTL = re_rows->getDouble()/runTime;
            // cout<<"TTL: "<< fixed << TTL << " rows/s"<<endl << endl;
            // cout<< fixed <<TTL<<endl;
            break;
        }
    }while (true);
    conn.run("undef all;dropDatabase(\""+dbName+"\")");
    return runTime;
}


static double test_MultithreadedTableWriter_inertUnwriteData_threadCount_2(DBConnection &conn, vector<COMPRESS_METHOD> *pcompress=nullptr)
{
    cout <<"MTW_inertUnwriteData_threadCount_2"<< ",";
    double_t tm_start, tm_end, runTime=0;

    string script1;
    string dbName = "dfs://test_MultithreadedTableWriter";
    script1 += "dbName = \"dfs://test_MultithreadedTableWriter\"\n";
    script1 += "tableName=\"pt\"\n";
    script1 += "login(\"admin\",\"123456\")\n";
    script1 += "DATA_FIRE=\"";
    script1 += DATA_FIRE;
    script1 += "\"\n";
    script1 += "if(existsDatabase(dbName)){\n";
    script1 += " dropDatabase(dbName)\n";
    script1 += "}\n";
    script1 += "db=database(dbName,RANGE,month(1990.01.01) +((0..52)*6))\n";
    script1 += "schema=extractTextSchema(DATA_FIRE)\n";
    script1 += "table1= table(1:0,schema[`name],schema[`type])\n";
    script1 += "pt = db.createPartitionedTable(table1, `pt, `date)";
    //cout<<script1<<endl;
    conn.run(script1);
    SmartPointer<MultithreadedTableWriter> mulwrite;
    ErrorCodeInfo pErrorInfo;
    mulwrite =new MultithreadedTableWriter(hostName,port, "admin","123456",dbName,"pt",NULL,false,NULL,1000,1,2,"date",pcompress);
    TableSP bt = conn.run("t0 = loadText('"+DATA_FIRE+"');t0");
    MultithreadedTableWriter::Status status;
    vector<std::vector<dolphindb::ConstantSP> *> datas;
    for(int i = 0; i < bt->rows(); ++i){
        vector<ConstantSP> *prow = new vector<ConstantSP>;
        vector<ConstantSP> &row = *prow;
        for(int j = 0; j < 21; ++j){
            row.emplace_back(bt->getColumn(j)->getRow(i));
        }
        datas.push_back(prow);
    }
    
    tm_start=getTimeStampMs();
    mulwrite->insertUnwrittenData(datas,pErrorInfo);

    do{
        mulwrite->getStatus(status);
        // usleep(100000);
        if(status.sentRows ==  bt->rows()){
            tm_end=getTimeStampMs();
            VectorSP resVec = conn.run("pt0=exec * from pt;each(eqObj,pt0.values(),t0.values())");
            for(unsigned int i=0;i<resVec->size();i++)
                assert(resVec->get(i)->getBool());
            cout<<Util::createTimestamp(tm_start)->castTemporal(DT_DATE)->getString()<<",";
            runTime = (tm_end - tm_start)/(double)1000;
            // cout<<"test_MultithreadedTableWriter_insert_threadCount_1-> Time expended: " << runTime << "s" << endl;
            cout << runTime<<","+Util::VER<< endl;
            // double TTL = re_rows->getDouble()/runTime;
            // cout<<"TTL: "<< fixed << TTL << " rows/s"<<endl << endl;
            // cout<< fixed <<TTL<<endl;
            break;
        }
    }while (true);
    conn.run("undef all;dropDatabase(\""+dbName+"\")");
    return runTime;
}


static double test_MultithreadedTableWriter_inertUnwriteData_threadCount_4(DBConnection &conn, vector<COMPRESS_METHOD> *pcompress=nullptr)
{
    cout <<"MTW_inertUnwriteData_threadCount_4"<< ",";
    double_t tm_start, tm_end, runTime=0;

    string script1;
    string dbName = "dfs://test_MultithreadedTableWriter";
    script1 += "dbName = \"dfs://test_MultithreadedTableWriter\"\n";
    script1 += "tableName=\"pt\"\n";
    script1 += "login(\"admin\",\"123456\")\n";
    script1 += "DATA_FIRE=\"";
    script1 += DATA_FIRE;
    script1 += "\"\n";
    script1 += "if(existsDatabase(dbName)){\n";
    script1 += " dropDatabase(dbName)\n";
    script1 += "}\n";
    script1 += "db=database(dbName,RANGE,month(1990.01.01) +((0..52)*6))\n";
    script1 += "schema=extractTextSchema(DATA_FIRE)\n";
    script1 += "table1= table(1:0,schema[`name],schema[`type])\n";
    script1 += "pt = db.createPartitionedTable(table1, `pt, `date)";
    //cout<<script1<<endl;
    conn.run(script1);
    SmartPointer<MultithreadedTableWriter> mulwrite;
    ErrorCodeInfo pErrorInfo;
    mulwrite =new MultithreadedTableWriter(hostName,port, "admin","123456",dbName,"pt",NULL,false,NULL,1000,1,4,"date",pcompress);
    TableSP bt = conn.run("t0 = loadText('"+DATA_FIRE+"');t0");
    MultithreadedTableWriter::Status status;
    vector<std::vector<dolphindb::ConstantSP> *> datas;
    for(int i = 0; i < bt->rows(); ++i){
        vector<ConstantSP> *prow = new vector<ConstantSP>;
        vector<ConstantSP> &row = *prow;
        for(int j = 0; j < 21; ++j){
            row.emplace_back(bt->getColumn(j)->getRow(i));
        }
        datas.push_back(prow);
    }
    
    tm_start=getTimeStampMs();
    mulwrite->insertUnwrittenData(datas,pErrorInfo);

    do{
        mulwrite->getStatus(status);
        // usleep(100000);
        if(status.sentRows ==  bt->rows()){
            tm_end=getTimeStampMs();
            VectorSP resVec = conn.run("pt0=exec * from pt;each(eqObj,pt0.values(),t0.values())");
            for(unsigned int i=0;i<resVec->size();i++)
                assert(resVec->get(i)->getBool());
            cout<<Util::createTimestamp(tm_start)->castTemporal(DT_DATE)->getString()<<",";
            runTime = (tm_end - tm_start)/(double)1000;
            // cout<<"test_MultithreadedTableWriter_insert_threadCount_1-> Time expended: " << runTime << "s" << endl;
            cout << runTime<<","+Util::VER<< endl;
            // double TTL = re_rows->getDouble()/runTime;
            // cout<<"TTL: "<< fixed << TTL << " rows/s"<<endl << endl;
            // cout<< fixed <<TTL<<endl;
            break;
        }
    }while (true);
    conn.run("undef all;dropDatabase(\""+dbName+"\")");
    return runTime;
}


static double test_MultithreadedTableWriter_inertUnwriteData_threadCount_8(DBConnection &conn, vector<COMPRESS_METHOD> *pcompress=nullptr)
{
    cout <<"MTW_inertUnwriteData_threadCount_8"<< ",";
    double_t tm_start, tm_end, runTime=0;

    string script1;
    string dbName = "dfs://test_MultithreadedTableWriter";
    script1 += "dbName = \"dfs://test_MultithreadedTableWriter\"\n";
    script1 += "tableName=\"pt\"\n";
    script1 += "login(\"admin\",\"123456\")\n";
    script1 += "DATA_FIRE=\"";
    script1 += DATA_FIRE;
    script1 += "\"\n";
    script1 += "if(existsDatabase(dbName)){\n";
    script1 += " dropDatabase(dbName)\n";
    script1 += "}\n";
    script1 += "db=database(dbName,RANGE,month(1990.01.01) +((0..52)*6))\n";
    script1 += "schema=extractTextSchema(DATA_FIRE)\n";
    script1 += "table1= table(1:0,schema[`name],schema[`type])\n";
    script1 += "pt = db.createPartitionedTable(table1, `pt, `date)";
    //cout<<script1<<endl;
    conn.run(script1);
    SmartPointer<MultithreadedTableWriter> mulwrite;
    ErrorCodeInfo pErrorInfo;
    mulwrite =new MultithreadedTableWriter(hostName,port, "admin","123456",dbName,"pt",NULL,false,NULL,1000,1,8,"date",pcompress);
    TableSP bt = conn.run("t0 = loadText('"+DATA_FIRE+"');t0");
    MultithreadedTableWriter::Status status;
    vector<std::vector<dolphindb::ConstantSP> *> datas;
    for(int i = 0; i < bt->rows(); ++i){
        vector<ConstantSP> *prow = new vector<ConstantSP>;
        vector<ConstantSP> &row = *prow;
        for(int j = 0; j < 21; ++j){
            row.emplace_back(bt->getColumn(j)->getRow(i));
        }
        datas.push_back(prow);
    }
    
    tm_start=getTimeStampMs();
    mulwrite->insertUnwrittenData(datas,pErrorInfo);

    do{
        mulwrite->getStatus(status);
        // usleep(100000);
        if(status.sentRows ==  bt->rows()){
            tm_end=getTimeStampMs();
            VectorSP resVec = conn.run("pt0=exec * from pt;each(eqObj,pt0.values(),t0.values())");
            for(unsigned int i=0;i<resVec->size();i++)
                assert(resVec->get(i)->getBool());
            cout<<Util::createTimestamp(tm_start)->castTemporal(DT_DATE)->getString()<<",";
            runTime = (tm_end - tm_start)/(double)1000;
            // cout<<"test_MultithreadedTableWriter_insert_threadCount_1-> Time expended: " << runTime << "s" << endl;
            cout << runTime<<","+Util::VER<< endl;
            // double TTL = re_rows->getDouble()/runTime;
            // cout<<"TTL: "<< fixed << TTL << " rows/s"<<endl << endl;
            // cout<< fixed <<TTL<<endl;
            break;
        }
    }while (true);
    conn.run("undef all;dropDatabase(\""+dbName+"\")");
    return runTime;
}



static double test_MultithreadedTableWriter_inertUnwriteData_threadCount_16(DBConnection &conn, vector<COMPRESS_METHOD> *pcompress=nullptr)
{
    cout <<"MTW_inertUnwriteData_threadCount_16"<< ",";
    double_t tm_start, tm_end, runTime=0;

    string script1;
    string dbName = "dfs://test_MultithreadedTableWriter";
    script1 += "dbName = \"dfs://test_MultithreadedTableWriter\"\n";
    script1 += "tableName=\"pt\"\n";
    script1 += "login(\"admin\",\"123456\")\n";
    script1 += "DATA_FIRE=\"";
    script1 += DATA_FIRE;
    script1 += "\"\n";
    script1 += "if(existsDatabase(dbName)){\n";
    script1 += " dropDatabase(dbName)\n";
    script1 += "}\n";
    script1 += "db=database(dbName,RANGE,month(1990.01.01) +((0..52)*6))\n";
    script1 += "schema=extractTextSchema(DATA_FIRE)\n";
    script1 += "table1= table(1:0,schema[`name],schema[`type])\n";
    script1 += "pt = db.createPartitionedTable(table1, `pt, `date)";
    //cout<<script1<<endl;
    conn.run(script1);
    SmartPointer<MultithreadedTableWriter> mulwrite;
    ErrorCodeInfo pErrorInfo;
    mulwrite =new MultithreadedTableWriter(hostName,port, "admin","123456",dbName,"pt",NULL,false,NULL,1000,1,16,"date",pcompress);
    TableSP bt = conn.run("t0 = loadText('"+DATA_FIRE+"');t0");
    MultithreadedTableWriter::Status status;
    vector<std::vector<dolphindb::ConstantSP> *> datas;
    for(int i = 0; i < bt->rows(); ++i){
        vector<ConstantSP> *prow = new vector<ConstantSP>;
        vector<ConstantSP> &row = *prow;
        for(int j = 0; j < 21; ++j){
            row.emplace_back(bt->getColumn(j)->getRow(i));
        }
        datas.push_back(prow);
    }
    
    tm_start=getTimeStampMs();
    mulwrite->insertUnwrittenData(datas,pErrorInfo);

    do{
        mulwrite->getStatus(status);
        // usleep(100000);
        if(status.sentRows ==  bt->rows()){
            tm_end=getTimeStampMs();
            VectorSP resVec = conn.run("pt0=exec * from pt;each(eqObj,pt0.values(),t0.values())");
            for(unsigned int i=0;i<resVec->size();i++)
                assert(resVec->get(i)->getBool());
            cout<<Util::createTimestamp(tm_start)->castTemporal(DT_DATE)->getString()<<",";
            runTime = (tm_end - tm_start)/(double)1000;
            // cout<<"test_MultithreadedTableWriter_insert_threadCount_1-> Time expended: " << runTime << "s" << endl;
            cout << runTime<<","+Util::VER<< endl;
            // double TTL = re_rows->getDouble()/runTime;
            // cout<<"TTL: "<< fixed << TTL << " rows/s"<<endl << endl;
            // cout<< fixed <<TTL<<endl;
            break;
        }
    }while (true);
    conn.run("undef all;dropDatabase(\""+dbName+"\")");
    return runTime;
}


static double test_MultithreadedTableWriter_inertUnwriteData_threadCount_32(DBConnection &conn, vector<COMPRESS_METHOD> *pcompress=nullptr)
{
    cout <<"MTW_inertUnwriteData_threadCount_32"<< ",";
    double_t tm_start, tm_end, runTime=0;

    string script1;
    string dbName = "dfs://test_MultithreadedTableWriter";
    script1 += "dbName = \"dfs://test_MultithreadedTableWriter\"\n";
    script1 += "tableName=\"pt\"\n";
    script1 += "login(\"admin\",\"123456\")\n";
    script1 += "DATA_FIRE=\"";
    script1 += DATA_FIRE;
    script1 += "\"\n";
    script1 += "if(existsDatabase(dbName)){\n";
    script1 += " dropDatabase(dbName)\n";
    script1 += "}\n";
    script1 += "db=database(dbName,RANGE,month(1990.01.01) +((0..52)*6))\n";
    script1 += "schema=extractTextSchema(DATA_FIRE)\n";
    script1 += "table1= table(1:0,schema[`name],schema[`type])\n";
    script1 += "pt = db.createPartitionedTable(table1, `pt, `date)";
    //cout<<script1<<endl;
    conn.run(script1);
    SmartPointer<MultithreadedTableWriter> mulwrite;
    ErrorCodeInfo pErrorInfo;
    mulwrite =new MultithreadedTableWriter(hostName,port, "admin","123456",dbName,"pt",NULL,false,NULL,1000,1,32,"date",pcompress);
    TableSP bt = conn.run("t0 = loadText('"+DATA_FIRE+"');t0");
    MultithreadedTableWriter::Status status;
    vector<std::vector<dolphindb::ConstantSP> *> datas;
    for(int i = 0; i < bt->rows(); ++i){
        vector<ConstantSP> *prow = new vector<ConstantSP>;
        vector<ConstantSP> &row = *prow;
        for(int j = 0; j < 21; ++j){
            row.emplace_back(bt->getColumn(j)->getRow(i));
        }
        datas.push_back(prow);
    }
    
    tm_start=getTimeStampMs();
    mulwrite->insertUnwrittenData(datas,pErrorInfo);

    do{
        mulwrite->getStatus(status);
        // usleep(100000);
        if(status.sentRows ==  bt->rows()){
            tm_end=getTimeStampMs();
            VectorSP resVec = conn.run("pt0=exec * from pt;each(eqObj,pt0.values(),t0.values())");
            for(unsigned int i=0;i<resVec->size();i++)
                assert(resVec->get(i)->getBool());
            cout<<Util::createTimestamp(tm_start)->castTemporal(DT_DATE)->getString()<<",";
            runTime = (tm_end - tm_start)/(double)1000;
            // cout<<"test_MultithreadedTableWriter_insert_threadCount_1-> Time expended: " << runTime << "s" << endl;
            cout << runTime<<","+Util::VER<< endl;
            // double TTL = re_rows->getDouble()/runTime;
            // cout<<"TTL: "<< fixed << TTL << " rows/s"<<endl << endl;
            // cout<< fixed <<TTL<<endl;
            break;
        }
    }while (true);
    conn.run("undef all;dropDatabase(\""+dbName+"\")");
    return runTime;
}


static double test_MultithreadedTableWriter_inertUnwriteData_threadCount_64(DBConnection &conn, vector<COMPRESS_METHOD> *pcompress=nullptr)
{
    cout <<"MTW_inertUnwriteData_threadCount_64"<< ",";
    double_t tm_start, tm_end, runTime=0;

    string script1;
    string dbName = "dfs://test_MultithreadedTableWriter";
    script1 += "dbName = \"dfs://test_MultithreadedTableWriter\"\n";
    script1 += "tableName=\"pt\"\n";
    script1 += "login(\"admin\",\"123456\")\n";
    script1 += "DATA_FIRE=\"";
    script1 += DATA_FIRE;
    script1 += "\"\n";
    script1 += "if(existsDatabase(dbName)){\n";
    script1 += " dropDatabase(dbName)\n";
    script1 += "}\n";
    script1 += "db=database(dbName,RANGE,month(1990.01.01) +((0..52)*6))\n";
    script1 += "schema=extractTextSchema(DATA_FIRE)\n";
    script1 += "table1= table(1:0,schema[`name],schema[`type])\n";
    script1 += "pt = db.createPartitionedTable(table1, `pt, `date)";
    //cout<<script1<<endl;
    conn.run(script1);
    SmartPointer<MultithreadedTableWriter> mulwrite;
    ErrorCodeInfo pErrorInfo;
    mulwrite =new MultithreadedTableWriter(hostName,port, "admin","123456",dbName,"pt",NULL,false,NULL,1000,1,64,"date",pcompress);
    TableSP bt = conn.run("t0 = loadText('"+DATA_FIRE+"');t0");
    MultithreadedTableWriter::Status status;
    vector<std::vector<dolphindb::ConstantSP> *> datas;
    for(int i = 0; i < bt->rows(); ++i){
        vector<ConstantSP> *prow = new vector<ConstantSP>;
        vector<ConstantSP> &row = *prow;
        for(int j = 0; j < 21; ++j){
            row.emplace_back(bt->getColumn(j)->getRow(i));
        }
        datas.push_back(prow);
    }
    
    tm_start=getTimeStampMs();
    mulwrite->insertUnwrittenData(datas,pErrorInfo);

    do{
        mulwrite->getStatus(status);
        // usleep(100000);
        if(status.sentRows ==  bt->rows()){
            tm_end=getTimeStampMs();
            VectorSP resVec = conn.run("pt0=exec * from pt;each(eqObj,pt0.values(),t0.values())");
            for(unsigned int i=0;i<resVec->size();i++)
                assert(resVec->get(i)->getBool());
            cout<<Util::createTimestamp(tm_start)->castTemporal(DT_DATE)->getString()<<",";
            runTime = (tm_end - tm_start)/(double)1000;
            // cout<<"test_MultithreadedTableWriter_insert_threadCount_1-> Time expended: " << runTime << "s" << endl;
            cout << runTime<<","+Util::VER<< endl;
            // double TTL = re_rows->getDouble()/runTime;
            // cout<<"TTL: "<< fixed << TTL << " rows/s"<<endl << endl;
            // cout<< fixed <<TTL<<endl;
            break;
        }
    }while (true);
    conn.run("undef all;dropDatabase(\""+dbName+"\")");
    return runTime;
}

static double test_batchTableWriter_performance(DBConnection &conn){
    cout <<"BatchTableWriter"<< ",";
    double_t tm_start, tm_end;
    double runTime=0;

    string script1;

    string dbName = "dfs://test_batchTableWriter";
    script1 += "dbName = \"dfs://test_batchTableWriter\"\n";
    script1 += "tableName=\"pt\"\n";
    script1 += "login(\"admin\",\"123456\")\n";
    script1 += "DATA_FIRE=\"";
    script1 += DATA_FIRE;
    script1 += "\"\n";
    script1 += "if(existsDatabase(dbName)){\n";
    script1 += " dropDatabase(dbName)\n";
    script1 += "}\n";
    script1 += "db=database(dbName,RANGE,month(1990.01.01) +((0..52)*6))\n";
    script1 += "schema=extractTextSchema(DATA_FIRE)\n";
    script1 += "table1= table(1:0,schema[`name],schema[`type])\n";
    script1 += "pt = db.createPartitionedTable(table1, `pt, `date)";
    //cout<<script1<<endl;
    conn.run(script1);
    SmartPointer<BatchTableWriter> batchwrite;
    batchwrite =new BatchTableWriter(hostName, port, "admin","123456",true);
    batchwrite->addTable(dbName,"pt");
    TableSP bt = conn.run("t0 = loadText('"+DATA_FIRE+"');t0");
    vector<ConstantSP> datas;
    datas.reserve(bt->rows() * 21);
    for(int i = 0; i < bt->rows(); ++i){
        for(int j = 0; j < 21; ++j)
       datas.emplace_back(bt->getColumn(j)->get(i));
    }
    tm_start=getTimeStampMs();
    for(int i=0;i < bt->rows();i++){
        batchwrite->insert(dbName,"pt",datas[i * 21 + 0],datas[i * 21 + 1],datas[i * 21 + 2],datas[i * 21 + 3],
        datas[i * 21 + 4],datas[i * 21 + 5],datas[i * 21 + 6],datas[i * 21 + 7],datas[i * 21 + 8],
        datas[i * 21 + 9],datas[i * 21 + 10],datas[i * 21 + 11],datas[i * 21 + 12],datas[i * 21 + 13],datas[i * 21 + 14],
        datas[i * 21 + 15],datas[i * 21 + 16],datas[i * 21 + 17],datas[i * 21 + 18],datas[i * 21 + 19],datas[i * 21 + 20]);
        // (datas[i * 21 + 0],datas[i * 21 + 1],datas[i * 21 + 2],datas[i * 21 + 3],
        // datas[i * 21 + 4],datas[i * 21 + 5],datas[i * 21 + 6],datas[i * 21 + 7],datas[i * 21 + 8],
        // datas[i * 21 + 9],datas[i * 21 + 10],datas[i * 21 + 11],datas[i * 21 + 12],datas[i * 21 + 13],datas[i * 21 + 14],
        // datas[i * 21 + 15],datas[i * 21 + 16],datas[i * 21 + 17],datas[i * 21 + 18],datas[i * 21 + 19],datas[i * 21 + 20]);
       
    }
    do{
        TableSP result = batchwrite->getAllStatus();
        //usleep(100000);
        if(result->getColumn(3)->getString(0).c_str() ==  to_string(bt->rows())){
            tm_end=getTimeStampMs();
            VectorSP resVec = conn.run("pt0=exec * from pt;each(eqObj,pt0.values(),t0.values())");
            for(unsigned int i=0;i<resVec->size();i++)
                assert(resVec->get(i)->getBool());
            cout<<Util::createTimestamp(tm_start)->castTemporal(DT_DATE)->getString()<<",";
            runTime = (tm_end - tm_start)/(double)1000;
            // cout<<"test_MultithreadedTableWriter_insert_threadCount_1-> Time expended: " << runTime << "s" << endl;
            cout << runTime<<","+Util::VER<< endl;
            // double TTL = re_rows->getDouble()/runTime;
            // cout<<"TTL: "<< fixed << TTL << " rows/s"<<endl << endl;
            // cout<< fixed <<TTL<<endl;
            break;
        }
    }while (true);
    conn.run("undef all;dropDatabase(\""+dbName+"\")");
    return runTime;
}


static double test_AutoFitTableAppender_performance(DBConnection &conn){
    cout <<"AutoFitTableAppender"<< ",";
    double_t tm_start, tm_end;
    double runTime=0;

    string script1;

    string dbName = "dfs://test_autoTableFitAppender";
    script1 += "dbName = \"dfs://test_autoTableFitAppender\"\n";
    script1 += "tableName=\"pt\"\n";
    script1 += "login(\"admin\",\"123456\")\n";
    script1 += "DATA_FIRE=\"";
    script1 += DATA_FIRE;
    script1 += "\"\n";
    script1 += "if(existsDatabase(dbName)){\n";
    script1 += " dropDatabase(dbName)\n";
    script1 += "}\n";
    script1 += "db=database(dbName,RANGE,month(1990.01.01) +((0..52)*6))\n";
    script1 += "schema=extractTextSchema(DATA_FIRE)\n";
    script1 += "table1= table(1:0,schema[`name],schema[`type])\n";
    script1 += "if(existsTable(dbName,`pt)){\n";
    script1 += " dropTable(db,`pt)\n";
    script1 += "}\n";
    script1 += "pt = db.createPartitionedTable(table1, `pt, `date)";
    //cout<<script1<<endl;
    conn.run(script1);
    AutoFitTableAppender appender("dfs://test_autoTableFitAppender", "pt", conn);
    TableSP bt = conn.run("t0 = loadText('"+DATA_FIRE+"');t0");

    tm_start=getTimeStampMs();
    int res = appender.append(bt);
    tm_end=getTimeStampMs();

    VectorSP resVec = conn.run("pt0=exec * from pt;each(eqObj,pt0.values(),t0.values())");
    for(unsigned int i=0;i<resVec->size();i++)
        assert(resVec->get(i)->getBool());

    cout<<Util::createTimestamp(tm_start)->castTemporal(DT_DATE)->getString()<<",";
    runTime = (tm_end-tm_start)/(double)1000;
    cout << runTime<<","+Util::VER<< endl;
    // double TTL = re_rows->getDouble()/runTime;
    // cout<< fixed <<TTL<<endl;
    conn.run("undef all;dropDatabase(\""+dbName+"\")");
    return runTime;
}

static double test_AutoFitTableUpsert_performance(DBConnection &conn){
    cout <<"AutoFitTableUpsert"<< ",";
    double_t tm_start, tm_end;
    double runTime=0;

    string script1;

    string dbName = "dfs://test_autoFitTableUpsert";
    script1 += "dbName = \"dfs://test_autoFitTableUpsert\"\n";
    script1 += "tableName=\"pt\"\n";
    script1 += "login(\"admin\",\"123456\")\n";
    script1 += "DATA_FIRE=\"";
    script1 += DATA_FIRE;
    script1 += "\"\n";
    script1 += "if(existsDatabase(dbName)){\n";
    script1 += " dropDatabase(dbName)\n";
    script1 += "}\n";
    script1 += "db=database(dbName,RANGE,month(1990.01.01) +((0..52)*6))\n";
    script1 += "schema=extractTextSchema(DATA_FIRE)\n";
    script1 += "table1= table(1:0,schema[`name],schema[`type])\n";
    script1 += "if(existsTable(dbName,`pt)){\n";
    script1 += " dropTable(db,`pt)\n";
    script1 += "}\n";
    script1 += "pt = db.createPartitionedTable(table1, `pt, `date)";
    // cout<<script1<<endl;
    conn.run(script1);

    vector<string> keycolName = {"date"};
    AutoFitTableUpsert aftu(dbName, "pt", conn, false, &keycolName);
    TableSP bt = conn.run("t0 = loadText('"+DATA_FIRE+"');t0");
    
    tm_start=getTimeStampMs();
    int res = aftu.upsert(bt);
    tm_end=getTimeStampMs();
    
    VectorSP resVec = conn.run("pt0=exec * from pt;each(eqObj,pt0.values(),t0.values())");
    for(unsigned int i=0;i<resVec->size();i++)
        assert(resVec->get(i)->getBool());
   
    cout<<Util::createTimestamp(tm_start)->castTemporal(DT_DATE)->getString()<<",";
    runTime = (tm_end-tm_start)/(double)1000;
    cout << runTime<<","+Util::VER<< endl;
    // double TTL = re_rows->getDouble()/runTime;
    // cout<< fixed <<TTL<<endl;
    conn.run("undef all;dropDatabase(\""+dbName+"\")");
    return runTime;
}

static double test_PartitionedTableAppender_performance(DBConnection &conn, DBConnectionPool &pool){
    cout <<"PartitionedTableAppender"<< ",";
    double_t tm_start, tm_end;
    double runTime=0;

    string script1;

    string dbName = "dfs://test_partitionedTableAppender";
    script1 += "dbName = \"dfs://test_partitionedTableAppender\"\n";
    script1 += "tableName=\"pt\"\n";
    script1 += "login(\"admin\",\"123456\")\n";
    script1 += "DATA_FIRE=\"";
    script1 += DATA_FIRE;
    script1 += "\"\n";
    script1 += "if(existsDatabase(dbName)){\n";
    script1 += " dropDatabase(dbName)\n";
    script1 += "}\n";
    script1 += "db=database(dbName,RANGE,month(1990.01.01) +((0..52)*6))\n";
    script1 += "schema=extractTextSchema(DATA_FIRE)\n";
    script1 += "table1= table(1:0,schema[`name],schema[`type])\n";
    script1 += "if(existsTable(dbName,`pt)){\n";
    script1 += " dropTable(db,`pt)\n";
    script1 += "}\n";
    script1 += "pt = db.createPartitionedTable(table1, `pt, `date)";
    //cout<<script1<<endl;
    conn.run(script1);
    PartitionedTableAppender appender("dfs://test_partitionedTableAppender", "pt", "date", pool);
    TableSP bt = conn.run("t0 = loadText('"+DATA_FIRE+"');t0");

    tm_start=getTimeStampMs();
    int res = appender.append(bt);
    tm_end=getTimeStampMs();

    VectorSP resVec = conn.run("pt0=exec * from pt;each(eqObj,pt0.values(),t0.values())");
    for(unsigned int i=0;i<resVec->size();i++)
        assert(resVec->get(i)->getBool());

    cout<<Util::createTimestamp(tm_start)->castTemporal(DT_DATE)->getString()<<",";
    runTime = (tm_end-tm_start)/(double)1000;
    cout << runTime<<","+Util::VER<< endl;
    // double TTL = re_rows->getInt()/runTime;
    // // cout<< fixed <<TTL<<endl;
    conn.run("undef all;dropDatabase(\""+dbName+"\")");
    return runTime;
}

static double test_DdbtableInsert_performance(DBConnection &conn){
    cout <<"runInsertToTable"<< ",";
    double_t tm_start, tm_end;
    double runTime=0;

    string script1;

    string dbName = "dfs://test_DdbtableInsert";
    script1 += "dbName = \"dfs://test_DdbtableInsert\"\n";
    script1 += "tableName=\"pt\"\n";
    script1 += "login(\"admin\",\"123456\")\n";
    script1 += "DATA_FIRE=\"";
    script1 += DATA_FIRE;
    script1 += "\"\n";
    script1 += "if(existsDatabase(dbName)){\n";
    script1 += " dropDatabase(dbName)\n";
    script1 += "}\n";
    script1 += "db=database(dbName,RANGE,month(1990.01.01) +((0..52)*6))\n";
    script1 += "schema=extractTextSchema(DATA_FIRE)\n";
    script1 += "table1= table(1:0,schema[`name],schema[`type])\n";
    script1 += "if(existsTable(dbName,`pt)){\n";
    script1 += " dropTable(db,`pt)\n";
    script1 += "}\n";
    script1 += "pt = db.createPartitionedTable(table1, `pt, `date)";
    //cout<<script1<<endl;
    conn.run(script1);
    TableSP bt = conn.run("t0 = loadText('"+DATA_FIRE+"');t0");
    vector<ConstantSP> args;
    args.push_back(bt);

    tm_start=getTimeStampMs();
    conn.run("tableInsert{loadTable('dfs://test_DdbtableInsert', `pt)}", args); 
    tm_end=getTimeStampMs();

    VectorSP resVec = conn.run("pt0=exec * from pt;each(eqObj,pt0.values(),t0.values())");
    for(unsigned int i=0;i<resVec->size();i++)
        assert(resVec->get(i)->getBool());

    cout<<Util::createTimestamp(tm_start)->castTemporal(DT_DATE)->getString()<<",";
    runTime = (tm_end-tm_start)/(double)1000;
    cout << runTime<<","+Util::VER<< endl;
    // double TTL = re_rows->getDouble()/runTime;
    // cout<< fixed <<TTL<<endl;
    conn.run("undef all;dropDatabase(\""+dbName+"\")");
    return runTime;
}


static double test_readTable_with_fetchSize_performance(DBConnection &conn){
    cout <<"readTable_with_fetchSize_uncompressed"<< ",";
    int64_t tm2, tm3=0;
    int total_size=0;
    string sb = "exec * from loadTable(\"dfs://test_readDfsTable\",`pt)";    
    int fetchSize = 100000;
    
    tm2 = Util::getEpochTime();
    BlockReaderSP reader = conn.run(sb,4,2,fetchSize);
    tm3 = Util::getEpochTime();

    while(reader->hasNext()){
        TableSP fetchTable=reader->read();
        total_size += fetchTable->rows();
    }

    cout<<"total get rows: "<<total_size<<endl;
    double runTime = (double)(tm3-tm2)/(double)1000;
    conn.run("undef all;");
    return runTime;
}

static double test_readTable_without_fetchSize_performance(DBConnection &conn){
    cout <<"readTable_without_fetchSize_uncompressed"<< ",";
    int64_t tm1, tm2=0;
    string sb = "exec * from loadTable(\"dfs://test_readDfsTable\",`pt)";
    
    tm1 = Util::getEpochTime();
    TableSP mytrades = conn.run(sb);
    tm2 = Util::getEpochTime();

    conn.upload("mytrades",mytrades);
    VectorSP res = conn.run("tab = exec * from loadTable(\"dfs://test_readDfsTable\",`pt);each(eqObj,mytrades.values(),tab.values())");
    for(auto i =0;i<res->size();i++)
        assert(res->get(i)->getBool());

    cout<<"total get rows: "<<mytrades->rows()<<endl;
    double runTime = (double)(tm2-tm1)/(double)1000;
    conn.run("undef all;");
    return runTime;
}


static double test_readTable_with_fetchSize_compressed_performance(DBConnection &conn){
    cout <<"readTable_with_fetchSize_compressed"<< ",";
    int64_t tm2, tm3=0;
    int total_size=0;
    string sb = "exec * from loadTable(\"dfs://test_readDfsTable\",`pt)";

    int fetchSize = 100000;
    
    tm2 = Util::getEpochTime();
    BlockReaderSP reader = conn.run(sb,4,2,fetchSize);
    tm3 = Util::getEpochTime();

    while(reader->hasNext()){
        TableSP fetchTable=reader->read();
        total_size += fetchTable->rows();
    }

    cout<<"total get rows: "<<total_size<<endl;
    double runTime = (double)(tm3-tm2)/(double)1000;
    conn.run("undef all;");
    return runTime;
}

static double test_readTable_without_fetchSize_compressed_performance(DBConnection &conn){
    cout <<"readTable_without_fetchSize_compressed"<< ",";
    int64_t tm1, tm2=0;
    string sb = "exec * from loadTable(\"dfs://test_readDfsTable\",`pt)";
    
    tm1 = Util::getEpochTime();
    TableSP mytrades = conn.run(sb);
    tm2 = Util::getEpochTime();

    conn.upload("mytrades",mytrades);
    VectorSP res = conn.run("tab = exec * from loadTable(\"dfs://test_readDfsTable\",`pt);each(eqObj,mytrades.values(),tab.values())");
    // for(auto i=0;i<mytrades->rows();i++){
    //     cout<<"i: "<<i<<endl;
    //     // if(i==8330){
    //     //     cout<<mytrades->getColumn(17)->get(i)->getString()<<endl;
    //     //     cout<<conn.run("test_run_tab.column(17)["+to_string(i)+"]")->getString()<<endl;
    //     // }
    //     assert(mytrades->getColumn(18)->get(i)->getString() == conn.run("test_run_tab.column(18)["+to_string(i)+"]")->getString());
    // }
        
    for(auto i =0;i<res->size();i++){
        assert(res->get(i)->getBool());
    }

    cout<<"total get rows: "<<mytrades->rows()<<endl;
    double runTime = (double)(tm2-tm1)/(double)1000;
    conn.run("undef all;");
    return runTime;
}

static void createinMemoryTable(int rows){
    DBConnection conn(false, false);
    conn.connect(hostName,port,"admin","123456");
    string str = "try{undef(`test_run_tab,SHARED);}catch(ex){};go;"
    "row_num="+to_string(rows)+";"
    "colName =  `cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring`cipaddr`cuuid`cint128;"
    "colType = [BOOL, CHAR, SHORT, INT,LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING, IPADDR, UUID, INT128];"
    "table1=table(row_num:0,colName,colType);"
    "col1 = rand(2 ,row_num);col2 = rand(256 ,row_num);col3 = rand(-row_num..row_num ,row_num);col4 = rand(-row_num..row_num ,row_num);col5 = rand(-row_num..row_num ,row_num);"
    "col6 = rand(-row_num..row_num ,row_num);col7 = rand(-row_num..row_num ,row_num);col8 = rand(-row_num..row_num ,row_num);col9 = rand(-row_num..row_num ,row_num);"
    "col10 = rand(-row_num..row_num ,row_num);col11 = rand(-row_num..row_num ,row_num);col12 = rand(-row_num..row_num ,row_num);col13 = rand(-row_num..row_num ,row_num);"
    "col14= rand(-row_num..row_num ,row_num);col15 = rand(round(row_num,2) ,row_num);col16 = rand(round(row_num,2) ,row_num);col17 = rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);"
    "col18 =rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);col19 =  take(ipaddr(\"192.168.1.13\"),row_num);col20 = take(uuid(\"5d212a78-cc48-e3b1-4235-b4d91473ee87\"),row_num);col21 = take(int128(\"e1671797c52e15f763380b45e841ec32\"),row_num);"
    "go;"
    "for (i in 0..(row_num-1)){tableInsert(table1,col1[i],col2[i],col3[i],col4[i],col5[i],col6[i],col7[i],col8[i],col9[i],col10[i],col11[i],col12[i],col13[i],col14[i],col15[i],col16[i],col17[i],col18[i],col19[i],col20[i],col21[i])}"
    "go;"
    "dbName = \"dfs://test_readDfsTable\";"
    "tableName=`pt;"
    "if(existsDatabase(dbName)){dropDatabase(dbName)};"
    "db=database(dbName,VALUE,`a`s`sym`d`zdg`f`y`ddvb);"
    "pt = db.createPartitionedTable(table1, `pt, `cstring).append!(table1)";    
    conn.run(str);
    conn.close();
}

static void testAndWriteTxt(DBConnection &conn, DBConnection &conn_compress, DBConnectionPool pool, string api_version, int read_rows, vector<COMPRESS_METHOD> *compressVec=nullptr){
    ofstream ofs;
    ofs.open("/home/codes/DolphinDBAPI/build/perf_results.txt", ios::out);
    ofs <<"method,date,time expended,api version"<<endl;
    if (api_version.find("1.30")==0){
        int subversion = atoi(api_version.substr(5,2).c_str());
        if(subversion>=19){
            ofs << "AutoFitTableAppender"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_AutoFitTableAppender_performance(conn)<<","<<Util::VER<<endl;
            ofs << "PartitionedTableAppender"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_PartitionedTableAppender_performance(conn, pool)<<","<<Util::VER<<endl;
            ofs << "AutoFitTableUpsert"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_AutoFitTableUpsert_performance(conn)<<","<<Util::VER<<endl;
            ofs << "BatchTableWriter"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_batchTableWriter_performance(conn)<<","<<Util::VER<<endl;
            ofs << "runInsertToTable"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_DdbtableInsert_performance(conn)<<","<<Util::VER<<endl;
            ofs << "MTW_insert_threadCount_1"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_MultithreadedTableWriter_insert_threadCount_1(conn)<<","<<Util::VER<<endl;
            ofs << "MTW_insert_threadCount_2"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_MultithreadedTableWriter_insert_threadCount_2(conn)<<","<<Util::VER<<endl;
            ofs << "MTW_insert_threadCount_4"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_MultithreadedTableWriter_insert_threadCount_4(conn)<<","<<Util::VER<<endl;
            ofs << "MTW_insert_threadCount_8"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_MultithreadedTableWriter_insert_threadCount_8(conn)<<","<<Util::VER<<endl;
            ofs << "MTW_insert_threadCount_16"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_MultithreadedTableWriter_insert_threadCount_16(conn)<<","<<Util::VER<<endl;
            ofs << "MTW_insert_threadCount_32"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_MultithreadedTableWriter_insert_threadCount_32(conn)<<","<<Util::VER<<endl;
            ofs << "MTW_insert_threadCount_64"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_MultithreadedTableWriter_insert_threadCount_64(conn)<<","<<Util::VER<<endl;
            ofs << "MTW_inertUnwriteData_threadCount_1"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_MultithreadedTableWriter_inertUnwriteData_threadCount_1(conn)<<","<<Util::VER<<endl;
            ofs << "MTW_inertUnwriteData_threadCount_2"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_MultithreadedTableWriter_inertUnwriteData_threadCount_2(conn)<<","<<Util::VER<<endl;
            ofs << "MTW_inertUnwriteData_threadCount_4"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_MultithreadedTableWriter_inertUnwriteData_threadCount_4(conn)<<","<<Util::VER<<endl;
            ofs << "MTW_inertUnwriteData_threadCount_8"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_MultithreadedTableWriter_inertUnwriteData_threadCount_8(conn)<<","<<Util::VER<<endl;
            ofs << "MTW_inertUnwriteData_threadCount_16"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_MultithreadedTableWriter_inertUnwriteData_threadCount_16(conn)<<","<<Util::VER<<endl;
            ofs << "MTW_inertUnwriteData_threadCount_32"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_MultithreadedTableWriter_inertUnwriteData_threadCount_32(conn)<<","<<Util::VER<<endl;
            ofs << "MTW_inertUnwriteData_threadCount_64"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_MultithreadedTableWriter_inertUnwriteData_threadCount_64(conn)<<","<<Util::VER<<endl;
            cout <<endl<<"Test MTW with compress method begins."<<endl;
            ofs << "MTW_insert_threadCount_1_compress"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_MultithreadedTableWriter_insert_threadCount_1(conn, compressVec)<<","<<Util::VER<<endl;
            ofs << "MTW_insert_threadCount_2_compress"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_MultithreadedTableWriter_insert_threadCount_2(conn, compressVec)<<","<<Util::VER<<endl;
            ofs << "MTW_insert_threadCount_4_compress"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_MultithreadedTableWriter_insert_threadCount_4(conn, compressVec)<<","<<Util::VER<<endl;
            ofs << "MTW_insert_threadCount_8_compress"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_MultithreadedTableWriter_insert_threadCount_8(conn, compressVec)<<","<<Util::VER<<endl;
            ofs << "MTW_insert_threadCount_16_compress"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_MultithreadedTableWriter_insert_threadCount_16(conn, compressVec)<<","<<Util::VER<<endl;
            ofs << "MTW_insert_threadCount_32_compress"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_MultithreadedTableWriter_insert_threadCount_32(conn, compressVec)<<","<<Util::VER<<endl;
            ofs << "MTW_insert_threadCount_64_compress"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_MultithreadedTableWriter_insert_threadCount_64(conn, compressVec)<<","<<Util::VER<<endl;
            ofs << "MTW_inertUnwriteData_threadCount_1_compress"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_MultithreadedTableWriter_inertUnwriteData_threadCount_1(conn, compressVec)<<","<<Util::VER<<endl;
            ofs << "MTW_inertUnwriteData_threadCount_2_compress"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_MultithreadedTableWriter_inertUnwriteData_threadCount_2(conn, compressVec)<<","<<Util::VER<<endl;
            ofs << "MTW_inertUnwriteData_threadCount_4_compress"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_MultithreadedTableWriter_inertUnwriteData_threadCount_4(conn, compressVec)<<","<<Util::VER<<endl;
            ofs << "MTW_inertUnwriteData_threadCount_8_compress"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_MultithreadedTableWriter_inertUnwriteData_threadCount_8(conn, compressVec)<<","<<Util::VER<<endl;
            ofs << "MTW_inertUnwriteData_threadCount_16_compress"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_MultithreadedTableWriter_inertUnwriteData_threadCount_16(conn, compressVec)<<","<<Util::VER<<endl;
            ofs << "MTW_inertUnwriteData_threadCount_32_compress"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_MultithreadedTableWriter_inertUnwriteData_threadCount_32(conn, compressVec)<<","<<Util::VER<<endl;
            ofs << "MTW_inertUnwriteData_threadCount_64_compress"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_MultithreadedTableWriter_inertUnwriteData_threadCount_64(conn, compressVec)<<","<<Util::VER<<endl;

            cout <<"Preparing table datas for read test......";
            createinMemoryTable(read_rows);
            cout <<"OK!"<<endl;
            ofs << "readTable_with_fetchSize_uncompressed"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_readTable_with_fetchSize_performance(conn)<<","<<Util::VER<<endl;
            ofs << "readTable_without_fetchSize_uncompressed"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_readTable_without_fetchSize_performance(conn)<<","<<Util::VER<<endl;
            ofs << "readTable_with_fetchSize_compressed"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_readTable_with_fetchSize_compressed_performance(conn_compress)<<","<<Util::VER<<endl;
            ofs << "readTable_without_fetchSize_compressed"<<","<< Util::createTimestamp(getTimeStampMs())->castTemporal(DT_DATE)->getString()+","<<test_readTable_without_fetchSize_compressed_performance(conn_compress)<<","<<Util::VER<<endl;
        }
        else{
            ofs<<",,,"<<endl;
        }
    }
    else{
        ofs<<",,,"<<endl;
    }
    ofs.close();
}

vector<COMPRESS_METHOD> getCompressVec(){
    DBConnection conn(false, false);
    conn.connect(hostName,port,"admin","123456");
    vector<COMPRESS_METHOD> compressVec;
    VectorSP compressV = conn.run("a = schema(loadText('"+DATA_FIRE+"'));"
                                    "colTypes=a[`colDefs][`typeString];"
                                    "colMethods = [];"
                                    "for(i in 0..(size(colTypes)-1)){"
                                    "    if(colTypes[i] in `INT`DATE`MONTH`TIME`MINUTE`SECOND`DATETIME`TIMESTAMP`NANOTIME`NANOTIMESTAMP){"
                                    "        colMethods.append!(`DELTA)"
                                    "    }"
                                    "    else{"
                                    "        colMethods.append!(`LZ4)"
                                    "    }"
                                    "};colMethods");
    for(auto i=0;i<compressV->size();i++){
        if(compressV->get(i)->getString() == "DELTA")
            compressVec.emplace_back(COMPRESS_DELTA);
        else{
            compressVec.emplace_back(COMPRESS_LZ4);
        }
    }
    return compressVec;
}

int main(int argc, char **argv){
    string api_version = Util::VER;
    hostName = argv[1];
    string temp = argv[2];
    port = atoi(temp.c_str());
    temp = argv[3];
    int rows = atoi(temp.c_str());

    static DBConnection conn(false, false);
    static DBConnection conn_compress(false, false, 7200, true);
    static DBConnectionPool pool(hostName, port, 10, "admin", "123456");
    vector<COMPRESS_METHOD> compressVec = getCompressVec();
    conn.connect(hostName,port,"admin","123456");
    conn_compress.connect(hostName,port,"admin","123456");

    testAndWriteTxt(conn, conn_compress, pool, api_version, rows, &compressVec);
    pool.shutDown();
    conn.close();
    conn_compress.close();
    
    return EXIT_SUCCESS;
    
}