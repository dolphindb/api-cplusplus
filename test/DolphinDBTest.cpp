#include "config.h"

string genRandString(int maxSize){
    string result;
    int size = rand()%maxSize;
    for(int i = 0 ;i < size ;i ++){
        int r = rand() % alphas.size(); 
        result += alphas[r];
    }
    return result;
}

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
    FILE * fp = fopen(fileName.c_str(),"wb+");
    if(fp == NULL)
    	throw RuntimeException("Failed to open file [" + fileName + "].");
    vector<string> result;
    result.push_back("total:" + std::to_string(pass + fail) + "\n") ;
    result.push_back("pass:" + std::to_string(pass) + "\n");
    result.push_back("fail:" + std::to_string(fail) + "\n");
    for(unsigned int i = 0 ;i < result.size(); i++)
    	fwrite(result[i].c_str(), 1, result[i].size(),fp);
}


void testStringVector(int vecSize){
    vector<string> values;
    for(int i = 0 ;i < vecSize; i++)
        values.push_back(genRandString(30));
    string script; 
    for(int i = 0 ;i < vecSize; i++)
        script += "`" + values[i];
    ConstantSP result = conn.run(script);
    for(int i = 0; i < vecSize; i++){
        ASSERTION("testStringVector",result->getString(i),values[i]);
    }
}

void testIntVector(int vecSize){
    vector<int> values;
    for(int i = 0 ;i < vecSize ; i++)
        values.push_back(rand()%INT_MAX);
    string script;
    for(int i = 0 ;i < vecSize; i++)
        script += " " + std::to_string(values[i]);
    ConstantSP result = conn.run(script);
    for(int i = 0 ;i < vecSize; i++)
        ASSERTION("testIntVector",result->getInt(i),values[i]);
}

void testDoubleVector(int vecSize){
    vector<double> values;
    for(int i = 0 ;i < vecSize; i++)
        values.push_back((double)(rand()));
    string script;
    for(int i = 0 ;i < vecSize; i++)
        script += " " + std::to_string(values[i]);
    ConstantSP result = conn.run(script);
    for(int i = 0 ;i < vecSize; i++)
        ASSERTION("testDoubleVector",result->getDouble(i),values[i]);
}


void testDateVector(){
    string beginDate = "2010.08.20";
    vector<int> testValues = {1,10,100,1000,10000,100000};
    vector<string> expectResults = {"2010.08.21","2010.08.30","2010.11.28","2013.05.16","2038.01.05","2284.06.04"};
    string script;
    for(unsigned int i = 0 ;i < testValues.size(); i++){
        script += " "+ std::to_string(testValues[i]);
    }
    script = beginDate + " + " + script;
    ConstantSP result = conn.run(script);
    for(unsigned int i = 0 ;i < testValues.size(); i++){
        ASSERTION("testDateVector",result->getString(i),expectResults[i]);
    }
}



void testDatetimeVector(){
    string beginDateTime = "2012.10.01 15:00:04";
    vector<int> testValues = {1,100,1000,10000,100000,1000000,10000000};
    vector<string> expectResults = {"2012.10.01T15:00:05","2012.10.01T15:01:44","2012.10.01T15:16:44","2012.10.01T17:46:44","2012.10.02T18:46:44","2012.10.13T04:46:44","2013.01.25T08:46:44"};
    string script;
    for(unsigned int i = 0 ;i < testValues.size(); i++){
        script += " "+ std::to_string(testValues[i]);
    }
    script = beginDateTime + " + " + script;
    ConstantSP result = conn.run(script);
    for(unsigned int i = 0 ;i < testValues.size(); i++){
        ASSERTION("testDatetimeVector",result->getString(i),expectResults[i]);
    }
}


void testTimeStampVector(){
    string beginTimeStamp = "2009.10.12T00:00:00.000";
    vector<long long> testValues = {1,10,100,1000,10000,100000,1000000,10000000,100000000,1000000000,10000000000,100000000000,1000000000000};
    vector<string> expectResults = {"2009.10.12T00:00:00.001","2009.10.12T00:00:00.010","2009.10.12T00:00:00.100",
            "2009.10.12T00:00:01.000","2009.10.12T00:00:10.000","2009.10.12T00:01:40.000","2009.10.12T00:16:40.000",
            "2009.10.12T02:46:40.000","2009.10.13T03:46:40.000","2009.10.23T13:46:40.000","2010.02.04T17:46:40.000",
            "2012.12.12T09:46:40.000","2041.06.20T01:46:40.000"};
    string script;
    for(unsigned int i = 0; i < testValues.size(); i++){
       script += " " + std::to_string(testValues[i]);
    }
    script = beginTimeStamp + " + " + script;
    ConstantSP result = conn.run(script);
    for(unsigned int i = 0; i < testValues.size(); i++){
        ASSERTION("testTimeStampVector",result->getString(i),expectResults[i]);
    }
}

void testFunctionDef(){
    string script = "def funcAdd(a,b){return a + b};funcAdd(100,200);";
    ConstantSP result = conn.run(script);
    ASSERTION("testFunctionDef",result->getString(),string("300"));
}

void testMarics(){
    vector<string> expectResults = {"{1,2}","{3,4}","{5,6}"};
    string script = "1..6$2:3";
    ConstantSP result = conn.run(script);
    for(unsigned int i = 0 ;i < expectResults.size(); i++){
        ASSERTION("testMarics",result->getString(i),expectResults[i]);
    }
}



void testTable(){
    string script;
    script += "n=20000\n";
    script += "syms=`IBM`C`MS`MSFT`JPM`ORCL`BIDU`SOHU`GE`EBAY`GOOG`FORD`GS`PEP`USO`GLD`GDX`EEM`FXI`SLV`SINA`BAC`AAPL`PALL`YHOO`KOH`TSLA`CS`CISO`SUN\n";
    script += "mytrades=table(09:30:00+rand(18000,n) as timestamp,rand(syms,n) as sym, 10*(1+rand(100,n)) as qty,5.0+rand(100.0,n) as price, 1..n as number);\n";
    script += "select min(number) as minNum, max(number) as maxNum from mytrades";

    ConstantSP table = conn.run(script);
    ASSERTION("testTable",table->getColumn(0)->getString(0),string("1"));
    ASSERTION("testTable",table->getColumn(1)->getString(0),string("20000"));
}


void testDictionary(){
    string script;
    script += "dict(1 2 3,`IBM`MSFT`GOOG)";
    DictionarySP dict = conn.run(script);

    ASSERTION("testDictionary",dict->get(Util::createInt(1))->getString(),string("IBM"));
    ASSERTION("testDictionary",dict->get(Util::createInt(2))->getString(),string("MSFT"));
    ASSERTION("testDictionary",dict->get(Util::createInt(3))->getString(),string("GOOG"));

    script = "x=[int128('1234567890abcdef1234567890abcdef'),int128('1234567890abcdef1234567890abcde0')];y=1..2;d=dict(x,y);d";
    DictionarySP dict1 = conn.run(script);
    ASSERTION("testDictionary",dict1->get(Util::parseConstant(DT_INT128,"1234567890abcdef1234567890abcdef"))->getInt(),1);
    ASSERTION("testDictionary",dict1->get(Util::parseConstant(DT_INT128,"1234567890abcdef1234567890abcde0"))->getInt(),2);

    script = "def addDict(mutable d,key,a){d[key]=a[key]};";
    conn.run(script);
    DictionarySP d1=Util::createDictionary(DT_IP,DT_INT128);
    d1->set(Util::parseConstant(DT_INT128,"c93b83f13b579d8eea21f23a4ded5050"), Util::createInt(3));
    vector<ConstantSP> args0;
    args0.push_back(Util::parseConstant(DT_INT128,"c93b83f13b579d8eea21f23a4ded5050"));
    args0.push_back(d1);
    conn.run("addDict{d}",args0);
    dict1=conn.run("d");
    ASSERTION("testDictionary3",dict1->get(Util::parseConstant(DT_INT128,"1234567890abcdef1234567890abcdef"))->getInt(),1);
    ASSERTION("testDictionary3",dict1->get(Util::parseConstant(DT_INT128,"c93b83f13b579d8eea21f23a4ded5050"))->getInt(),3);



    script = "x=[uuid('12345678-90ab-cdef-1234-567890abcdef'),uuid('12345678-90ab-cdef-1234-567890abcde0')];y=1..2;dict(x,y)";
    DictionarySP dict2 = conn.run(script);
    ASSERTION("testDictionary",dict2->get(Util::parseConstant(DT_UUID,"12345678-90ab-cdef-1234-567890abcdef"))->getInt(),1);
    ASSERTION("testDictionary",dict2->get(Util::parseConstant(DT_UUID,"12345678-90ab-cdef-1234-567890abcde0"))->getInt(),2);
    DictionarySP d2=Util::createDictionary(DT_IP,DT_INT);
    d2->set(Util::parseConstant(DT_UUID,"12345678-90ab-cdef-1234-567890ab5050"), Util::createInt(3));
    vector<ConstantSP> args1;
    args1.push_back(Util::parseConstant(DT_UUID,"12345678-90ab-cdef-1234-567890ab5050"));
    args1.push_back(d2);
    conn.run("addDict{d}",args1);
    dict2=conn.run("d");
    ASSERTION("testDictionary4",dict2->get(Util::parseConstant(DT_UUID,"12345678-90ab-cdef-1234-567890abcdef"))->getInt(),1);
    ASSERTION("testDictionary4",dict2->get(Util::parseConstant(DT_UUID,"12345678-90ab-cdef-1234-567890ab5050"))->getInt(),3);



    script = "d=dict([ipaddr('4a80:5098:a255:cf1e:d4b8:3f69:d1fb:a6fd'),ipaddr('c93b:83f1:3b57:9d8e:ea21:f23a:4ded:4949'),ipaddr('bb94:24d5:33f9:9363:f15c:f929:abde:9d19'),ipaddr('4782:29a8:6f0f:aa7b:73b9:759c:d81e:e42e'),ipaddr('4aac:44d8:18fd:40fe:60c5:5b43:6449:f0e1'),ipaddr('7069:8a02:3385:a5a4:60f3:5206:d69:bcde')], 1..6);d";
    DictionarySP dict3= conn.run(script);
    ASSERTION("testDictionary5",dict3->get(Util::parseConstant(DT_IP,"4a80:5098:a255:cf1e:d4b8:3f69:d1fb:a6fd"))->getInt(),1);
    ASSERTION("testDictionary5",dict3->get(Util::parseConstant(DT_IP,"c93b:83f1:3b57:9d8e:ea21:f23a:4ded:4949"))->getInt(),2);
    DictionarySP d=Util::createDictionary(DT_IP,DT_INT);
    d->set(Util::parseConstant(DT_IP,"c93b:83f1:3b57:9d8e:ea21:f23a:4ded:5050"), Util::createInt(3));
    vector<ConstantSP> args;
    args.push_back(Util::parseConstant(DT_IP,"c93b:83f1:3b57:9d8e:ea21:f23a:4ded:5050"));
    args.push_back(d);
    conn.run("addDict{d}",args);
    dict3=conn.run("d");
    ASSERTION("testDictionary6",dict3->get(Util::parseConstant(DT_IP,"4a80:5098:a255:cf1e:d4b8:3f69:d1fb:a6fd"))->getInt(),1);
    ASSERTION("testDictionary6",dict3->get(Util::parseConstant(DT_IP,"c93b:83f1:3b57:9d8e:ea21:f23a:4ded:5050"))->getInt(),3);

    vector<string> codes = {"000001.SZ", "000002.SZ"};
    VectorSP code_vec = Util::createVector(DT_SYMBOL, codes.size(), codes.size());
    code_vec->setString(0, codes.size(), codes.data());

    DictionarySP adict=Util::createDictionary(DT_INT,DT_ANY);
    adict->set(Util::createInt(0), code_vec);
    adict->set(Util::createInt(1), code_vec);
    script="z=dict(INT,any); def add2dict(mutable z,d){ for(i in 0:d.size()) z[i]=d[i];}";
    conn.run(script);
    vector<ConstantSP> args10;
    args10.push_back(adict);
    conn.run("add2dict{z}",args10);
    dict=conn.run("z");
    ASSERTION("testDictionary7",dict->size(),2);
    ASSERTION("testDictionary7",dict->get(Util::createInt(1))->get(0)->getString(),string("000001.SZ"));
    ASSERTION("testDictionary7",dict->get(Util::createInt(0))->get(1)->getString(),string("000002.SZ"));

    DictionarySP acdict=Util::createDictionary(DT_CHAR,DT_ANY);
    acdict->set(Util::createChar(0), code_vec);
    acdict->set(Util::createChar(1), code_vec);
    script="z=dict(char,any); def add2dict(mutable z,d){ for(i in 0:d.size()) z[i]=d[i];}";
    conn.run(script);
    vector<ConstantSP> args11;
    args11.push_back(acdict);
    conn.run("add2dict{z}",args11);
    dict=conn.run("z");
    ASSERTION("testDictionary8",dict->size(),2);
    ASSERTION("testDictionary8",dict->get(Util::createChar(1))->get(0)->getString(),string("000001.SZ"));
    ASSERTION("testDictionary8",dict->get(Util::createChar(0))->get(1)->getString(),string("000002.SZ"));

    DictionarySP asdict=Util::createDictionary(DT_SHORT,DT_ANY);
    asdict->set(Util::createShort(0), code_vec);
    asdict->set(Util::createShort(1), code_vec);
    script="z=dict(SHORT,any); def add2dict(mutable z,d){ for(i in 0:d.size()) z[i]=d[i];}";
    conn.run(script);
    vector<ConstantSP> args12;
    args12.push_back(asdict);
    conn.run("add2dict{z}",args12);
    dict=conn.run("z");
    ASSERTION("testDictionary9",dict->size(),2);
    ASSERTION("testDictionary9",dict->get(Util::createShort(1))->get(0)->getString(),string("000001.SZ"));
    ASSERTION("testDictionary9",dict->get(Util::createShort(0))->get(1)->getString(),string("000002.SZ"));

    DictionarySP aldict=Util::createDictionary(DT_LONG,DT_ANY);
    aldict->set(Util::createLong(0), code_vec);
    aldict->set(Util::createLong(1), code_vec);
    script="z=dict(LONG,any); def add2dict(mutable z,d){ for(i in 0:d.size()) z[long(i)]=d[long(i)];}";
    conn.run(script);
    vector<ConstantSP> args13;
    args13.push_back(aldict);
    conn.run("add2dict{z}",args13);
    dict=conn.run("z");
    ASSERTION("testDictionary10",dict->size(),2);
    ASSERTION("testDictionary10",dict->get(Util::createLong(1))->get(0)->getString(),string("000001.SZ"));
    ASSERTION("testDictionary",dict->get(Util::createLong(0))->get(1)->getString(),string("000002.SZ"));

    DictionarySP adtdict=Util::createDictionary(DT_DATE,DT_ANY);
    adtdict->set(Util::createDate(2020,4,26), code_vec);
    adtdict->set(Util::createDate(2020,4,27), code_vec);
    script="z=dict(DATE,any); def add2dict(mutable z,d){ for(i in 0:d.size()) z[2020.04.26+i]=d[2020.04.26+i];}";
    conn.run(script);
    vector<ConstantSP> args14;
    args14.push_back(adtdict);
    conn.run("add2dict{z}",args14);
    dict=conn.run("z");
    ASSERTION("testDictionary11",dict->size(),2);
    ASSERTION("testDictionary11",dict->get(Util::createDate(2020,4,27))->get(0)->getString(),string("000001.SZ"));
    ASSERTION("testDictionary11",dict->get(Util::createDate(2020,4,26))->get(1)->getString(),string("000002.SZ"));


    adtdict=Util::createDictionary(DT_DATETIME,DT_ANY);
    adtdict->set(Util::createDateTime(2020,4,26,0,0,0), code_vec);
    adtdict->set(Util::createDateTime(2020,4,26,0,0,1), code_vec);
    script="z=dict(DATETIME,any); def add2dict(mutable z,d){ for(i in 0:d.size()) z[2020.04.26T00:00:00+i]=d[2020.04.26T00:00:00+i];}";
    conn.run(script);
    vector<ConstantSP> args15;
    args15.push_back(adtdict);
    conn.run("add2dict{z}",args15);
    dict=conn.run("z");
    ASSERTION("testDictionary12",dict->size(),2);
    ASSERTION("testDictionary12",dict->get(Util::createDateTime(2020,4,26,0,0,0))->get(0)->getString(),string("000001.SZ"));
    ASSERTION("testDictionary12",dict->get(Util::createDateTime(2020,4,26,0,0,1))->get(1)->getString(),string("000002.SZ"));

    adtdict=Util::createDictionary(DT_TIMESTAMP,DT_ANY);
    adtdict->set(Util::createTimestamp(2020,4,26,0,0,0,0), code_vec);
    adtdict->set(Util::createTimestamp(2020,4,26,0,0,0,1), code_vec);
    script="z=dict(TIMESTAMP,any); def add2dict(mutable z,d){ for(i in 0:d.size()) z[2020.04.26T00:00:00.000+i]=d[2020.04.26T00:00:00.000+i];}";
    conn.run(script);
    vector<ConstantSP> args16;
    args16.push_back(adtdict);
    conn.run("add2dict{z}",args16);
    dict=conn.run("z");
    ASSERTION("testDictionary13",dict->size(),2);
	
    ASSERTION("testDictionary13",dict->get(Util::createTimestamp(2020,4,26,0,0,0,0))->get(0)->getString(),string("000001.SZ"));
    ASSERTION("testDictionary13",dict->get(Util::createTimestamp(2020,4,26,0,0,0,1))->get(1)->getString(),string("000002.SZ"));
	
	
    adtdict=Util::createDictionary(DT_SECOND,DT_ANY);
    adtdict->set(Util::createSecond(12,0,0), code_vec);
    adtdict->set(Util::createSecond(12,0,1), code_vec);
    script="z=dict(SECOND,any); def add2dict(mutable z,d){ for(i in 0:d.size()) z[12:00:00+i]=d[12:00:00+i];}";
    conn.run(script);
    vector<ConstantSP> args17;
    args17.push_back(adtdict);
    conn.run("add2dict{z}",args17);
    dict=conn.run("z");
    ASSERTION("testDictionary14",dict->size(),2);
    ASSERTION("testDictionary14",dict->get(Util::createSecond(12,0,0))->get(0)->getString(),string("000001.SZ"));
    ASSERTION("testDictionary14",dict->get(Util::createSecond(12,0,1))->get(1)->getString(),string("000002.SZ"));

    adtdict=Util::createDictionary(DT_MINUTE,DT_ANY);
    adtdict->set(Util::createMinute(12,0), code_vec);
    adtdict->set(Util::createMinute(12,1), code_vec);
    script="z=dict(MINUTE,any); def add2dict(mutable z,d){ for(i in 0:d.size()) z[12:00m+i]=d[12:00m+i];}";
    conn.run(script);
    vector<ConstantSP> args18;
    args18.push_back(adtdict);
    conn.run("add2dict{z}",args18);
    dict=conn.run("z");
    ASSERTION("testDictionary15",dict->size(),2);
    ASSERTION("testDictionary15",dict->get(Util::createMinute(12,0))->get(0)->getString(),string("000001.SZ"));
    ASSERTION("testDictionary15",dict->get(Util::createMinute(12,0))->get(1)->getString(),string("000002.SZ"));

    adtdict=Util::createDictionary(DT_NANOTIMESTAMP,DT_ANY);
    adtdict->set(Util::createNanoTimestamp(2020,4,26,0,0,0,0), code_vec);
    adtdict->set(Util::createNanoTimestamp(2020,4,26,0,0,0,1), code_vec);
    script="z=dict(NANOTIMESTAMP,any); def add2dict(mutable z,d){ for(i in 0:d.size()) z[2020.04.26T00:00:00.000000000+i]=d[2020.04.26T00:00:00.000000000+i];}";
    conn.run(script);
    vector<ConstantSP> args1a;
    args1a.push_back(adtdict);
    conn.run("add2dict{z}",args1a);
    dict=conn.run("z");
    ASSERTION("testDictionary17",dict->size(),2);
    ASSERTION("testDictionary17",dict->get(Util::createNanoTimestamp(2020,4,26,0,0,0,0))->get(0)->getString(),string("000001.SZ"));
    ASSERTION("testDictionary17",dict->get(Util::createNanoTimestamp(2020,4,26,0,0,0,1))->get(1)->getString(),string("000002.SZ"));

}
void testSet(){
    string script;
    script += "x=set(4 5 5 2 3 11 11 11 6 6  6 6  6);x;";
    SetSP set = conn.run(script);
    ASSERTION("testSet",set->size(),6);

    script = "y=(int128(\"1234567890abcdef1234567890abcdef\"),int128(\"1234567890abcdef1234567890abcdef\"));y";
    set= conn.run(script);
    ASSERTION("testSet",set->size(),2);
    ASSERTION("testDictionary",set->get(0)->getString(),string("1234567890abcdef1234567890abcdef"));
    ASSERTION("testDictionary",set->get(1)->getString(),string("1234567890abcdef1234567890abcdef"));

    script = "z=(uuid('12345678-90ab-cdef-1234-567890abcdef'),uuid('12345678-90ab-cdef-1234-567890abcde0'));z";
    set= conn.run(script);
    ASSERTION("testSet",set->size(),2);
    ASSERTION("testDictionary",set->get(0)->getString(),string("12345678-90ab-cdef-1234-567890abcdef"));
    ASSERTION("testDictionary",set->get(1)->getString(),string("12345678-90ab-cdef-1234-567890abcde0"));

    script = "u=(ipaddr('4a80:5098:a255:cf1e:d4b8:3f69:d1fb:a6fd'),ipaddr('c93b:83f1:3b57:9d8e:ea21:f23a:4ded:4949'),ipaddr('bb94:24d5:33f9:9363:f15c:f929:abde:9d19'),ipaddr('4782:29a8:6f0f:aa7b:73b9:759c:d81e:e42e'),ipaddr('4aac:44d8:18fd:40fe:60c5:5b43:6449:f0e1'),ipaddr('7069:8a02:3385:a5a4:60f3:5206:d69:bcde'));u";
    set= conn.run(script);
    ASSERTION("testSet",set->size(),6);
    ASSERTION("testDictionary",set->get(0)->getString(),string("4a80:5098:a255:cf1e:d4b8:3f69:d1fb:a6fd"));
    ASSERTION("testDictionary",set->get(1)->getString(),string("c93b:83f1:3b57:9d8e:ea21:f23a:4ded:4949"));

    SetSP d=Util::createSet(DT_IP,1);
    d->append(Util::parseConstant(DT_IP,"c93b:83f1:3b57:9d8e:ea21:f23a:4ded:6060"));
    vector<ConstantSP> args;
    args.push_back(d);
    conn.run("append!{u}",args);
    set=conn.run("u");
    ASSERTION("testSet",set->size(),7);
}

void testMemoryTable(){
    string script;
    //simulation to generate data to be saved to the memory table
    VectorSP names = Util::createVector(DT_STRING,5,100);
    VectorSP dates = Util::createVector(DT_DATE,5,100);
    VectorSP prices = Util::createVector(DT_DOUBLE,5,100);
    for(int i = 0 ;i < 5;i++){
        names->set(i,Util::createString("name_"+std::to_string(i)));
        dates->set(i,Util::createDate(2010,1,i+1));
        prices->set(i,Util::createDouble(i*i));
    }
    vector<string> allnames = {"names","dates","prices"};
    vector<ConstantSP> allcols = {names,dates,prices};
    conn.upload(allnames,allcols);//upload data to server
    script += "insert into tglobal values(names,dates,prices);";
    script += "select * from tglobal;";
    TableSP table = conn.run(script);
    cout<<table->getString()<<endl;
}

TableSP createDemoTable(){
    vector<string> colNames = {"name","date","price"};
    vector<DATA_TYPE> colTypes = {DT_STRING,DT_DATE,DT_DOUBLE};
    int colNum = 3,rowNum = 3;
    ConstantSP table = Util::createTable(colNames,colTypes,rowNum,100);
    vector<VectorSP> columnVecs;
    for(int i = 0 ;i < colNum ;i ++)
        columnVecs.push_back(table->getColumn(i));

    for(int i =  0 ;i < rowNum; i++){
        columnVecs[0]->set(i,Util::createString("name_"+std::to_string(i)));
        columnVecs[1]->set(i,Util::createDate(2010,1,i+1));
        columnVecs[2]->set(i,Util::createDouble(i*i));
    }
    return table;
}


void testDiskTable(){
    TableSP table = createDemoTable();
    conn.upload("mt",table);
    string script;
    script += "db=database(\"/home/psui/demoTable1\");";
    script += "tDiskGlobal.append!(mt);";
    script += "saveTable(db,tDiskGlobal,`dt);";
    script += "select * from tDiskGlobal;";
    TableSP result = conn.run(script);
    cout<<result->getString()<<endl;
}

void testDFSTable(){
    string script;
    TableSP table = createDemoTable();
    conn.upload("mt",table);
    script += "login(`admin,`123456);";
    script += "dbPath = \"dfs://SAMPLE_TRDDB\";";
    script += "tableName = `demoTable;";
    script += "database(dbPath).loadTable(tableName).append!(mt);";
    script += "tradTable= database(dbPath).loadTable(tableName);";
    script += "select * from tradTable;";
    TableSP result = conn.run(script);
    cout<<result->getString()<<endl;
}

void ASSERTIONHASH(const string test, const int ret[], const int expect[],int len) {
	bool equal=true;
	for(int i=0;i<len;i++){
		if(ret[i]!=expect[i])
			equal=false;
	}

    if(!equal){
        std::cout<<"ASSERT FAIL--" << test<< "-- expect return --";
    	for(int i=0;i<len;i++)
    		std::cout<<expect[i]<<",";
    	cout<<", real return--";
    	for(int i=0;i<len;i++)
    		std::cout<<ret[i]<<",";
    	cout<< std::endl;
        fail++;
    }
    else
        pass++;

}
void testCharVectorHash(){
    vector<char> testValues{127,-127,12,0,-12,-128};
    int buckets[5]={13,43,71,97,4097};
    int expected[5][6]={
    		{10,12,12,0,10,-1},
    		{41,18,12,0,4,-1},
    		{56,24,12,0,68,-1},
    		{30,5,12,0,23,-1},
    		{127,129,12,0,244,-1}};
    int hv[6]={0};

    for(unsigned int j=0;j<5;j++){
		for(unsigned int i = 0 ;i < testValues.size(); i++){
			ConstantSP val=Util::createChar(testValues[i]);
			hv[i]=val->getHash(buckets[j]);
		}
		ASSERTIONHASH("testCharVectorHash",hv,expected[j],6);
    }
    VectorSP v=Util::createVector(DT_CHAR,0);
    v->appendChar(testValues.data(),testValues.size());
    for(unsigned int j=0;j<5;j++){
		v->getHash(0,6,buckets[j],hv);
		ASSERTIONHASH("testCharVectorHash",hv,expected[j],6);
    }
}

void testShortVectorHash(){
    vector<short> testValues{32767,-32767,12,0,-12,-32768};
    int buckets[5]={13,43,71,97,4097};
    int expected[5][6]={
    		{7,2,12,0,10,-1},
    		{1,15,12,0,4,-1},
    		{36,44,12,0,68,-1},
    		{78,54,12,0,23,-1},
    		{4088,265,12,0,244,-1}};
    int hv[6]={0};

    for(unsigned int j=0;j<5;j++){
		for(unsigned int i = 0 ;i < testValues.size(); i++){
			ConstantSP val=Util::createShort(testValues[i]);
			hv[i]=val->getHash(buckets[j]);
		}
		ASSERTIONHASH("testShortVectorHash",hv,expected[j],6);
    }
    VectorSP v=Util::createVector(DT_SHORT,0);
    v->appendShort(testValues.data(),testValues.size());
    for(unsigned int j=0;j<5;j++){
		v->getHash(0,6,buckets[j],hv);
		ASSERTIONHASH("testShortVectorHash",hv,expected[j],6);
    }
}
void testIntVectorHash(){
    vector<int> testValues{INT_MAX,INT_MAX*(-1),12,0,-12,INT_MIN};
    int buckets[5]={13,43,71,97,4097};
    int expected[5][6]={
    		{10,12,12,0,10,-1},
    		{7,9,12,0,4,-1},
    		{39,41,12,0,68,-1},
    		{65,67,12,0,23,-1},
    		{127,129,12,0,244,-1}};
    int hv[6]={0};

    for(unsigned int j=0;j<5;j++){
		for(unsigned int i = 0 ;i < testValues.size(); i++){
			ConstantSP val=Util::createInt(testValues[i]);
			hv[i]=val->getHash(buckets[j]);
		}
		ASSERTIONHASH("testIntVectorHash",hv,expected[j],6);
    }
    VectorSP v=Util::createVector(DT_INT,0);
    v->appendInt(testValues.data(),testValues.size());
    for(unsigned int j=0;j<5;j++){
		v->getHash(0,6,buckets[j],hv);
		ASSERTIONHASH("testIntVectorHash",hv,expected[j],6);
    }
}

void testLongVectorHash(){
    vector<long long> testValues{LLONG_MAX,(-1)*LLONG_MAX,12,0,-12,LLONG_MIN};
    int buckets[5]={13,43,71,97,4097};
    int expected[5][6]={
    		{7,9,12,0,4,-1},
    		{41,0,12,0,29,-1},
    		{4,6,12,0,69,-1},
    		{78,80,12,0,49,-1},
    		{4088,4090,12,0,4069,-1}};
    int hv[6]={0};

    for(unsigned int j=0;j<5;j++){
		for(unsigned int i = 0 ;i < testValues.size(); i++){
			ConstantSP val=Util::createLong(testValues[i]);
			hv[i]=val->getHash(buckets[j]);
		}
		ASSERTIONHASH("testLongVectorHash",hv,expected[j],6);
    }
    VectorSP v=Util::createVector(DT_LONG,0);
    v->appendLong(testValues.data(),testValues.size());
    for(unsigned int j=0;j<5;j++){
		v->getHash(0,6,buckets[j],hv);
		ASSERTIONHASH("testLongVectorHash",hv,expected[j],6);
    }
}


void testStringVectorHash(){
    vector<string> testValues{"9223372036854775807","helloworldabcdefghijklmnopqrstuvwxyz","智臾科技","hello,智臾科技","123abc您好！",""};
    int buckets[5]={13,43,71,97,4097};
    int expected[5][6]={
            {8,1,11,8,10,0},
            {37,20,14,23,41,0},
            {31,0,41,63,40,0},
            {24,89,51,54,42,0},
            {739,3737,814,3963,3488,0}};
    int hv[6]={0};

    for(unsigned int j=0;j<5;j++){
		for(unsigned int i = 0 ;i < testValues.size(); i++){
			ConstantSP val=Util::createString(testValues[i]);
			hv[i]=val->getHash(buckets[j]);
		}
		ASSERTIONHASH("testShortVectorHash",hv,expected[j],6);
    }
    VectorSP v=Util::createVector(DT_STRING,0);
    v->appendString(testValues.data(),testValues.size());
    for(unsigned int j=0;j<5;j++){
		v->getHash(0,6,buckets[j],hv);
		ASSERTIONHASH("testShortVectorHash",hv,expected[j],6);
    }
}
void testUUIDvectorHash(){
    string script;
    script = "a=rand(uuid(),6);table(a as k,hashBucket(a,13) as v1,hashBucket(a,43) as v2,hashBucket(a,71) as v3,hashBucket(a,97) as v4,hashBucket(a,4097) as v5)";
    TableSP t = conn.run(script);

    int buckets[5]={13,43,71,97,4097};
    int hv[6]={0};
    int expected[5][6]={0};

    for(unsigned int j=0;j<5;j++){
 		for( int i = 0 ;i < t->size(); i++){
 			ConstantSP val=Util::parseConstant(DT_UUID,t->getColumn(0)->getString(i));
 			hv[i]=val->getHash(buckets[j]);
 			expected[j][i]=t->getColumn(j+1)->getInt(i);
 		}
 		ASSERTIONHASH("testUUIDvectorHash",hv,expected[j],6);
    }
    VectorSP v=Util::createVector(DT_UUID,0);
    for( int i = 0 ;i < t->size(); i++)
    	v->append(Util::parseConstant(DT_UUID,t->getColumn(0)->getString(i)));
    for(unsigned int j=0;j<5;j++){
 		v->getHash(0,6,buckets[j],hv);
 		ASSERTIONHASH("testUUIDvectorHash",hv,expected[j],6);
    }

}
void testIpAddrvectorHash(){
    string script;
    script = "a=rand(ipaddr(),6);table(a as k,hashBucket(a,13) as v1,hashBucket(a,43) as v2,hashBucket(a,71) as v3,hashBucket(a,97) as v4,hashBucket(a,4097) as v5)";
    TableSP t = conn.run(script);

    int buckets[5]={13,43,71,97,4097};
    int hv[6]={0};
    int expected[5][6]={0};

    for(unsigned int j=0;j<5;j++){
 		for( int i = 0 ;i < t->size(); i++){
 			ConstantSP val=Util::parseConstant(DT_IP,t->getColumn(0)->getString(i));
 			hv[i]=val->getHash(buckets[j]);
 			expected[j][i]=t->getColumn(j+1)->getInt(i);
 		}
 		ASSERTIONHASH("testIpAddrvectorHash",hv,expected[j],6);
    }
    VectorSP v=Util::createVector(DT_IP,0);
    for( int i = 0 ;i < t->size(); i++)
        v->append(Util::parseConstant(DT_IP,t->getColumn(0)->getString(i)));
    for(unsigned int j=0;j<5;j++){
 		v->getHash(0,6,buckets[j],hv);
 		ASSERTIONHASH("testIpAddrvectorHash",hv,expected[j],6);
    }

}
void testInt128vectorHash(){
    string script;
    script = "a=rand(int128(),6);table(a as k,hashBucket(a,13) as v1,hashBucket(a,43) as v2,hashBucket(a,71) as v3,hashBucket(a,97) as v4,hashBucket(a,4097) as v5)";
    TableSP t = conn.run(script);

    int buckets[5]={13,43,71,97,4097};
    int hv[6]={0};
    int expected[5][6]={0};

    for(unsigned int j=0;j<5;j++){
 		for( int i = 0 ;i < t->size(); i++){
 			ConstantSP val=Util::parseConstant(DT_INT128,t->getColumn(0)->getString(i));
 			hv[i]=val->getHash(buckets[j]);
 			expected[j][i]=t->getColumn(j+1)->getInt(i);
 		}
 		ASSERTIONHASH("testInt128vectorHash",hv,expected[j],6);
    }
    VectorSP v=Util::createVector(DT_INT128,0);
    for( int i = 0 ;i < t->size(); i++)
        v->append(Util::parseConstant(DT_INT128,t->getColumn(0)->getString(i)));
    for(unsigned int j=0;j<5;j++){
 		v->getHash(0,6,buckets[j],hv);
 		ASSERTIONHASH("testInt128vectorHash",hv,expected[j],6);
    }
}


int main(int argc, char ** argv){
    DBConnection::initialize();
    bool ret = conn.connect(hostName,port);
    if(!ret){
		cout<<"Failed to connect to the server"<<endl;
		return 0;
	}
    int testVectorSize = 20;

    testStringVector(testVectorSize);
    testIntVector(testVectorSize);
    testDoubleVector(testVectorSize);
    testDateVector();
    testDatetimeVector();
    testTimeStampVector();
    testFunctionDef();
    testMarics();
    testTable();
    testDictionary();
    testSet();
    //testMemoryTable();
    //testDiskTable();
    //testDFSTable();

    testCharVectorHash();
    testShortVectorHash();
    testIntVectorHash();
    testLongVectorHash();
    testStringVectorHash();
    testUUIDvectorHash();
    testIpAddrvectorHash();
    testInt128vectorHash();

    string fileName(argv[1]);
    printTestResults(fileName);

    return 0;
}
