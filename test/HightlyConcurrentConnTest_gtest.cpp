class HightlyConcurrentConnTest:public testing::Test
{
protected:
    //Suite
    static void SetUpTestCase() {
        //DBConnection conn;
        conn_compress.initialize();
        bool ret = conn_compress.connect(hostName, port, "admin", "123456");
        if (!ret) {
            cout << "Failed to connect to the server" << endl;
        }
        else {
            conn_compress.initialize();
            cout << "connect to " + hostName + ":" + std::to_string(port)<< endl;
            string script1 = "login(`admin,`123456)\n\
                    if (existsDatabase(\"dfs://Quote_test_DB\")) {\
                    dropDatabase(\"dfs://Quote_test_DB\")}\
                    \n\
                    a=table(`XYZF`XYZG`XYZH`XYZJ`XYZK`XYZM`XYZN`XYZQ`XYZU`XYZV`XYZX`XYZZ as sym)\n\
                    b=table(0..9 as y)\n\
                    n = 1200\n\
                    m = 1000\n\
                    date1= 2010.01.01T00:00:00.000+rand(500000000000,n)\n\
                    date2= 2010.01.01T00:00:00.000+rand(500000000000,m)\n\
                    contract1=rand(exec string(sym) + string(y) as contract from cj(a,b),n)\n\
                    contract2=rand(exec string(sym) + string(y) as contract from cj(a,b),m)\n\
                    \n\
                    price1=rand(20.0,n)\n\
                    price2=rand(20.0,m)\n\
                    quotess1= table(date1 as date, contract1 as contract, price1 as price)\n\
                    quotess2= table(date2 as date, contract2 as contract, price2 as price)\n\
                    //l=select top 10* from quotess\n\
                    //schema(quotess1)\n\
                    \n\
                    dbDate = database(, VALUE, 2010.01M..2022.12M)\n\
                    dbsym=database(, HASH,[SYMBOL,10]);\
                    db = database(\"dfs://Quote_test_DB\", COMPO, [dbDate, dbsym]);\
                    \n\
                    Quote_test= db.createPartitionedTable(quotess1, `Quote_test, `date`contract)\n\
                    //Quote_test=loadTable(\"dfs://Quote_test_DB\",`Quote_test)\n\
                    \n\
                    Quote_test.append!(quotess1);\
                    Quote_test.append!(quotess2);";
        conn_compress.run(script1);
        }
    }
    static void TearDownTestCase(){
        conn_compress.close();
    }

    //Case
    virtual void SetUp()
    {
        cout<<"check connect...";
		ConstantSP res = conn_compress.run("1+1");
		if(!(res->getBool())){
			cout<<"Server not responed, please check."<<endl;
		}
		else
		{
			cout<<"ok"<<endl;
		}
    }
    virtual void TearDown()
    {
        pass;
    }
};

void job(int id){
	//DBConnection conn(false,false);  // 出错
	DBConnection conn_withKeepAliveTime(false,false,7200);
	bool ret = conn_withKeepAliveTime.connect(hostName, port, "admin", "123456");
	if(!ret){
        cout<<"Failed to connect to the server"<<endl;
        return;
    }
    string tb1="tb"+to_string(id);
    string script1 = tb1+"=select * from Quote_test;rows("+tb1+")";
	
    ConstantSP re = conn_withKeepAliveTime.run("Quote_test=loadTable('dfs://Quote_test_DB',`Quote_test)");
    ConstantSP row1 = conn_withKeepAliveTime.run(script1);
    conn_withKeepAliveTime.close();

    EXPECT_EQ(row1->getInt(),2200);
}

void job2(int id){
	DBConnection conn_withDefaultKeepAliveTime(false,false);
	bool ret = conn_withDefaultKeepAliveTime.connect(hostName, port, "admin", "123456");
	if(!ret){
        cout<<"Failed to connect to the server"<<endl;
        return;
    }
    string tb2="tb"+to_string(id);
    string script2 = tb2+"=select * from Quote_test;rows("+tb2+")";

	ConstantSP re = conn_withDefaultKeepAliveTime.run("Quote_test=loadTable('dfs://Quote_test_DB',`Quote_test)");
    ConstantSP row2 = conn_withDefaultKeepAliveTime.run(script2);
    conn_withDefaultKeepAliveTime.close();

    EXPECT_EQ(row2->getInt(),2200);    
}

TEST_F(HightlyConcurrentConnTest,test_HightlyConcurrentConnWithKeepAliveTime) {
	std::thread th[500];
    int sum=0;
	for (int i = 0; i < 500; i++){
		th[i] = std::thread(job, i);
	}
	for (int i = 0; i < 500; i++){
		th[i].join();
        sum+=1;
	}
    EXPECT_EQ(sum,500);
}

TEST_F(HightlyConcurrentConnTest,test_HightlyConcurrentConnWithDefaultKeepAliveTime) {
	std::thread th[500];
    int sum=0;
	for (int i = 0; i < 500; i++){
		th[i] = std::thread(job2, i);
	}
	for (int i = 0; i < 500; i++){
		th[i].join();
        sum+=1;
	}
    EXPECT_EQ(sum,500);
}