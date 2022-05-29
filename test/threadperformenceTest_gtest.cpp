using namespace std; 

class FooTest : public testing::Test {
public:
    static void SetUpTestCase() 
    {
        cout<<"SetUpTestCase()"<<endl;
        string script1;
        string DATA_FIRE="/home/yzou/Api_test/DATA/USPrices.csv";
        // string dbName = "dfs://test_MultithreadedTableWriter";
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
    }
    static void TearDownTestCase() 
    {
        cout<<"TearDownTestCase()"<<endl;
    }
    // Some expensive resource shared by all tests.
    // void SetUp()
    // {
    //     cout<<"SetUp()"<<endl;
    //     mp.insert(make_pair(1,1));
    //     mp.insert(make_pair(2,1));
    //     mp.insert(make_pair(3,1));
    //     mp.insert(make_pair(4,1));
    // }

    // void TearDown()
    // {
    //     mp.clear();
    //     cout<<"TearDown()"<<endl;
    // }
    // SmartPointer<MultithreadedTableWriter> mulwrite;
    // SmartPointer<BatchTableWriter> batchwrite;
};

TEST_F(FooTest, test_MultithreadedTableWriter_performence_threadCount_1)
 {
    // you can refer to shared_resource here 
    string dbName = "dfs://test_MultithreadedTableWriter";
    string DATA_FIRE="/home/yzou/Api_test/DATA/USPrices.csv";
    struct timeval tm_start, tm_end;
    double runTime=0;

    SmartPointer<MultithreadedTableWriter> mulwrite;
    ErrorCodeInfo pErrorInfo;
    mulwrite =new MultithreadedTableWriter(hostName,port, "admin","123456",dbName,"pt",NULL,false,NULL,1000,1,1,"date");
    ConstantSP bt = conn.run("loadText('"+DATA_FIRE+"')");
    MultithreadedTableWriter::Status status;
    // vector<vector<ConstantSP>> datas;
    // srand((int)time(NULL));
    gettimeofday(&tm_start, NULL);
    for(int i=0;i < bt->rows();i++){
        // mulwrite->insert(pErrorInfo,bt->getColumn(0)->get(i),bt->getColumn(1)->get(i),bt->getColumn(2)->get(i),bt->getColumn(3)->get(i),
        // bt->getColumn(4)->get(i),bt->getColumn(5)->get(i),bt->getColumn(6)->get(i),bt->getColumn(7)->get(i),bt->getColumn(8)->get(i),
        // bt->getColumn(9)->get(i),bt->getColumn(10)->get(i),bt->getColumn(11)->get(i),bt->getColumn(12)->get(i),bt->getColumn(13)->get(i),
        // bt->getColumn(14)->get(i),bt->getColumn(15)->get(i),bt->getColumn(16)->get(i),bt->getColumn(17)->get(i),bt->getColumn(18)->get(i),
        // bt->getColumn(19)->get(i),bt->getColumn(20)->get(i));
        mulwrite->insert(pErrorInfo,bt->getColumn(0)->get(i),bt->getColumn(1)->get(i),bt->getColumn(2)->get(i),"a",
        bt->getColumn(4)->get(i),bt->getColumn(5)->get(i),bt->getColumn(6)->get(i),"b","c",
        "d",bt->getColumn(10)->get(i),bt->getColumn(11)->get(i),bt->getColumn(12)->get(i),bt->getColumn(13)->get(i),
        bt->getColumn(14)->get(i),bt->getColumn(15)->get(i),bt->getColumn(16)->get(i),bt->getColumn(17)->get(i),bt->getColumn(18)->get(i),
        bt->getColumn(19)->get(i),bt->getColumn(20)->get(i));
    }
    if (pErrorInfo.errorCode != 0){
        cout<< pErrorInfo.errorInfo <<endl;
    }
    mulwrite->waitForThreadCompletion();
    gettimeofday(&tm_end, NULL); 
    runTime = (tm_end.tv_sec - tm_start.tv_sec ) + (double)(tm_end.tv_usec -tm_start.tv_usec)/1000000;
    cout<<"Test_MultithreadedTableWriter_performence_threadCount_1-> Time expended: " << runTime << "s" << endl;
}

TEST_F(FooTest, test_batchTableWriter_performence)
{
    string dbName = "dfs://test_MultithreadedTableWriter";
    string DATA_FIRE="/home/yzou/Api_test/DATA/USPrices.csv";
    struct timeval tm_start, tm_end;
    double runTime=0;

    SmartPointer<BatchTableWriter> batchwrite;
    batchwrite =new BatchTableWriter(hostName, port, "admin","123456",true);
    batchwrite->addTable(dbName,"pt");
    ConstantSP bt = conn.run("loadText('"+DATA_FIRE+"')");
    // vector<vector<ConstantSP>> datas;
    // srand((int)time(NULL));
    gettimeofday(&tm_start, NULL);
    for(int i=0;i < bt->rows();i++){ 
        // batchwrite->insert(dbName,"pt",bt->getColumn(0)->get(i),bt->getColumn(1)->get(i),bt->getColumn(2)->get(i),bt->getColumn(3)->get(i),
        // bt->getColumn(4)->get(i),bt->getColumn(5)->get(i),bt->getColumn(6)->get(i),bt->getColumn(7)->get(i),bt->getColumn(8)->get(i),
        // bt->getColumn(9)->get(i),bt->getColumn(10)->get(i),bt->getColumn(11)->get(i),bt->getColumn(12)->get(i),bt->getColumn(13)->get(i),
        // bt->getColumn(14)->get(i),bt->getColumn(15)->get(i),bt->getColumn(16)->get(i),bt->getColumn(17)->get(i),bt->getColumn(18)->get(i),
        // bt->getColumn(19)->get(i),bt->getColumn(20)->get(i));
        batchwrite->insert(dbName,"pt",bt->getColumn(0)->get(i),bt->getColumn(1)->get(i),bt->getColumn(2)->get(i),"SD",
        bt->getColumn(4)->get(i),bt->getColumn(5)->get(i),bt->getColumn(6)->get(i),"A","B",
        "C",bt->getColumn(10)->get(i),bt->getColumn(11)->get(i),bt->getColumn(12)->get(i),bt->getColumn(13)->get(i),
        bt->getColumn(14)->get(i),bt->getColumn(15)->get(i),bt->getColumn(16)->get(i),bt->getColumn(17)->get(i),bt->getColumn(18)->get(i),
        bt->getColumn(19)->get(i),bt->getColumn(20)->get(i));
    }
    do{
        TableSP result = batchwrite->getAllStatus();
        if(result->getColumn(3)->getString(0).c_str() ==  to_string(bt->rows())){
            gettimeofday(&tm_end, NULL); 
            runTime = (tm_end.tv_sec - tm_start.tv_sec ) + (double)(tm_end.tv_usec -tm_start.tv_usec)/1000000;
            cout<<"Test_batchTableWriter_performence-> Time expended: " << runTime << "s" << endl;
            break;
        }
    }while (true);

}
