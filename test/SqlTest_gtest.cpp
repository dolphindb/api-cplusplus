#include "config.h"

class SqlTest : public testing::Test
{
protected:
    // Suite
    static void SetUpTestCase()
    {
        // DBConnection conn;
        conn.initialize();
        bool ret = conn.connect(hostName, port, "admin", "123456");
        if (!ret)
        {
            cout << "Failed to connect to the server" << endl;
        }
        else
        {
            cout << "connect to " + hostName + ":" + std::to_string(port) << endl;
        }
    }
    static void TearDownTestCase()
    {
        conn.close();
    }

    // Case
    virtual void SetUp()
    {
        cout << "check connect...";
		try
		{
			ConstantSP res = conn.run("1+1");
		}
		catch(const std::exception& e)
		{
			conn.connect(hostName, port, "admin", "123456");
		}

        cout << "ok" << endl;
    }
    virtual void TearDown()
    {
        conn.run("undef all;");
    }
};

TEST_F(SqlTest, test_selectNULL)
{
    string sql1 = "select *, NULL as val from table(1..100 as id)";
    TableSP res = conn.run(sql1);
    VectorSP ex_col0 = conn.run("table(1..100 as id)[`id]");
    EXPECT_EQ(res->columns(), 2);
    EXPECT_EQ(res->rows(), ex_col0->size());
    EXPECT_EQ(res->getColumn(1)->getType(), DT_VOID);
    for (auto i = 0; i < ex_col0->size(); i++)
    {
        EXPECT_EQ(ex_col0->get(i)->getInt(), res->getColumn(0)->get(i)->getInt());
        EXPECT_TRUE(res->getColumn(1)->get(i)->isNull());
    }

    string sql2 = "select NULL as val from table(1..100 as id)";
    TableSP res2 = conn.run(sql2);
    EXPECT_EQ(res2->columns(), 1);
    EXPECT_EQ(res2->rows(), 1);
    EXPECT_EQ(res2->getColumn(0)->getType(), DT_VOID);
    EXPECT_EQ(res2->getString(), "val\n"
                                "---\n"
                                "   \n");
    EXPECT_TRUE(res2->getColumn(0)->get(0)->isNull());
    vector<ConstantSP> tabs = {res,res2};
    vector<string> names = {"t1_snull","t2_snull"};

    conn.upload(names,tabs);
    string assert_s1 = "res = bool([]);res.append!(eqObj(t1_snull.column(0), 1..100));res.append!(t1_snull.column(1).isNull());all(res)";
    string assert_s2 = "res = bool([]);res.append!(t2_snull.column(0).isNull());all(res)";
    EXPECT_TRUE(conn.run(assert_s1)->getBool());
    EXPECT_TRUE(conn.run(assert_s2)->getBool());

}

TEST_F(SqlTest, test_selectNULL_from_hugeTable)
{
    TableSP t = conn.run("t = table(1..20000000 as id, rand(`a`c`sd``qx, 20000000) as sym);t");
    string sql1 = "select *, NULL as null_c1, NULL as null_c2, NULL as null_c3, NULL as null_c4 from t";
    TableSP res = conn.run(sql1);
    EXPECT_EQ(res->columns(), 6);

    for(auto j=0;j<t->rows();j++){
        EXPECT_EQ(t->getColumn(0)->get(j)->getInt(), res->getColumn(0)->get(j)->getInt());
        EXPECT_EQ(t->getColumn(1)->get(j)->getString(), res->getColumn(1)->get(j)->getString());
    }

    for (auto i = 2; i < res->columns(); i++)
    {
        // cout<<res->getColumn(i)->getString()<<endl;
        EXPECT_EQ(res->getColumnName(i), "null_c"+to_string(i-1));
        for(auto j = 0; j < res->rows(); j++){
            EXPECT_TRUE(res->getColumn(i)->isNull(j));
        }
    }

}
