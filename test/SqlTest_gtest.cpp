#include <gtest/gtest.h>
#include "config.h"

class SqlTest : public testing::Test
{
    public:
        static dolphindb::DBConnection conn;
        // Suite
        static void SetUpTestSuite()
        {
            bool ret = conn.connect(HOST, PORT, USER, PASSWD);
            if (!ret)
            {
                std::cout << "Failed to connect to the server" << std::endl;
            }
            else
            {
                std::cout << "connect to " + HOST + ":" + std::to_string(PORT) << std::endl;
            }
        }
        static void TearDownTestSuite()
        {
            conn.close();
        }

    protected:
        // Case
        virtual void SetUp()
        {

        }
        virtual void TearDown()
        {

        }
};

dolphindb::DBConnection SqlTest::conn(false, false);

TEST_F(SqlTest, test_selectNULL)
{
    std::string sql1 = "select *, NULL as val from table(1..100 as id)";
    dolphindb::TableSP res = conn.run(sql1);
    dolphindb::VectorSP ex_col0 = conn.run("table(1..100 as id)[`id]");
    ASSERT_EQ(res->columns(), 2);
    ASSERT_EQ(res->rows(), ex_col0->size());
    ASSERT_EQ(res->getColumn(1)->getType(), dolphindb::DT_VOID);
    for (auto i = 0; i < ex_col0->size(); i++)
    {
        ASSERT_EQ(ex_col0->get(i)->getInt(), res->getColumn(0)->get(i)->getInt());
        ASSERT_TRUE(res->getColumn(1)->get(i)->isNull());
    }

    std::string sql2 = "select NULL as val from table(1..100 as id)";
    dolphindb::TableSP res2 = conn.run(sql2);
    ASSERT_EQ(res2->columns(), 1);
    ASSERT_EQ(res2->rows(), 1);
    ASSERT_EQ(res2->getColumn(0)->getType(), dolphindb::DT_VOID);
    ASSERT_EQ(res2->getString(), "val\n"
                                "---\n"
                                "   \n");
    ASSERT_TRUE(res2->getColumn(0)->get(0)->isNull());
    std::vector<dolphindb::ConstantSP> tabs = {res,res2};
    std::vector<std::string> names = {"t1_snull","t2_snull"};

    conn.upload(names,tabs);
    std::string assert_s1 = "res = bool([]);res.append!(eqObj(t1_snull.column(0), 1..100));res.append!(t1_snull.column(1).isNull());all(all(res))";
    std::string assert_s2 = "res = bool([]);res.append!(t2_snull.column(0).isNull());all(all(res))";
    ASSERT_TRUE(conn.run(assert_s1)->getBool());
    ASSERT_TRUE(conn.run(assert_s2)->getBool());

}

TEST_F(SqlTest, test_selectNULL_from_hugeTable)
{
    dolphindb::TableSP t = conn.run("t = table(1..20000000 as id, rand(`a`c`sd``qx, 20000000) as sym);t");
    std::string sql1 = "select *, NULL as null_c1, NULL as null_c2, NULL as null_c3, NULL as null_c4 from t";
    dolphindb::TableSP res = conn.run(sql1);
    ASSERT_EQ(res->columns(), 6);

    for(auto j=0;j<t->rows();j++){
        ASSERT_EQ(t->getColumn(0)->get(j)->getInt(), res->getColumn(0)->get(j)->getInt());
        ASSERT_EQ(t->getColumn(1)->get(j)->getString(), res->getColumn(1)->get(j)->getString());
    }

    for (auto i = 2; i < res->columns(); i++)
    {
        ASSERT_EQ(res->getColumnName(i), "null_c"+std::to_string(i-1));
        for(auto j = 0; j < res->rows(); j++){
            ASSERT_TRUE(res->getColumn(i)->isNull(j));
        }
    }

}
