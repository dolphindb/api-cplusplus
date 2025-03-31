#include "DolphinDB.h"
#include "Util.h"
#include <iostream>
#include <string>
using namespace dolphindb;
using namespace std;

int main()
{
    DBConnection conn;
    bool ret = conn.connect("127.0.0.1", 8848);
    if (!ret) {
        cout << "Failed to connect to the server" << endl;
        return 0;
    }
    ConstantSP vector = conn.run("`IBM`GOOG`YHOO");
    int size = vector->rows();
    for (int i = 0; i < size; ++i)
        cout << vector->getString(i) << endl;
    return 0;
}
