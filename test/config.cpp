#include "config.h"
#ifdef WINDOWS
    #include <windows.h>
    #define GET_PID GetCurrentProcessId
#else
    #define GET_PID getpid
#endif

string hostName = "127.0.0.1";
string errCode = "0";
int port = 8902;
int ctl_port = 8900;

int port300 = 9002;
int ctl_port300 = 9000;

string table = "trades";
vector<int> listenPorts = {18901, 18902, 18903, 18904, 18905, 18906, 18907, 18908, 18909, 18910};
unordered_set<int> usedPorts{};
string alphas = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
int pass, fail;
bool assertObj = true;
int vecSize = 20;

int const INDEX_MAX_1 = 1;
int const INDEX_MIN_2 = -1;

vector<string> sites = {"127.0.0.1:8902:datanode1", "127.0.0.1:8903:datanode2", "127.0.0.1:8904:datanode3"};
vector<string> backupSites = {"127.0.0.1:8902", "127.0.0.1:8903", "127.0.0.1:8904"};
string raftsGroup = "11";
vector<string> nodeNames = {"datanode1", "datanode2", "datanode3"};

DBConnection conn(false, false);
DBConnection connReconn(false, false);
DBConnection conn_compress(false, false, 7200, true);
DBConnectionSP connsp(new DBConnection());
DBConnectionSP conn300(new DBConnection());

// check server version
bool isNewServer(DBConnection &conn, const int &major, const int &minor, const int &revision){
    VectorSP serverVersion = conn.run("int(version().split(' ')[0].split('.')).nullFill(0)");
    int serverMajor = serverVersion->get(0)->getInt();
    int serverMinor = serverVersion->get(1)->getInt();
    int serverRevision = serverVersion->get(2)->getInt();
    if(serverMajor > major || (serverMajor == major && serverMinor > minor) || (serverMajor == major && serverMinor == minor && serverRevision >= revision))
        return true;
    return false;
};

void checkAPIVersion()
{
    const char *api_version = std::getenv("API_VERSION");
    if (api_version == nullptr)
    {
        cout << "API_VERSION is not set, skip version check\n";
        return;
    }

    if (api_version == "")
    {
        cout << "API_VERSION is not set, skip version check\n";
        return;
    }

    string actual_version = Util::VER;
    if (string(api_version) == actual_version)
    {
        cout << "version check successfully" << endl;
    }
    else
    {
        throw RuntimeException("API_VERSION is not match to the env");
    }
}

string getRandString(int len){
    unsigned int seed = (unsigned)time(NULL) ^ (unsigned)GET_PID();
    srand(seed);
    string str;
    for (int i = 0; i < len; i++)
    {
        str += alphas[rand() % alphas.size()];
    }
    return str;
}

TableSP AnyVectorToTable(VectorSP vec)
{
    vector<string> colNames;
    vector<ConstantSP> columnVecs;
    auto col_count = vec->size();
    for(auto i=0;i<col_count;i++){
        colNames.emplace_back("col"+ to_string(i));
    }

    columnVecs.reserve(col_count);
    for (auto i=0;i<col_count;i++)
    {
        DATA_FORM form = vec->get(i)->getForm();
        DATA_TYPE _t = vec->get(i)->getType();
        DATA_TYPE type = form == DF_VECTOR && _t < ARRAY_TYPE_BASE ? static_cast<DATA_TYPE>(_t+ARRAY_TYPE_BASE):_t;
        int extraParam = vec->get(i)->getExtraParamForType();
        if (vec->get(i)->getForm() == DF_VECTOR){
            VectorSP avCol = Util::createArrayVector(type, 0, 0, true, extraParam);
            avCol->append(vec->get(i));
            columnVecs.emplace_back(avCol);
        }else{
            VectorSP col = Util::createVector(type, 0, 0, true, extraParam);
            for (auto j=0;j<vec->get(i)->rows();j++)
            {
                col->append(vec->get(i)->get(j));
            }
            columnVecs.emplace_back(col);
        }
    }

    return Util::createTable(colNames, columnVecs);
}