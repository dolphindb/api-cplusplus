#include "../include/DolphinDB.h"
#include "../include/Util.h"
#include <unistd.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <map>
#include "thread"

using namespace dolphindb;
using namespace std;

string hostName = "192.168.0.16";
int port = 9002;
int ctl_port=9000;
vector<string> AllDataNodes = {"192.168.0.16:9002:datanode1", "192.168.0.16:9003:datanode2", "192.168.0.16:9004:datanode3",\
"192.168.0.16:9005:datanode4","192.168.0.16:9006:datanode5","192.168.0.16:9007:datanode6","192.168.0.16:9008:datanode7"};

double getLoadDiff(){
    DBConnection conn1(false,false);
    conn1.connect(hostName, ctl_port, "admin", "123456");
    ConstantSP curNodeLoad = conn1.run("dnload=take(double(0),7)\n\
				for (i in 0..6){\n\
				nodeload =exec double(workerNum+executorNum+connectionNum)/3 from  getClusterPerf() where name = \"datanode\"+string(i+1)\n\
				dnload[i]=nodeload[0]}\n\
				max(dnload)-min(dnload)");// 获取当前节点的负载，并与其他节点负载求差，检查每次连接时最大的差值是否过大
    conn1.close();
	return curNodeLoad->getDouble();
}

// void connTask(vector<string> AllDataNodes){
// 	DBConnection conn;
// 	conn.connect(hostName, port, "admin", "123456", "", true, AllDataNodes, 7200, false);
// 	std::this_thread::sleep_for(std::chrono::seconds(5));
// 	string cur_node = conn.run("getNodeAlias()")->getString();
// 	cout<<"Courrent datanode is "<<cur_node<<endl;
// 	conn.close();
// }

// int main(){
// 	int num=10;
// 	vector<thread> th1Vec;
// 	vector<thread> th2Vec;
// 	for(int i=0;i<num;i++){
// 		th1Vec.emplace_back(thread(connTask,AllDataNodes));
// 		th2Vec.emplace_back(thread(getLoadDiff));
// 	}
// 	for(auto &t1:th1Vec){
// 		sleep(2);
// 		t1.join();
// 		cout<<"down"<<endl;
// 	}
// 	for(auto &t2:th2Vec){
// 		sleep(2);
// 		t2.join();

// 	}

// 	return 0;
// }

int main(){
	int insertRows=7;
	DBConnection conn_ctl;
	conn_ctl.connect(hostName,ctl_port,"admin","123456");
	pid_t pid;
	int num=1;
	int status; //定义状态码变量
	cout<<"creating conn task total:"+to_string(num)<<endl;
	for(int i=0;i<num;i++){
        pid = fork();
		sleep(2);
        //子进程退出循环，不再创建子进程，全部由主进程创建子进程
        if(pid==0||pid==-1)
        {
            break;
        }
	}
    if(pid==-1)
    {
        cout<<"fail to fork!"<<endl;
        exit(-1);
    }
    else if(pid==0)
    {
        //子进程
		double tm_start = (double)Util::getEpochTime()/(double)1000;
		cout<<endl<<"current process PID=" <<getpid()<<endl;
		DBConnection conn;
		conn.connect(hostName, port, "admin", "123456", "", true, AllDataNodes, 7200, false);
		sleep(1);
		cout<<"now the max load diff= "<<getLoadDiff()<<endl;
		string cur_node = conn.run("getNodeAlias()")->getString();
		cout<<"Courrent datanode is "<<cur_node<<endl;
        conn.run("a=table(1 2 3 as col1,`a`b`c as col2);\
									for(i in 1..100)\
									{tableInsert(a,rand(100,1),rand(`d`e`f`g`h`i`j`k`l`m`n,1));sleep(2000)}");
		double tm_end = (double)Util::getEpochTime()/(double)1000;
		cout<<"PID:"+to_string(getpid())+" spend time: "<<tm_end-tm_start<<endl;
		conn.close();
		exit(0);
    }
	else{
		wait(&status); //等待子进程退出
	}
	sleep(num*2);
	return 0;
}

