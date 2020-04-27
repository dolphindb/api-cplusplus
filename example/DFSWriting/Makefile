CFLAGS = -g -Wall -std=c++11 -fPIC -DLINUX -DLOGGING_LEVEL_2 -DLINUX -O2 
INCLUDES= -I../../include
OBJS=DFSWriting.o

all: $(OBJS)
	g++ -L../../bin/linux_x64 -L../../bin/mingw_x64 -L../../bin/vx2017_x64 $(OBJS) $(CFLAGS)  -lDolphinDBAPI -lssl  -lpthread -luuid -L../../bin  -Wl,-rpath,.:../../bin/linux_x64/ -o DFSWriting 
DFSWriting.o: DFSWriting.cpp
	g++ $(CFLAGS) $(INCLUDES) -c DFSWriting.cpp -o DFSWriting.o
clean:
	rm *.o DFSWriting  core -rf
	
	
	
