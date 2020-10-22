CFLAGS = -g -Wall -std=c++11 -fPIC -DLINUX -DLOGGING_LEVEL_2 -DLINUX -D_GLIBCXX_USE_CXX11_ABI=0 -O2 
INCLUDES= -I../../include
OBJS=DFSWriting.o

all: $(OBJS)
	g++ -L../../bin/linux_x64/ABI0 $(OBJS) $(CFLAGS)  -lDolphinDBAPI -lpthread -lssl -L../../bin  -Wl,-rpath,.:../../bin/linux_x64/ABI0/ -o DFSWriting 
DFSWriting.o: DFSWriting.cpp
	g++ $(CFLAGS) $(INCLUDES) -c DFSWriting.cpp -o DFSWriting.o
clean:
	rm *.o DFSWriting  core -rf
	
	
	
