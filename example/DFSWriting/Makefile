CFLAGS = -g -Wall -std=c++11 -fPIC -DLINUX -DLOGGING_LEVEL_2 -DLINUX -O0 
INCLUDES= -I../../include
OBJS=dfsWriting.o

all: $(OBJS)
	g++ -L../../bin/linux_x64 -L../../bin/mingw_x64 -L../../bin/vx2017_x64 $(OBJS) $(CFLAGS)  -lDolphinDBAPI -lssl  -lpthread -luuid -L../../bin  -Wl,-rpath,.:../../bin/linux_x64/ -o dfsWriting 
dfsWriting.o: dfsWriting.cpp
	g++ $(CFLAGS) $(INCLUDES) -c dfsWriting.cpp -o dfsWriting.o
clean:
	rm *.o dfsWriting  core -rf
	
	
	