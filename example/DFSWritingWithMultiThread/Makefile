CFLAGS = -g -Wall -std=c++11 -fPIC -DLINUX -DLOGGING_LEVEL_2 -DLINUX -D_GLIBCXX_USE_CXX11_ABI=0 -O2 
INCLUDES= -I../../include
OBJS=MultiThreadDfsWriting.o

all: $(OBJS)
	g++ -L../../bin/linux_x64/ABI0 $(OBJS) $(CFLAGS)  -lDolphinDBAPI   -lpthread -lssl  -L../../bin  -Wl,-rpath,.:../../bin/linux_x64/ABI0/ -o MultiThreadDfsWriting 
MultiThreadDfsWriting.o: MultiThreadDfsWriting.cpp
	g++ $(CFLAGS) $(INCLUDES) -c MultiThreadDfsWriting.cpp -o MultiThreadDfsWriting.o
clean:
	rm *.o MultiThreadDfsWriting  core -rf
	
	
	
