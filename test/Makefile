CFLAGS = -g -Wall -std=c++11 -fPIC -DLINUX -DLOGGING_LEVEL_2 -DLINUX -O0 
INCLUDES= -I../include
OBJS=DolphinDBTester.o

all: $(OBJS)
	g++ -L../bin/linux_x64 -L../bin/mingw_x64 -L../bin/vx2017_x64 $(OBJS) $(CFLAGS)  -lDolphinDBAPI -lssl  -lpthread -luuid -L../bin  -Wl,-rpath ../bin/ -o DolphinDBTester 
DolphinDBTester.o: DolphinDBTester.cpp
	g++ $(CFLAGS) $(INCLUDES) -c DolphinDBTester.cpp -o DolphinDBTester.o
clean:
	rm *.o DolphinDBTester  core -rf
