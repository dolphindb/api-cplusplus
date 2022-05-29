CXXFLAGS = -g -Wall -I../include -std=c++11 -fPIC -DLINUX -DLOGGING_LEVEL_2 -DLINUX -std=c++11 -L../bin/linux_x64/ABI0 -D_GLIBCXX_USE_CXX11_ABI=0
LFLAGS=-lDolphinDBAPI  -lpthread -lssl  -L../bin/linux_x64/ABI0   -Wl,-rpath,.:../bin/linux_x64/ABI0/
SRCS=$(wildcard *.cpp)
OBJS=$(patsubst %.cpp,%.o,$(SRCS))
EXES=$(patsubst %.cpp,%,$(SRCS))
CC=g++

all : $(OBJS)
	$(CC)  DolphinDBTest.o $(CFLAGS) $(LFLAGS)  -o DolphinDBTest 
	$(CC)  DolphinDBTestINDEX_MAX.o $(CFLAGS) $(LFLAGS)  -o DolphinDBTestINDEX_MAX 
	$(CC)  streamingMultithreadSubUnsub.o $(CFLAGS) $(LFLAGS)  -o streamingMultithreadSubUnsub 
	$(CC)  streamingMulThreadMoreTables.o $(CFLAGS) $(LFLAGS)  -o streamingMulThreadMoreTables 
	$(CC)  streamingPressureSub.o $(CFLAGS) $(LFLAGS)  -o streamingPressureSub 
	$(CC)  streamingCheckNum.o $(CFLAGS) $(LFLAGS)  -o streamingCheckNum 
	$(CC)  StreamingThreadPooledClientTester.o $(CFLAGS) $(LFLAGS)  -o StreamingThreadPooledClientTester
	$(CC)  StreamingPollingClientTester.o $(CFLAGS) $(LFLAGS)  -o StreamingPollingClientTester
	$(CC)  StreamingThreadedClientTester.o $(CFLAGS) $(LFLAGS)  -o StreamingThreadedClientTester
%.o : %.cpp %.h
	$(CC) -o $@ -c $<  $(CXXFLAGS)
clean:
	rm $(OBJS) $(EXES)  -rf

