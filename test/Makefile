CFLAGS = -g -Wall -std=c++11 -fPIC -DLINUX -DLOGGING_LEVEL_2 -DLINUX -O0 
INCLUDES= -I../include
OBJS=test.o

all: $(OBJS)
	g++ $(OBJS) $(CFLAGS)  -lDolphinDBAPI -lssl  -lpthread -luuid -L../bin  -Wl,-rpath ../bin/ -o test 
test.o: test.cpp
	g++ $(CFLAGS) $(INCLUDES) -c test.cpp -o test.o
clean:
	rm *.o test  core -rf
