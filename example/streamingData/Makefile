CFLAGS = -g -Wall -std=c++11 -fPIC -DLINUX -DLOGGING_LEVEL_2 -DLINUX -O0 
INCLUDES= -I../../include
SRC_DIR := .
BUILD_DIR := .
OBJ_DIR := $(BUILD_DIR)
SRCS := $(wildcard $(SRC_DIR)/*.cpp)
OBJS := $(SRCS:$(SRC_DIR)/%.cpp=$(OBJ_DIR)/%.o)

all: $(OBJS)
	g++ -L../../bin/linux_x64 -L../../bin/mingw_x64 -L../../bin/vx2017_x64 StreamingThreadedClientSubscriber.o $(CFLAGS)  -lDolphinDBAPI -lssl  -lpthread -luuid -L../../bin  -Wl,-rpath,.:../../bin/linux_x64/ -o StreamingThreadedClientSubscriber 
	g++ -L../../bin/linux_x64 -L../../bin/mingw_x64 -L../../bin/vx2017_x64 StreamingDataWriting.o $(CFLAGS)  -lDolphinDBAPI -lssl  -lpthread -luuid -L../../bin  -Wl,-rpath,.:../../bin/linux_x64/ -o StreamingDataWriting
StreamingThreadedClientSubscriber.o: StreamingThreadedClientSubscriber.cpp
	g++ $(CFLAGS) $(INCLUDES) -c StreamingThreadedClientSubscriber.cpp -o StreamingThreadedClientSubscriber.o
	
StreamingDataWriting.o: StreamingDataWriting.cpp
	g++ $(CFLAGS) $(INCLUDES) -c StreamingDataWriting.cpp -o StreamingDataWriting.o
	
clean:
	rm *.o StreamingThreadedClientSubscriber  core -rf
	rm *.o StreamingDataWriting  core -rf

