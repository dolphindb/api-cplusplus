TARGET_LIB ?= libDolphinDBAPI.so
LD_FLAGS = -shared -fPIC -Wl,-rpath,.

BUILD_DIR ?= ./build
SRC_DIRS_LIB ?= ./src


SRCS := $(shell find $(SRC_DIRS_LIB) -name *.cpp -or -name *.c -or -name *.s)

OBJS := $(SRCS:%=$(BUILD_DIR)/%.o)
DEPS := $(OBJS:.o=.d)

INC_DIRS := $(shell find $(SRC_DIRS_LIB) -type d)
INC_FLAGS := $(addprefix -I,$(INC_DIRS))
INC_FLAGS += -I src/python3.7m


CPPFLAGS ?= $(INC_FLAGS) -fPIC -DLINUX -DNDEBUG -DLOGGING_LEVEL_2 -O3 -Wall -c -fmessage-length=0 -std=gnu++11 -msse -msse2 -msse3 -funroll-loops -MMD -MP -MF

$(TARGET_LIB):$(OBJS)
	g++ $(LD_FLAGS) $(OBJS) -o $(TARGET_LIB) -Wl,--whole-archive libuuid.a -Wl,--no-whole-archive


# assembly
$(BUILD_DIR)/%.s.o: %.s
	$(MKDIR_P) $(dir $@)
	$(AS) $(ASFLAGS) -c $< -o $@

# c source
$(BUILD_DIR)/%.c.o: %.c
	$(MKDIR_P) $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

# c++ source
$(BUILD_DIR)/%.cpp.o: %.cpp
	$(MKDIR_P) $(dir $@)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $< -o $@


.PHONY: clean

clean:
	$(RM) -rf $(BUILD_DIR)
	$(RM) -rf libDolphinDBAPI.so

-include $(DEPS)

MKDIR_P ?= mkdir -p
