INCLUDES=-I includes/
CXXFLAGS=-std=c++2a -Wall -Wextra  -pedantic -pthread $(INCLUDES) -g
CXX=g++

.DEFAULT_GOAL := exec
.PHONY: clean # tests

# exec: CXXFLAGS += -O3
exec: build/exec

# debug: CXXFLAGS += -g
# debug: bin/debug

# tests: CXXFLAGS += -g
# tests: bin/tests

INCFILES=includes/socket.hpp includes/constants.hpp includes/message.hpp includes/node.hpp includes/socket.hpp includes/logging.hpp

clean:
	rm -f build/*

build/logging.o: src/logging.cpp $(INCFILES)
	mkdir -p build
	$(CXX) $(CXXFLAGS) -c src/logging.cpp -o $@

build/main.o: src/main.cpp $(INCFILES)
	mkdir -p build
	$(CXX) $(CXXFLAGS) -c src/main.cpp -o $@

build/message.o: src/message.cpp $(INCFILES)
	mkdir -p build
	$(CXX) $(CXXFLAGS) -c src/message.cpp -o $@

build/node.o: src/node.cpp  $(INCFILES)
	mkdir -p build
	$(CXX) $(CXXFLAGS) -c src/node.cpp -o $@

build/socket.o: src/socket.cpp $(INCFILES)
	mkdir -p build
	$(CXX) $(CXXFLAGS) -c src/socket.cpp -o $@

build/exec: build/main.o build/message.o build/node.o build/socket.o build/logging.o
	$(CXX) $(CXXFLAGS) $^ -o $@

# bin/tests: tests/unittests.cpp src/client_utils.cpp src/cmdRunner.cpp src/client_socket.cpp src/server_socket.cpp src/socket_utils.cpp
# 	$(CXX) $(CXXFLAGS) $^ -o $@
