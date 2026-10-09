# make file for easy compilation
# used provided example in IPC.zip for reference

CXX = g++
CXXFLAGS = -g -Wall -std=c++17
LIBRARIES = -lrt -lpthread
EXECUTABLES = client dispatcher worker logger

# make executables out of all
all: $(EXECUTABLES)

# build each executable 
client: client.cpp struct.h
	$(CXX) $(CXXFLAGS) -o client client.cpp $(LIBRARIES)

dispatcher: dispatcher.cpp struct.h
	$(CXX) $(CXXFLAGS) -o dispatcher dispatcher.cpp $(LIBRARIES)

worker: worker.cpp struct.h
	$(CXX) $(CXXFLAGS) -o worker worker.cpp $(LIBRARIES)

logger: logger.cpp struct.h
	$(CXX) $(CXXFLAGS) -o logger logger.cpp $(LIBRARIES)
