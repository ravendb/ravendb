#Notice: LevelDB source should be copiled under indicated path 
LEVELDB = $(HOME)/Downloads/ldb/leveldb-1.14.0
LEVELDB_INCLUDES = $(LEVELDB)/include
CXX	= g++
W	= -W -Wall -Wno-unused-parameter -Wno-unused-result -Wno-unused-but-set-variable -Wno-unused-variable -Wno-sign-compare -Wl,-rpath -Wl,$(LEVELDB)
THREADS = -pthread
OPT = -O2 -g
FLAGS	= $(THREADS) $(OPT) $(W) $(XCFLAGS)
CPPFLAGS	= $(THREADS) $(OPT) $(WCPP) $(XCFLAGS)
LIBS	= -L$(LEVELDB) -lleveldb -I$(LEVELDB_INCLUDES)

########################################################################


all:
	$(CXX) $(FLAGS) leveldb_perf_test.cpp  $(LIBS) -o leveldb_perf_test -std=c++11 -std=gnu++11
	
clean:
	rm -rf *.[ao] *.so *.csv *~ leveldb_perf_test leveldb_test