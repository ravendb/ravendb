#Notice: LMDB source should be copiled under indicated path 
LMDB = $(HOME)/lmdb/mdb-mdb/libraries/liblmdb
CXX	= g++
W	= -W -Wall -Wno-unused-parameter -Wno-unused-result -Wno-unused-but-set-variable -Wno-unused-variable -Wno-sign-compare -Wl,-rpath -Wl,$(LMDB)
THREADS = -pthread
OPT = -O2 -g
FLAGS	= $(THREADS) $(OPT) $(W) $(XCFLAGS)
CPPFLAGS	= $(THREADS) $(OPT) $(WCPP) $(XCFLAGS)
LIBS	= -L$(LMDB) -llmdb -I$(LMDB)
prefix	= /usr/local

########################################################################


all:
	$(CXX) $(FLAGS) lmdb_perf_test.cpp  $(LIBS) -o lmdb_perf_test
	
clean:
	rm -rf *.[ao] *.so *.csv *~ lmdb_perf_test lmdb_test