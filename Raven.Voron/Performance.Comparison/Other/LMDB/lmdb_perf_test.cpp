#include <sstream>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <time.h>
#include <stdio.h>
#include <algorithm>
#include <vector>
#include <set>
#include <unistd.h>
#include <chrono>
#include <ctime>
#include <thread>
#include <future>
#include <iterator>

extern "C"
{
  #include "lmdb.h"
}

using namespace std;
using namespace chrono;

class TestData {
public:
  int Id;
  int ValueSize;
};

class ParallelTestData {
public:
  int SkipCount;
  int NumberOfTransactions;
  int ItemsPerTransaction;
};

class PerformanceRecord {
public:
  long ProcessedItems;
  long Duration;
  time_t Time; 
};

#define RND 0
#define SEQ 1

vector<ParallelTestData> SplitData(vector<TestData> data, int currentItemsPerTransaction, int currentNumberOfTransactions, int numberOfThreads) {

  vector<ParallelTestData> results;
  
  int numberOfTransactionsPerThread = currentNumberOfTransactions / numberOfThreads;
  
  for(int i = 0; i < numberOfThreads; i++) {
     int actualNumberOfTransactionsPerThread = 0;
    
     if(i < numberOfThreads - 1) {
      actualNumberOfTransactionsPerThread = numberOfTransactionsPerThread;
     } else {
      actualNumberOfTransactionsPerThread = currentNumberOfTransactions - (i * numberOfTransactionsPerThread);
     }
    
      ParallelTestData item;
      item.SkipCount = (i * currentItemsPerTransaction * numberOfTransactionsPerThread);
      item.ItemsPerTransaction = currentItemsPerTransaction;
      item.NumberOfTransactions = actualNumberOfTransactionsPerThread;
      
      results.push_back(item);
  }
  
  return results;
}

vector<TestData> InitValue(set<int> ids, int minValueSize, int maxValueSize) {
  vector<TestData> data;
  
  set<int>::iterator it;
  for(it = ids.begin(); it != ids.end(); ++it) {
    
    TestData t;
    t.Id = *it;
    t.ValueSize = rand() % (maxValueSize + 1 - minValueSize) + minValueSize;
    
    data.push_back(t);
  }
  
  return data;
};

vector<TestData> InitSequentialNumbers(int count, int minValueSize, int maxValueSize) {
    set<int> ids;
  
    for(int i = 0; i < count; i++) {
      ids.insert(i);
    }
    
    return InitValue(ids, minValueSize, maxValueSize);
};

vector<TestData> InitRandomNumbers(int count, int minValueSize, int maxValueSize) {
  vector<TestData> data = InitSequentialNumbers(count, minValueSize, maxValueSize);
  
  random_shuffle(data.begin(), data.end());
  
  return data;
};

string GetTime(time_t t) {
  struct tm tstruct;
  char buf[80];
  tstruct = *localtime(&t);
  
  strftime(buf, sizeof(buf), "%Y-%m-%d.%X", &tstruct);
  
  stringstream s;
  string output;
  s << buf;
  s >> output;
  
  return output;
};

void WritePerfData(string name, vector<PerformanceRecord> data) {
  ofstream file;
  string fileName = name + "_LMDB.csv";
  
  file.open((char*)fileName.c_str());
  
  file << "Items,Time,Duration\n";
  
  for(int i = 0; i < data.size(); i++) {
    PerformanceRecord r = data.at(i);

    file << r.ProcessedItems << "," << GetTime(r.Time) << "," << r.Duration << "\n";
  }
  
  file.close();
};

MDB_env* StorageEnvironment(bool deleteOldData, MDB_dbi *pdbi){
 
  if(deleteOldData)
  {
    system("exec rm -r ./lmdb_test");
    system("exec mkdir ./lmdb_test");
  }
  
  MDB_env *env;
  
  mdb_env_create(&env);
  mdb_env_set_mapsize(env, 1024*1024*1024*(long)10);
  mdb_env_open(env, "./lmdb_test", MDB_WRITEMAP, 0664);

  MDB_txn *txn;
  mdb_txn_begin(env, NULL, 0, &txn);
  mdb_open(txn, NULL, MDB_INTEGERKEY, pdbi);
  mdb_txn_commit(txn);
  
  return env;
}

vector<PerformanceRecord> WriteInternal(vector<TestData>::iterator begin, int itemsPerTransaction, int numberOfTransactions, int seqrand, MDB_env* env, MDB_dbi dbi) {
  vector<PerformanceRecord> records;
  
  int rc, flag = 0;

  MDB_val key, data;

  MDB_stat mst;
  MDB_cursor *cursor;
  char sval[87 * 1024];
  
  if (seqrand == SEQ)
  	flag = MDB_APPEND;

  for(int transactions = 0; transactions < numberOfTransactions; transactions++) {
    time_point<system_clock> sw = system_clock::now();
    
    MDB_txn *txn;

    rc = mdb_txn_begin(env, NULL, 0, &txn);
	rc = mdb_cursor_open(txn, dbi, &cursor);
    
    for(int i = 0; i < itemsPerTransaction; i++) {
      TestData *item = &*begin;
      
      key.mv_size = sizeof(int);
      key.mv_data = &item->Id;
  
      data.mv_size = item->ValueSize;
      data.mv_data = sval;
       
      rc = mdb_cursor_put(cursor, &key, &data, flag);
      
      if(rc != 0){
	cout << "Unable to PUT: " << rc << endl;
	throw exception();
      }
      
      *begin++;
    }
    
    rc = mdb_txn_commit(txn);
    
    PerformanceRecord r;
    
    time_point<system_clock> now = system_clock::now();
    
    duration<double> timeInSeconds  = now - sw;
    
    r.Duration = timeInSeconds.count() * 1000; // in miliseconds
    r.ProcessedItems = itemsPerTransaction;
    r.Time = chrono::system_clock::to_time_t(now);
    
    records.push_back(r);
  }
  
  return records;
}

vector<PerformanceRecord> Write(vector<TestData> dataItems, int itemsPerTransaction, int numberOfTransactions, int seqrand) {

  MDB_dbi dbi;
  MDB_env *env = StorageEnvironment(true, &dbi);
  
  time_point<system_clock> start, end;
  start = system_clock::now();
  
  vector<PerformanceRecord> records = WriteInternal(dataItems.begin(),itemsPerTransaction,numberOfTransactions, seqrand, env, dbi);
  
  end = system_clock::now();
  
  duration<double> elapsed_seconds = end - start;
  
  double secs = elapsed_seconds.count();
  
  mdb_env_close(env);
  
  cout << "Wrote " << numberOfTransactions * itemsPerTransaction << " items in " << secs << " sec, " << (long)((numberOfTransactions * itemsPerTransaction)/secs) << " ops/s" << endl;
  

  return records;
};

vector<PerformanceRecord> WriteParallel(vector<TestData> data, int itemsPerTransaction, int numberOfTransactions, int seqrand, int numberOfThreads) {
  vector<PerformanceRecord> records;
  
  MDB_dbi dbi;
  MDB_env *env = StorageEnvironment(true, &dbi);
  
  vector<ParallelTestData> testData = SplitData(data, itemsPerTransaction, numberOfTransactions, numberOfThreads);
  
  time_point<system_clock> start, end;
  start = system_clock::now();
  
  vector<future<vector<PerformanceRecord>>> results(numberOfThreads);
  
  for(int i = 0; i < numberOfThreads; i++) {
    ParallelTestData d = testData.at(i);
    
    vector<TestData>::iterator it = data.begin();
    advance(it, d.SkipCount);
    
    results.at(i) = async(&WriteInternal, it, d.ItemsPerTransaction, d.NumberOfTransactions, seqrand, env, dbi);
  }
  
  for(int i = 0; i < numberOfThreads; i++) {
    vector<PerformanceRecord> r = results.at(i).get();
    for(int j = 0; j < r.size(); j++) {
      records.push_back(r.at(j));
    }
  }
  
  end = system_clock::now();
  
  duration<double> elapsed_seconds = end - start;
  
  double secs = elapsed_seconds.count();
  
  cout << "Write Parallel [" << numberOfThreads << "] " << numberOfTransactions * itemsPerTransaction << " items in " << secs << " sec, " << (long)((numberOfTransactions * itemsPerTransaction)/secs) << " ops/s" << endl;
  
  return records;
};

void ReadInternal(vector<TestData> testData, int itemsPerTransaction, int numberOfTransactions, MDB_env* env, MDB_dbi dbi) {
  int rc;
  MDB_val key, data;
  MDB_txn *txn;

  rc = mdb_txn_begin(env, NULL, MDB_RDONLY, &txn);
    
  for(int i = 0; i < testData.size(); i++) {
    TestData item = testData.at(i);
    
    key.mv_size = sizeof(int);
    key.mv_data = &item.Id;
    
    rc = mdb_get(txn, dbi, &key, &data);
    
    if(rc != 0)
    {
      cout << "GET exception: " << rc << endl;
      
      throw exception();
    }
  }
    
  rc = mdb_txn_commit(txn);
}

vector<PerformanceRecord> Read(vector<TestData> testData, int itemsPerTransaction, int numberOfTransactions) {
  vector<PerformanceRecord> records;
  
  int rc;
  MDB_val key, data;

  MDB_stat mst;
  MDB_cursor *cursor;
  char sval[87 * 1024];

  MDB_dbi dbi;
  MDB_env *env = StorageEnvironment(false, &dbi);

  time_point<system_clock> start, end;
  start = system_clock::now();
  
  ReadInternal(testData, itemsPerTransaction, numberOfTransactions, env, dbi);
  
  end = system_clock::now();
  
  duration<double> elapsed_seconds = end - start;
  
  double secs = elapsed_seconds.count();
  
  PerformanceRecord r;
  r.Duration = secs * 1000; // in miliseconds
  r.ProcessedItems = testData.size();
  r.Time = chrono::system_clock::to_time_t(end);
  
  records.push_back(r);
  
  mdb_env_close(env);
  
  cout << "Read " << testData.size() << " items in " << secs << " sec, " << (long)(testData.size()/secs) << " ops/s" << endl;
  
  return records;
};

vector<PerformanceRecord> ReadParallel(vector<TestData> testData, int itemsPerTransaction, int numberOfTransactions, int numberOfThreads) {
  vector<PerformanceRecord> records;

  MDB_stat mst;
  MDB_cursor *cursor;
  char sval[87 * 1024];

  MDB_dbi dbi;
  MDB_env *env = StorageEnvironment(false, &dbi);
  
  time_point<system_clock> start, end;
  start = system_clock::now();
  
  vector<thread> threads(numberOfThreads);
  
  for(int i = 0; i < numberOfThreads; i++) {
    threads.at(i) = thread(ReadInternal, testData, itemsPerTransaction, numberOfTransactions, env, dbi);
  }
  
  for(int i = 0; i < numberOfThreads; i++) {
    threads.at(i).join();
  }
  
  end = system_clock::now();
  
  duration<double> elapsed_seconds = end - start;
  
  double secs = elapsed_seconds.count();
  
  PerformanceRecord r;
  r.Duration = elapsed_seconds.count() * 1000; // in milliseconds
  r.ProcessedItems = numberOfTransactions * itemsPerTransaction * numberOfThreads;
  r.Time = chrono::system_clock::to_time_t(end);
  
  records.push_back(r);
  
  cout << "Read Parallel [" << numberOfThreads << "] " << numberOfTransactions * itemsPerTransaction * numberOfThreads << " items in " << secs << " sec, " << (long)((numberOfTransactions * itemsPerTransaction * numberOfThreads)/secs) << " ops/s" << endl;
  
  return records;
};

int main(int argc,char * argv[])
{
  srand(time(NULL));
  
  int writeTransactions = 10 * 10;
  int itemsPerTransaction = 100;
  int readItems = writeTransactions * itemsPerTransaction;
  
  vector<TestData> sequentialIds = InitSequentialNumbers(readItems, 128, 128);
  vector<TestData> randomIds = InitRandomNumbers(readItems, 128, 128);
  
  vector<TestData> sequentialIdsLarge = InitSequentialNumbers(readItems, 512, 87 * 1024);
  vector<TestData> randomIdsLarge = InitRandomNumbers(readItems, 512, 87 * 1024);
  
  vector<PerformanceRecord> records = Write(sequentialIds, itemsPerTransaction, writeTransactions, SEQ);
  WritePerfData("WriteSeq", records);
  
  records = WriteParallel(sequentialIds, itemsPerTransaction, writeTransactions, SEQ, 2);
  WritePerfData("WriteSeq_Parallel_2", records);
  
  records = WriteParallel(sequentialIds, itemsPerTransaction, writeTransactions, SEQ, 4);
  WritePerfData("WriteSeq_Parallel_4", records);
  
  records = WriteParallel(sequentialIds, itemsPerTransaction, writeTransactions, SEQ, 8);
  WritePerfData("WriteSeq_Parallel_8", records);
  
  records = WriteParallel(sequentialIds, itemsPerTransaction, writeTransactions, SEQ, 16);
  WritePerfData("WriteSeq_Parallel_16", records);
  
  records = Read(sequentialIds, itemsPerTransaction, writeTransactions);
  WritePerfData("ReadSeq", records);
  
  records = ReadParallel(sequentialIds, itemsPerTransaction, writeTransactions, 2);
  WritePerfData("ReadSeq_Parallel_2", records);
  
  records = ReadParallel(sequentialIds, itemsPerTransaction, writeTransactions, 4);
  WritePerfData("ReadSeq_Parallel_4", records);
  
  records = ReadParallel(sequentialIds, itemsPerTransaction, writeTransactions, 8);
  WritePerfData("ReadSeq_Parallel_8", records);
  
  records = ReadParallel(sequentialIds, itemsPerTransaction, writeTransactions, 16);
  WritePerfData("ReadSeq_Parallel_16", records);
  
  records = Write(randomIds, itemsPerTransaction, writeTransactions, RND);
  WritePerfData("WriteRandom", records);
  records = Read(randomIds, itemsPerTransaction, writeTransactions);
  WritePerfData("ReadRandom", records);
  
  records = Write(sequentialIdsLarge, itemsPerTransaction, writeTransactions, SEQ);
  WritePerfData("WriteLargeSeq", records);
  records = Read(sequentialIdsLarge, itemsPerTransaction, writeTransactions);
  WritePerfData("ReadLargeSeq", records);
  
  records = Write(randomIdsLarge, itemsPerTransaction, writeTransactions, RND);
  WritePerfData("WriteLargeRandom", records);;
  records = Read(randomIdsLarge, itemsPerTransaction, writeTransactions);
  WritePerfData("ReadLargeRandom", records);
 
 return 0;
}
