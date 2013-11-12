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

extern "C"
{
  #include "lmdb.h"
}

class TestData {
public:
  int Id;
  int ValueSize;
};

class PerformanceRecord {
public:
  long ProcessedItems;
  long Duration;
  time_t Time; 
};

double ToMiliseconds(int endTime, int startTime) {
  return (endTime - startTime)/(CLOCKS_PER_SEC/1000);
};

double ToSeconds(int endTime, int startTime) {
  return ToMiliseconds(endTime, startTime) / (double)1000;
};

std::vector<TestData> InitValue(std::set<int> ids, int minValueSize, int maxValueSize) {
  std::vector<TestData> data;
  
  std::set<int>::iterator it;
  for(it = ids.begin(); it != ids.end(); ++it) {
    
    TestData t;
    t.Id = *it;
    t.ValueSize = rand() % (maxValueSize + 1 - minValueSize) + minValueSize;
    
    data.push_back(t);
  }
  
  return data;
};

std::vector<TestData> InitSequentialNumbers(int count, int minValueSize, int maxValueSize) {
    std::set<int> ids;
  
    for(int i = 0; i < count; i++) {
      ids.insert(i);
    }
    
    return InitValue(ids, minValueSize, maxValueSize);
};

std::vector<TestData> InitRandomNumbers(int count, int minValueSize, int maxValueSize) {
  std::vector<TestData> data = InitSequentialNumbers(count, minValueSize, maxValueSize);
  
  std::random_shuffle(data.begin(), data.end());
  
  return data;
};

std::string GetTime(time_t t) {
  struct tm tstruct;
  char buf[80];
  tstruct = *localtime(&t);
  
  strftime(buf, sizeof(buf), "%Y-%m-%d.%X", &tstruct);
  
  std::stringstream s;
  std::string output;
  s << buf;
  s >> output;
  
  return output;
};

void WritePerfData(std::string name, std::vector<PerformanceRecord> data) {
  std::ofstream file;
  std::string fileName = name + "_LMDB.csv";
  file.open((char*)fileName.c_str());
  
  for(int i = 0; i < data.size(); i++) {
    PerformanceRecord r = data.at(i);

    file << r.ProcessedItems << "," << GetTime(r.Time) << "," << r.Duration << "\n";
  }
  
  file.close();
};

std::vector<PerformanceRecord> Write(std::vector<TestData> dataItems, int itemsPerTransaction, int numberOfTransactions) {
  std::vector<PerformanceRecord> records;
  
  int rc;
  MDB_env *env;
  MDB_val key, data;

  MDB_stat mst;
  MDB_cursor *cursor;
  char sval[87 * 1024]; //TODO

  system("exec rm -r ./lmdb_test");
  system("exec mkdir ./lmdb_test");
  
  rc = mdb_env_create(&env);
  rc = mdb_env_set_mapsize(env, 1024*1024*1024*1);
  rc = mdb_env_set_flags(env, MDB_WRITEMAP, 1);
  rc = mdb_env_open(env, "./lmdb_test", 0, 0664);

  
  int startTime, endTime;
  time_t t;
  
  startTime = clock();
  
  for(int transactions = 0; transactions < numberOfTransactions; transactions++) {
    int sw = clock();
    
    MDB_txn *txn;
    MDB_dbi dbi;

    rc = mdb_txn_begin(env, NULL, 0, &txn);
    rc = mdb_open(txn, NULL, 0, &dbi);
    
    for(int i = 0; i < itemsPerTransaction; i++) {
      TestData item = dataItems.at(transactions * itemsPerTransaction + i);
      
      key.mv_size = sizeof(int);
      key.mv_data = &item.Id;
  
      data.mv_size = item.ValueSize;
      data.mv_data = sval;
       
      rc = mdb_put(txn, dbi, &key, &data, 0);
    }
    
    rc = mdb_txn_commit(txn);
    mdb_close(env, dbi);
    
    time(&t);
    
    PerformanceRecord r;
    r.Duration = ToMiliseconds(clock(), sw);
    r.ProcessedItems = itemsPerTransaction;
    r.Time = t;
    
    records.push_back(r);
  }
  
  endTime = clock();
  
  double secs = ToSeconds(endTime, startTime);
  
  mdb_env_close(env);
  
  std::cout << "Wrote " << numberOfTransactions * itemsPerTransaction << " items in " << secs << " sec, " << (numberOfTransactions * itemsPerTransaction)/secs << " ops/s" << std::endl;
  
  return records;
};

std::vector<PerformanceRecord> Read(std::vector<TestData> dataItems, int itemsPerTransaction, int numberOfTransactions) {
  std::vector<PerformanceRecord> records;
  
  int rc;
  MDB_env *env;
  MDB_val key, data;

  MDB_stat mst;
  MDB_cursor *cursor;
  char sval[87 * 1024]; //TODO

  rc = mdb_env_create(&env);
  rc = mdb_env_set_mapsize(env, 1024*1024*1024*1);
  rc = mdb_env_set_flags(env, MDB_WRITEMAP, 1);
  rc = mdb_env_open(env, "./lmdb_test", 0, 0664);

  
  int startTime, endTime;
  time_t t;
  
  startTime = clock();
  
  MDB_txn *txn;
  MDB_dbi dbi;

  rc = mdb_txn_begin(env, NULL, 0, &txn);
  rc = mdb_open(txn, NULL, 0, &dbi);
    
    for(int i = 0; i < dataItems.size(); i++) {
      TestData item = dataItems.at(i);
      
      key.mv_size = sizeof(int);
      key.mv_data = &item.Id;
      
      mdb_get(txn, dbi, &key, &data);
    }
    
  rc = mdb_txn_commit(txn);
  mdb_close(env, dbi);
  
  time(&t);
    
  endTime = clock();
  
  PerformanceRecord r;
  r.Duration = ToMiliseconds(endTime, startTime);
  r.ProcessedItems = numberOfTransactions * itemsPerTransaction;
  r.Time = t;
  
  records.push_back(r);
  
  double secs = ToSeconds(endTime, startTime);
  
  mdb_env_close(env);
  
  std::cout << "Read " << numberOfTransactions * itemsPerTransaction << " items in " << secs << " sec, " << (numberOfTransactions * itemsPerTransaction)/secs << " ops/s" << std::endl;
  
  return records;
};

int main(int argc,char * argv[])
{
  
  srand(time(NULL));

  int writeTransactions = 10 * 1000;
  int itemsPerTransaction = 100;
  int readItems = writeTransactions * itemsPerTransaction;

  std::vector<TestData> sequentialIds = InitSequentialNumbers(readItems, 128, 128);
  std::vector<TestData> randomIds = InitRandomNumbers(readItems, 128, 128);

  std::vector<TestData> sequentialIdsLarge = InitSequentialNumbers(readItems, 512, 87 * 1024);
  std::vector<TestData> randomIdsLarge = InitRandomNumbers(readItems, 512, 87 * 1024);

  std::vector<PerformanceRecord> records = Write(sequentialIds, itemsPerTransaction, writeTransactions);
  WritePerfData("WriteSeq", records);
  records = Read(sequentialIds, itemsPerTransaction, writeTransactions);
  WritePerfData("ReadSeq", records);

  records = Write(sequentialIdsLarge, itemsPerTransaction, writeTransactions);
  WritePerfData("WriteLargeSeq", records);
  records = Read(sequentialIdsLarge, itemsPerTransaction, writeTransactions);
  WritePerfData("ReadLargeSeq", records);

  records = Write(randomIdsLarge, itemsPerTransaction, writeTransactions);
  WritePerfData("WriteLargeRandom", records);;
  records = Read(randomIdsLarge, itemsPerTransaction, writeTransactions);
  WritePerfData("ReadLargeRandom", records);

  return 0;
}