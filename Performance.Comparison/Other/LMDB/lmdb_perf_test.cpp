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

extern "C"
{
  #include "lmdb.h"
}

using namespace std;

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
  
  for(int i = 0; i < data.size(); i++) {
    PerformanceRecord r = data.at(i);

    file << r.ProcessedItems << "," << GetTime(r.Time) << "," << r.Duration << "\n";
  }
  
  file.close();
};

vector<PerformanceRecord> Write(vector<TestData> dataItems, int itemsPerTransaction, int numberOfTransactions) {
  vector<PerformanceRecord> records;
  
  int rc;
  MDB_env *env;
  MDB_val key, data;

  MDB_stat mst;
  MDB_cursor *cursor;
  char sval[87 * 1024];

  system("exec rm -r ./lmdb_test");
  system("exec mkdir ./lmdb_test");
  
  rc = mdb_env_create(&env);
  rc = mdb_env_set_mapsize(env, 1024*1024*1024*(long)10);
  rc = mdb_env_open(env, "./lmdb_test", MDB_WRITEMAP, 0664);

  time_t t;
  
  chrono::time_point<chrono::system_clock> start, end;
  start = chrono::system_clock::now();
  
  for(int transactions = 0; transactions < numberOfTransactions; transactions++) {
    chrono::time_point<chrono::system_clock> sw = chrono::system_clock::now();
    
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
      
      if(rc != 0){
	cout << "Unable to PUT: " << rc << endl;
	throw exception();
      }
    }
    
    rc = mdb_txn_commit(txn);
    mdb_close(env, dbi);
    
    time(&t);
    
    PerformanceRecord r;
    
    chrono::duration<double> timeInSeconds  = chrono::system_clock::now() - sw;
    
    r.Duration = timeInSeconds.count() * 1000; // in miliseconds
    r.ProcessedItems = itemsPerTransaction;
    r.Time = t;
    
    records.push_back(r);
  }
  
  end = chrono::system_clock::now();
  
  chrono::duration<double> elapsed_seconds = end - start;
  
  double secs = elapsed_seconds.count();
  
  mdb_env_close(env);
  
  cout << "Wrote " << numberOfTransactions * itemsPerTransaction << " items in " << secs << " sec, " << (numberOfTransactions * itemsPerTransaction)/secs << " ops/s" << endl;
  

  return records;
};

vector<PerformanceRecord> Read(vector<TestData> dataItems, int itemsPerTransaction, int numberOfTransactions) {
  vector<PerformanceRecord> records;
  
  int rc;
  MDB_env *env;
  MDB_val key, data;

  MDB_stat mst;
  MDB_cursor *cursor;
  char sval[87 * 1024];

  rc = mdb_env_create(&env);
  rc = mdb_env_set_mapsize(env, 1024*1024*1024*(long)10);
  rc = mdb_env_open(env, "./lmdb_test", MDB_WRITEMAP, 0664);

  time_t t;
  
  chrono::time_point<chrono::system_clock> start, end;
  start = chrono::system_clock::now();
  
  MDB_txn *txn;
  MDB_dbi dbi;

  rc = mdb_txn_begin(env, NULL, 0, &txn);
  rc = mdb_open(txn, NULL, 0, &dbi);
    
  for(int i = 0; i < dataItems.size(); i++) {
    TestData item = dataItems.at(i);
    
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
  mdb_close(env, dbi);
  
  time(&t);
    
  end = chrono::system_clock::now();
  
  chrono::duration<double> elapsed_seconds = end - start;
  
  double secs = elapsed_seconds.count();
  
  PerformanceRecord r;
  r.Duration = secs * 1000; // in miliseconds
  r.ProcessedItems = numberOfTransactions * itemsPerTransaction;
  r.Time = t;
  
  records.push_back(r);
  
  mdb_env_close(env);
  
  cout << "Read " << numberOfTransactions * itemsPerTransaction << " items in " << secs << " sec, " << (numberOfTransactions * itemsPerTransaction)/secs << " ops/s" << endl;
  
  return records;
};

int main(int argc,char * argv[])
{
  
  srand(time(NULL));

  int writeTransactions = 1 * 1000;
  int itemsPerTransaction = 100;
  int readItems = writeTransactions * itemsPerTransaction;

  vector<TestData> sequentialIds = InitSequentialNumbers(readItems, 128, 128);
  vector<TestData> randomIds = InitRandomNumbers(readItems, 128, 128);

  vector<TestData> sequentialIdsLarge = InitSequentialNumbers(readItems, 512, 87 * 1024);
  vector<TestData> randomIdsLarge = InitRandomNumbers(readItems, 512, 87 * 1024);

  vector<PerformanceRecord> records = Write(sequentialIds, itemsPerTransaction, writeTransactions);
  WritePerfData("WriteSeq", records);
  records = Read(sequentialIds, itemsPerTransaction, writeTransactions);
  WritePerfData("ReadSeq", records);
  
  records = Write(randomIds, itemsPerTransaction, writeTransactions);
  WritePerfData("WriteRandom", records);
  records = Read(randomIds, itemsPerTransaction, writeTransactions);
  WritePerfData("ReadRandom", records);

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