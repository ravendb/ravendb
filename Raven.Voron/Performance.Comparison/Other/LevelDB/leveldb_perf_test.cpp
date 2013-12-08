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
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/write_batch.h"

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

vector<PerformanceRecord> WriteInternal(vector<TestData>::iterator begin, int itemsPerTransaction, int numberOfTransactions, leveldb::DB* db) {
  vector<PerformanceRecord> records;
  
  leveldb::WriteOptions wo;
  wo.sync = true;
  
  for(int transactions = 0; transactions < numberOfTransactions; transactions++) {
    time_point<system_clock> sw = system_clock::now();
    leveldb::WriteBatch wb;
    
    for(int i = 0; i < itemsPerTransaction; i++) {
      
      TestData *item = &*begin;
      
      stringstream sKey;
      sKey << setw(16) << setfill('0') << item->Id;
      string key = sKey.str();
      
      stringstream sValue;
      sValue << setw(item->ValueSize) << setfill('1') << 1;
      string value = sValue.str();
      
      wb.Put(key, value);
      
      *begin++;
    }
    
    db->Write(wo, &wb);
    
    time_point<system_clock> now = system_clock::now();
    duration<double> timeInSeconds = now - sw;
    
    PerformanceRecord r;
    r.Duration = timeInSeconds.count() * 1000; // in milliseconds
    r.ProcessedItems = itemsPerTransaction;
    r.Time = chrono::system_clock::to_time_t(now);
    
    records.push_back(r);
  }
  
  return records;
}

vector<PerformanceRecord> Write(vector<TestData> data, int itemsPerTransaction, int numberOfTransactions) {

  leveldb::Options o;
  o.error_if_exists = false;
  o.create_if_missing = true;
  
  leveldb::DestroyDB("./testDB", o);
  
  leveldb::DB* db;
  leveldb::Status s = leveldb::DB::Open(o, "./testDB", &db);
  
  time_point<system_clock> start, end;
  start = system_clock::now();
  
  vector<PerformanceRecord> records = WriteInternal(data.begin(), itemsPerTransaction, numberOfTransactions, db);
  
  end = system_clock::now();
  
  duration<double> elapsed_seconds = end - start;
  
  double secs = elapsed_seconds.count();
  
  db->~DB();
  
  cout << "Write " << numberOfTransactions * itemsPerTransaction << " items in " << secs << " sec, " << (long)((numberOfTransactions * itemsPerTransaction)/secs) << " ops/s" << endl;
  
  return records;
};

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

vector<PerformanceRecord> WriteParallel(vector<TestData> data, int itemsPerTransaction, int numberOfTransactions, int numberOfThreads) {
  vector<PerformanceRecord> records;
  
  leveldb::Options o;
  o.error_if_exists = false;
  o.create_if_missing = true;
  
  leveldb::DestroyDB("./testDB", o);
  
  leveldb::DB* db;
  leveldb::Status s = leveldb::DB::Open(o, "./testDB", &db);
  
  vector<ParallelTestData> testData = SplitData(data, itemsPerTransaction, numberOfTransactions, numberOfThreads);
  
  time_point<system_clock> start, end;
  start = system_clock::now();
  
  vector<future<vector<PerformanceRecord>>> results(numberOfThreads);
  
  for(int i = 0; i < numberOfThreads; i++) {
    ParallelTestData d = testData.at(i);
    
    vector<TestData>::iterator it = data.begin();
    advance(it, d.SkipCount);
    
    results.at(i) = async(&WriteInternal, it, d.ItemsPerTransaction, d.NumberOfTransactions, db);
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
  
  db->~DB();
  
  cout << "Write Parallel [" << numberOfThreads << "] " << numberOfTransactions * itemsPerTransaction << " items in " << secs << " sec, " << (long)((numberOfTransactions * itemsPerTransaction)/secs) << " ops/s" << endl;
  
  return records;
};

void ReadInternal(vector<TestData> data, int itemsPerTransaction, int numberOfTransactions, leveldb::DB* db) {
  leveldb::ReadOptions ro;
  
  for(int transactions = 0; transactions < numberOfTransactions; transactions++) {
    for(int i = 0; i < itemsPerTransaction; i++) {
      TestData item = data.at(transactions * itemsPerTransaction + i);
      
      stringstream sKey;
      sKey << setw(16) << setfill('0') << item.Id;
      string key = sKey.str();
      
      string result;
      db->Get(ro, key, &result);
    }
  }
}

vector<PerformanceRecord> Read(vector<TestData> data, int itemsPerTransaction, int numberOfTransactions) {
  vector<PerformanceRecord> records;
  
  leveldb::Options o;
  o.error_if_exists = false;
  o.create_if_missing = false;
  
  leveldb::DB* db;
  leveldb::Status s = leveldb::DB::Open(o, "./testDB", &db);
  
  time_point<system_clock> start, end;
  start = system_clock::now();
  
  ReadInternal(data, itemsPerTransaction, numberOfTransactions, db);
  
  end = system_clock::now();
  
  duration<double> elapsed_seconds = end - start;
  
  double secs = elapsed_seconds.count();
  
  PerformanceRecord r;
  r.Duration = elapsed_seconds.count() * 1000; // in milliseconds
  r.ProcessedItems = numberOfTransactions * itemsPerTransaction;
  r.Time = chrono::system_clock::to_time_t(end);
  
  records.push_back(r);
  
  db->~DB();
  
  cout << "Read " << numberOfTransactions * itemsPerTransaction << " items in " << secs << " sec, " << (long)((numberOfTransactions * itemsPerTransaction)/secs) << " ops/s" << endl;
  
  return records;
};

vector<PerformanceRecord> ReadParallel(vector<TestData> data, int itemsPerTransaction, int numberOfTransactions, int numberOfThreads) {
  vector<PerformanceRecord> records;
  
  leveldb::Options o;
  o.error_if_exists = false;
  o.create_if_missing = false;
  
  leveldb::DB* db;
  leveldb::Status s = leveldb::DB::Open(o, "./testDB", &db);
  
  time_point<system_clock> start, end;
  start = system_clock::now();
  
  vector<thread> threads(numberOfThreads);
  
  for(int i = 0; i < numberOfThreads; i++) {
    threads.at(i) = thread(ReadInternal, data, itemsPerTransaction, numberOfTransactions, db);
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
  
  db->~DB();
  
  cout << "Read Parallel [" << numberOfThreads << "] " << numberOfTransactions * itemsPerTransaction * numberOfThreads << " items in " << secs << " sec, " << (long)((numberOfTransactions * itemsPerTransaction * numberOfThreads)/secs) << " ops/s" << endl;
  
  return records;
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
  string fileName = name + "_LEVELDB.csv";
  
  file.open((char*)fileName.c_str());
  
  file << "Items,Time,Duration\n";
  
  for(int i = 0; i < data.size(); i++) {
    PerformanceRecord r = data.at(i);

    file << r.ProcessedItems << "," << GetTime(r.Time) << "," << r.Duration << "\n";
  }
  
  file.close();
};

int main(int argc, char** argv) {

  srand(time(NULL));
  
  int writeTransactions = 10 * 100;
  int itemsPerTransaction = 100;
  int readItems = writeTransactions * itemsPerTransaction;
  
  vector<TestData> sequentialIds = InitSequentialNumbers(readItems, 128, 128);
  vector<TestData> randomIds = InitRandomNumbers(readItems, 128, 128);
  
  vector<TestData> sequentialIdsLarge = InitSequentialNumbers(readItems, 512, 87 * 1024);
  vector<TestData> randomIdsLarge = InitRandomNumbers(readItems, 512, 87 * 1024);
  
  vector<PerformanceRecord> records = Write(sequentialIds, itemsPerTransaction, writeTransactions);
  WritePerfData("WriteSeq", records);
  
  records = WriteParallel(sequentialIds, itemsPerTransaction, writeTransactions, 2);
  WritePerfData("WriteSeq_Parallel_2", records);
  
  records = WriteParallel(sequentialIds, itemsPerTransaction, writeTransactions, 4);
  WritePerfData("WriteSeq_Parallel_4", records);
  
  records = WriteParallel(sequentialIds, itemsPerTransaction, writeTransactions, 8);
  WritePerfData("WriteSeq_Parallel_8", records);
  
  records = WriteParallel(sequentialIds, itemsPerTransaction, writeTransactions, 16);
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
