#include <sstream>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <time.h>
#include <stdio.h>
#include <algorithm>
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/version_edit.h"
#include "db/write_batch_internal.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/options.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/write_batch.h"
#include "util/logging.h"
#include "util/crc32c.h"
#include "memtable.h"

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

std::vector<PerformanceRecord> Write(std::vector<TestData> data, int itemsPerTransaction, int numberOfTransactions) {
  std::vector<PerformanceRecord> records;
  
  leveldb::Options o;
  o.error_if_exists = false;
  o.create_if_missing = true;
  
  leveldb::DestroyDB("./testDB", o);
  
  leveldb::DB* db;
  leveldb::Status s = leveldb::DB::Open(o, "./testDB", &db);
  
  leveldb::WriteOptions wo;
  wo.sync = true;
  
  int startTime, endTime;
  time_t t;
  
  startTime = clock();
  
  for(int transactions = 0; transactions < numberOfTransactions; transactions++) {
    int sw = clock();
    leveldb::WriteBatch wb;
    
    for(int i = 0; i < itemsPerTransaction; i++) {
      TestData item = data.at(transactions * itemsPerTransaction + i);
      
      std::stringstream sKey;
      sKey << std::setw(16) << std::setfill('0') << item.Id;
      std::string key = sKey.str();
      
      std::stringstream sValue;
      sValue << std::setw(item.ValueSize) << std::setfill('1') << 1;
      std::string value = sValue.str();
      
      wb.Put(key, value);
    }
    
    db->Write(wo, &wb);
    
    time(&t);
    
    PerformanceRecord r;
    r.Duration = ToMiliseconds(clock(), sw);
    r.ProcessedItems = itemsPerTransaction;
    r.Time = t;
    
    records.push_back(r);
  }
  
  endTime = clock();
  
  double secs = ToSeconds(endTime, startTime);
  
  db->~DB();
  
  std::cout << "Wrote " << numberOfTransactions * itemsPerTransaction << " items in " << secs << " sec, " << (numberOfTransactions * itemsPerTransaction)/secs << " ops/s" << std::endl;
  
  return records;
};

std::vector<PerformanceRecord> Read(std::vector<TestData> data, int itemsPerTransaction, int numberOfTransactions) {
  std::vector<PerformanceRecord> records;
  
  leveldb::Options o;
  o.error_if_exists = false;
  o.create_if_missing = false;
  
  leveldb::DB* db;
  leveldb::Status s = leveldb::DB::Open(o, "./testDB", &db);
  
  leveldb::ReadOptions ro;
  
  int startTime, endTime;
  time_t t;
  
  startTime = clock();
  
  for(int transactions = 0; transactions < numberOfTransactions; transactions++) {
    for(int i = 0; i < itemsPerTransaction; i++) {
      TestData item = data.at(transactions * itemsPerTransaction + i);
      
      std::stringstream sKey;
      sKey << std::setw(16) << std::setfill('0') << item.Id;
      std::string key = sKey.str();
      
      std::string result;
      db->Get(ro, key, &result);
    }
  }
  
  time(&t);
    
  endTime = clock();
  
  PerformanceRecord r;
  r.Duration = ToMiliseconds(endTime, startTime);
  r.ProcessedItems = numberOfTransactions * itemsPerTransaction;
  r.Time = t;
  
  records.push_back(r);
  
  double secs = ToSeconds(endTime, startTime);
  
  db->~DB();
  
  std::cout << "Read " << numberOfTransactions * itemsPerTransaction << " items in " << secs << " sec, " << (numberOfTransactions * itemsPerTransaction)/secs << " ops/s" << std::endl;
  
  return records;
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

int main(int argc, char** argv) {

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