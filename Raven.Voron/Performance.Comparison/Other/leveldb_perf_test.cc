#include <sstream>
#include <iostream>
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

void Write(std::vector<TestData> data, int itemsPerTransaction, int numberOfTransactions) {
	leveldb::Options o;
	o.error_if_exists = false;
	o.create_if_missing = true;

	leveldb::DestroyDB("./testDB", o);

	leveldb::DB* db;
	leveldb::Status s = leveldb::DB::Open(o, "./testDB", &db);

	leveldb::WriteOptions wo;
	wo.sync = true;

	int startTime, endTime;

	startTime = clock();

	for (int transactions = 0; transactions < numberOfTransactions; transactions++) {

		leveldb::WriteBatch wb;

		for (int i = 0; i < itemsPerTransaction; i++) {
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
	}

	endTime = clock();

	double secs = (endTime - startTime) / (CLOCKS_PER_SEC / 1000) / (double)1000;

	db->~DB();

	std::cout << "Wrote " << numberOfTransactions * itemsPerTransaction << " items in " << secs << " sec, " << (numberOfTransactions * itemsPerTransaction) / secs << " ops/s" << std::endl;
};

void Read(std::vector<TestData> data, int itemsPerTransaction, int numberOfTransactions) {

	leveldb::Options o;
	o.error_if_exists = false;
	o.create_if_missing = false;

	leveldb::DB* db;
	leveldb::Status s = leveldb::DB::Open(o, "./testDB", &db);

	leveldb::ReadOptions ro;

	int startTime, endTime;

	startTime = clock();

	for (int transactions = 0; transactions < numberOfTransactions; transactions++) {
		for (int i = 0; i < itemsPerTransaction; i++) {
			TestData item = data.at(transactions * itemsPerTransaction + i);

			std::stringstream sKey;
			sKey << std::setw(16) << std::setfill('0') << item.Id;
			std::string key = sKey.str();

			std::string result;
			db->Get(ro, key, &result);
		}
	}

	endTime = clock();

	double secs = (endTime - startTime) / (CLOCKS_PER_SEC / 1000) / (double)1000;

	db->~DB();

	std::cout << "Read " << numberOfTransactions * itemsPerTransaction << " items in " << secs << " sec, " << (numberOfTransactions * itemsPerTransaction) / secs << " ops/s" << std::endl;
};

std::vector<TestData> InitValue(std::set<int> ids, int minValueSize, int maxValueSize) {
	std::vector<TestData> data;

	std::set<int>::iterator it;
	for (it = ids.begin(); it != ids.end(); ++it) {

		TestData t;
		t.Id = *it;
		t.ValueSize = rand() % (maxValueSize + 1 - minValueSize) + minValueSize;

		data.push_back(t);
	}

	return data;
};

std::vector<TestData> InitSequentialNumbers(int count, int minValueSize, int maxValueSize) {
	std::set<int> ids;

	for (int i = 0; i < count; i++) {
		ids.insert(i);
	}

	return InitValue(ids, minValueSize, maxValueSize);
};

std::vector<TestData> InitRandomNumbers(int count, int minValueSize, int maxValueSize) {
	std::vector<TestData> data = InitSequentialNumbers(count, minValueSize, maxValueSize);

	std::random_shuffle(data.begin(), data.end());

	return data;
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

	Write(sequentialIds, itemsPerTransaction, writeTransactions);
	Read(sequentialIds, itemsPerTransaction, writeTransactions);

	Write(randomIds, itemsPerTransaction, writeTransactions);
	Read(randomIds, itemsPerTransaction, writeTransactions);

	Write(sequentialIdsLarge, itemsPerTransaction, writeTransactions);
	Read(sequentialIdsLarge, itemsPerTransaction, writeTransactions);

	Write(randomIdsLarge, itemsPerTransaction, writeTransactions);
	Read(randomIdsLarge, itemsPerTransaction, writeTransactions);

	return 0;
}