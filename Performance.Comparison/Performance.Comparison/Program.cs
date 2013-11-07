using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Performance.Comparison.SQLCE;
using Performance.Comparison.SQLite;
using Performance.Comparison.SQLServer;
using Performance.Comparison.Voron;
using Voron.Impl;

namespace Performance.Comparison
{
    using System.Security.Policy;

    using Performance.Comparison.Esent;
    using Performance.Comparison.LMDB;

    class Program
    {
        static void Main(string[] args)
        {
            var random = new Random();
            var buffer = new byte[87 * 1024];
            random.NextBytes(buffer);

            var path = @"c:\work\temp\";

            var sequentialIds = InitSequentialNumbers(Constants.WriteTransactions * Constants.ItemsPerTransaction, minValueSize: 128, maxValueSize: 128);
            var randomIds = InitRandomNumbers(Constants.WriteTransactions * Constants.ItemsPerTransaction, minValueSize: 128, maxValueSize: 128);

            var sequentialIdsLarge = InitSequentialNumbers(Constants.WriteTransactions * Constants.ItemsPerTransaction, minValueSize: 512, maxValueSize: 87 * 1024);
            var randomIdsLarge = InitRandomNumbers(Constants.WriteTransactions * Constants.ItemsPerTransaction, minValueSize: 512, maxValueSize: 87 * 1024);

            var performanceTests = new List<IStoragePerformanceTest>()
				{
					//new SqlServerTest(),
					//new SqlLiteTest(path),
					//new SqlCeTest(path),
                    //new LmdbTest(path, buffer),
                    //new EsentTest(path),
                    new VoronTest(path, FlushMode.Full, buffer)
				};

            foreach (var test in performanceTests)
            {
                Console.WriteLine("Testing: " + test.StorageName);

                var writeSeq = test.WriteSequential(sequentialIds);
                var items = writeSeq.Sum(x => x.ProcessedItems);
                double totalDuration = writeSeq.Sum(x => x.Duration);
                //WritePerfData("WriteSeq", test, writeSeq);
                Console.WriteLine("Write sequential ->  {0} items in {1} sec, {2,10:#,#} ops / sec", items, totalDuration / 1000, items / (totalDuration / 1000));

                var readSeq = test.ReadSequential();
                items = readSeq.ProcessedItems;
                totalDuration = readSeq.Duration;
                //WritePerfData("ReadSeq", test, readSeq);
                Console.WriteLine("Read sequential ->  {0} items in {1} sec, {2,10:#,#} ops / sec", items, totalDuration / 1000, items / (totalDuration / 1000));

                var writeRandom = test.WriteRandom(randomIds);
                items = writeRandom.Sum(x => x.ProcessedItems);
                totalDuration = writeRandom.Sum(x => x.Duration);
                Console.WriteLine("Write random ->  {0} items in {1} sec, {2,10:#,#} ops / sec", items, totalDuration / 1000, items / (totalDuration / 1000));

                var readRandom = test.ReadRandom(randomIds.Select(x => x.Id));
                items = readRandom.ProcessedItems;
                totalDuration = readRandom.Duration;
                Console.WriteLine("Read random ->  {0} items in {1} sec, {2,10:#,#} ops / sec", items, totalDuration / 1000, items / (totalDuration / 1000));

                writeSeq = test.WriteSequential(sequentialIdsLarge);
                items = writeSeq.Sum(x => x.ProcessedItems);
                totalDuration = writeSeq.Sum(x => x.Duration);
                //WritePerfData("WriteSeq", test, writeSeq);
                Console.WriteLine("Write large sequential ->  {0} items in {1} sec, {2,10:#,#} ops / sec", items, totalDuration / 1000, items / (totalDuration / 1000));

                readSeq = test.ReadSequential();
                items = readSeq.ProcessedItems;
                totalDuration = readSeq.Duration;
                //WritePerfData("ReadSeq", test, readSeq);
                Console.WriteLine("Read large sequential ->  {0} items in {1} sec, {2,10:#,#} ops / sec", items, totalDuration / 1000, items / (totalDuration / 1000));

                writeRandom = test.WriteRandom(randomIdsLarge);
                items = writeRandom.Sum(x => x.ProcessedItems);
                totalDuration = writeRandom.Sum(x => x.Duration);
                Console.WriteLine("Write large random ->  {0} items in {1} sec, {2,10:#,#} ops / sec", items, totalDuration / 1000, items / (totalDuration / 1000));

                readRandom = test.ReadRandom(randomIdsLarge.Select(x => x.Id));
                items = readRandom.ProcessedItems;
                totalDuration = readRandom.Duration;
                Console.WriteLine("Read large random ->  {0} items in {1} sec, {2,10:#,#} ops / sec", items, totalDuration / 1000, items / (totalDuration / 1000));

                Console.WriteLine("---------------------------");
            }
        }

        private static void WritePerfData(string name, IStoragePerformanceTest test, List<PerformanceRecord> writeSeq)
        {
            using (var file = File.Open(name + "_" + test.GetType().Name + ".csv", FileMode.Create))
            using (var writer = new StreamWriter(file))
            {
                foreach (var p in writeSeq)
                {
                    writer.WriteLine("{0},{1},{2}", p.ProcessedItems, p.Time, p.Duration);
                }
            }
        }

        private static HashSet<TestData> InitRandomNumbers(int count, int minValueSize, int maxValueSize)
        {
            var random = new Random(1337 ^ 13);
            var randomNumbers = new HashSet<int>();

            while (randomNumbers.Count < count)
            {
                randomNumbers.Add(random.Next(0, int.MaxValue));
            }

            return InitValue(randomNumbers, minValueSize, maxValueSize);
        }

        private static HashSet<TestData> InitSequentialNumbers(int count, int minValueSize, int maxValueSize)
        {
            return InitValue(Enumerable.Range(0, count), minValueSize, maxValueSize);
        }

        private static HashSet<TestData> InitValue(IEnumerable<int> ids, int minValueSize, int maxValueSize)
        {
            var data = new HashSet<TestData>();

            var random = new Random(1337 ^ 13);

            foreach (var id in ids)
            {
                data.Add(new TestData { Id = id, ValueSize = random.Next(minValueSize, maxValueSize) });
            }

            return data;
        }
    }
}
