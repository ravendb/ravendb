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
					new SqlServerTest(buffer),
					new SqlLiteTest(path, buffer),
					new SqlCeTest(path, buffer),
                    new LmdbTest(path, buffer),
                    new EsentTest(path, buffer),
                    new VoronTest(path, FlushMode.Full, buffer)
				};

            for(var i = 0; i < performanceTests.Count; i++)
            {
                var test = performanceTests[i];

                Console.WriteLine("Testing: " + test.StorageName);

                var writeSeq = test.WriteSequential(sequentialIds);
                var items = writeSeq.Sum(x => x.ProcessedItems);
                double totalDuration = writeSeq.Sum(x => x.Duration);
                var avgMemoryInMegaBytes = writeSeq.Average(x => x.Memory) / (1024 * 1024);
                //WritePerfData("WriteSeq", test, writeSeq);
                Console.WriteLine("Write seq    ->  {0} items in {1} sec, {2,10:#,#} ops/s. Mem: {3:0} MB", items, totalDuration / 1000, items / (totalDuration / 1000), avgMemoryInMegaBytes);

                var readSeq = test.ReadSequential();
                items = readSeq.ProcessedItems;
                totalDuration = readSeq.Duration;
                avgMemoryInMegaBytes = readSeq.Memory / (1024 * 1024);
                //WritePerfData("ReadSeq", test, readSeq);
                Console.WriteLine("Read seq     ->  {0} items in {1} sec, {2,10:#,#} ops/s. Mem: {3:0} MB", items, totalDuration / 1000, items / (totalDuration / 1000), avgMemoryInMegaBytes);

                var writeRandom = test.WriteRandom(randomIds);
                items = writeRandom.Sum(x => x.ProcessedItems);
                totalDuration = writeRandom.Sum(x => x.Duration);
                avgMemoryInMegaBytes = writeRandom.Average(x => x.Memory) / (1024 * 1024);
                Console.WriteLine("Write rnd    ->  {0} items in {1} sec, {2,10:#,#} ops/s. Mem: {3:0} MB", items, totalDuration / 1000, items / (totalDuration / 1000), avgMemoryInMegaBytes);

                var readRandom = test.ReadRandom(randomIds.Select(x => x.Id));
                items = readRandom.ProcessedItems;
                totalDuration = readRandom.Duration;
                avgMemoryInMegaBytes = readRandom.Memory / (1024 * 1024);
                Console.WriteLine("Read rnd     ->  {0} items in {1} sec, {2,10:#,#} ops/s. Mem: {3:0} MB", items, totalDuration / 1000, items / (totalDuration / 1000), avgMemoryInMegaBytes);

                writeSeq = test.WriteSequential(sequentialIdsLarge);
                items = writeSeq.Sum(x => x.ProcessedItems);
                totalDuration = writeSeq.Sum(x => x.Duration);
                avgMemoryInMegaBytes = writeSeq.Average(x => x.Memory) / (1024 * 1024);
                //WritePerfData("WriteSeq", test, writeSeq);
                Console.WriteLine("Write lrg seq    ->  {0} items in {1} sec, {2,10:#,#} ops/s. Mem: {3:0} MB", items, totalDuration / 1000, items / (totalDuration / 1000), avgMemoryInMegaBytes);

                readSeq = test.ReadSequential();
                items = readSeq.ProcessedItems;
                totalDuration = readSeq.Duration;
                avgMemoryInMegaBytes = readSeq.Memory / (1024 * 1024);
                //WritePerfData("ReadSeq", test, readSeq);
                Console.WriteLine("Read lrg seq     ->  {0} items in {1} sec, {2,10:#,#} ops/s. Mem: {3:0} MB", items, totalDuration / 1000, items / (totalDuration / 1000), avgMemoryInMegaBytes);

                writeRandom = test.WriteRandom(randomIdsLarge);
                items = writeRandom.Sum(x => x.ProcessedItems);
                totalDuration = writeRandom.Sum(x => x.Duration);
                avgMemoryInMegaBytes = writeRandom.Average(x => x.Memory) / (1024 * 1024);
                Console.WriteLine("Write lrg rnd    ->  {0} items in {1} sec, {2,10:#,#} ops/s. Mem: {3:0} MB", items, totalDuration / 1000, items / (totalDuration / 1000), avgMemoryInMegaBytes);

                readRandom = test.ReadRandom(randomIdsLarge.Select(x => x.Id));
                items = readRandom.ProcessedItems;
                totalDuration = readRandom.Duration;
                avgMemoryInMegaBytes = readRandom.Memory / (1024 * 1024);
                Console.WriteLine("Read lrg rnd     ->  {0} items in {1} sec, {2,10:#,#} ops/s. Mem: {3:0} MB", items, totalDuration / 1000, items / (totalDuration / 1000), avgMemoryInMegaBytes);

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
