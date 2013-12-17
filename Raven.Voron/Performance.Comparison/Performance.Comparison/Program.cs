using System.Collections;

namespace Performance.Comparison
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using Performance.Comparison.Esent;
    using Performance.Comparison.Voron;

    class Program
    {
        private const bool EnableCsvOutput = true;

        private static StreamWriter writer;

        static void Main()
        {
            var random = new Random();
            var buffer = new byte[87 * 1024];
            random.NextBytes(buffer);

            var path = @"c:\work\temp\";

            writer = new StreamWriter("output.txt", false) { AutoFlush = true };

            var sequentialIds = InitSequentialNumbers(Constants.WriteTransactions * Constants.ItemsPerTransaction, minValueSize: 128, maxValueSize: 128);
            var randomIds = InitRandomNumbers(Constants.WriteTransactions * Constants.ItemsPerTransaction, minValueSize: 128, maxValueSize: 128);

            var sequentialIdsLarge = InitSequentialNumbers(Constants.WriteTransactions * Constants.ItemsPerTransaction, minValueSize: 512, maxValueSize: 87 * 1024);
            var randomIdsLarge = InitRandomNumbers(Constants.WriteTransactions * Constants.ItemsPerTransaction, minValueSize: 512, maxValueSize: 87 * 1024);


            var performanceTests = new List<IStoragePerformanceTest>()
				{
                    //new SqlServerTest(buffer),
                    //new SqlLiteTest(path, buffer),
                    //new SqlCeTest(path, buffer),
                    //new LmdbTest(path, buffer),
                    new EsentTest(path, buffer),
                    //new FdbTest(buffer),
					new VoronTest(path, buffer)
				};

            var perfTracker = new PerfTracker();
            for (var i = 0; i < performanceTests.Count; i++)
            {
                var test = performanceTests[i];

                Console.WriteLine("Testing: " + test.StorageName);

                PerformanceRecord performanceRecord;
                List<PerformanceRecord> performanceRecords;
                long totalDuration;
                long items;

                performanceRecords = test.WriteSequential(sequentialIds, perfTracker);
                items = performanceRecords.Sum(x => x.ProcessedItems);
                totalDuration = performanceRecords.Sum(x => x.Duration);
                OutputResults("Write Seq", items, totalDuration, perfTracker);
                WritePerfData("WriteSeq", test, performanceRecords);

                performanceRecords = test.WriteParallelSequential(sequentialIds, perfTracker, 2, out totalDuration);
                items = performanceRecords.Sum(x => x.ProcessedItems);
                OutputResults("Write Seq [2]", items, totalDuration, perfTracker);
                WritePerfData("WriteSeq_Parallel_2", test, performanceRecords);


                performanceRecords = test.WriteParallelSequential(sequentialIds, perfTracker, 4, out totalDuration);
                items = performanceRecords.Sum(x => x.ProcessedItems);
                OutputResults("Write Seq [4]", items, totalDuration, perfTracker);
                WritePerfData("WriteSeq_Parallel_4", test, performanceRecords);


                performanceRecords = test.WriteParallelSequential(sequentialIds, perfTracker, 8, out totalDuration);
                items = performanceRecords.Sum(x => x.ProcessedItems);
                OutputResults("Write Seq [8]", items, totalDuration, perfTracker);
                WritePerfData("WriteSeq_Parallel_8", test, performanceRecords);

                performanceRecords = test.WriteParallelSequential(sequentialIds, perfTracker, 16, out totalDuration);
                items = performanceRecords.Sum(x => x.ProcessedItems);
                OutputResults("Write Seq [16]", items, totalDuration, perfTracker);
                WritePerfData("WriteSeq_Parallel_16", test, performanceRecords);


                performanceRecord = test.ReadSequential(perfTracker);
                items = performanceRecord.ProcessedItems;
                totalDuration = performanceRecord.Duration;
                OutputResults("Read Seq", items, totalDuration, perfTracker);
                WritePerfData("ReadSeq", test, new List<PerformanceRecord> { performanceRecord });

                performanceRecord = test.ReadParallelSequential(perfTracker, 2);
                items = performanceRecord.ProcessedItems;
                totalDuration = performanceRecord.Duration;
                OutputResults("Read Seq [2]", items, totalDuration, perfTracker);
                WritePerfData("ReadSeq_Parallel_2", test, new List<PerformanceRecord> { performanceRecord });

                performanceRecord = test.ReadParallelSequential(perfTracker, 4);
                items = performanceRecord.ProcessedItems;
                totalDuration = performanceRecord.Duration;
                OutputResults("Read Seq [4]", items, totalDuration, perfTracker);
                WritePerfData("ReadSeq_Parallel_4", test, new List<PerformanceRecord> { performanceRecord });

                performanceRecord = test.ReadParallelSequential(perfTracker, 8);
                items = performanceRecord.ProcessedItems;
                totalDuration = performanceRecord.Duration;
                OutputResults("Read Seq [8]", items, totalDuration, perfTracker);
                WritePerfData("ReadSeq_Parallel_8", test, new List<PerformanceRecord> { performanceRecord });

                performanceRecord = test.ReadParallelSequential(perfTracker, 16);
                items = performanceRecord.ProcessedItems;
                totalDuration = performanceRecord.Duration;
                OutputResults("Read Seq [16]", items, totalDuration, perfTracker);
                WritePerfData("ReadSeq_Parallel_16", test, new List<PerformanceRecord> { performanceRecord });

                performanceRecords = test.WriteRandom(randomIds, perfTracker);
                items = performanceRecords.Sum(x => x.ProcessedItems);
                totalDuration = performanceRecords.Sum(x => x.Duration);
                OutputResults("Write Rnd", items, totalDuration, perfTracker);
                WritePerfData("WriteRnd", test, performanceRecords);

                performanceRecords = test.WriteParallelRandom(randomIds, perfTracker, 2, out totalDuration);
                items = performanceRecords.Sum(x => x.ProcessedItems);
                OutputResults("Write Rnd [2]", items, totalDuration, perfTracker);
                WritePerfData("WriteRnd_Parallel_2", test, performanceRecords);

                performanceRecords = test.WriteParallelRandom(randomIds, perfTracker, 4, out totalDuration);
                items = performanceRecords.Sum(x => x.ProcessedItems);
                OutputResults("Write Rnd [4]", items, totalDuration, perfTracker);
                WritePerfData("WriteRnd_Parallel_4", test, performanceRecords);

                performanceRecords = test.WriteParallelRandom(randomIds, perfTracker, 8, out totalDuration);
                items = performanceRecords.Sum(x => x.ProcessedItems);
                OutputResults("Write Rnd [8]", items, totalDuration, perfTracker);
                WritePerfData("WriteRnd_Parallel_8", test, performanceRecords);

                performanceRecords = test.WriteParallelRandom(randomIds, perfTracker, 16, out totalDuration);
                items = performanceRecords.Sum(x => x.ProcessedItems);
                OutputResults("Write Rnd [16]", items, totalDuration, perfTracker);
                WritePerfData("WriteRnd_Parallel_16", test, performanceRecords);

                performanceRecord = test.ReadRandom(randomIds.Select(x => x.Id), perfTracker);
                items = performanceRecord.ProcessedItems;
                totalDuration = performanceRecord.Duration;
                OutputResults("Read Rnd", items, totalDuration, perfTracker);
                WritePerfData("ReadRnd", test, new List<PerformanceRecord> { performanceRecord });

                performanceRecord = test.ReadParallelRandom(randomIds.Select(x => x.Id), perfTracker, 2);
                items = performanceRecord.ProcessedItems;
                totalDuration = performanceRecord.Duration;
                OutputResults("Read Rnd [2]", items, totalDuration, perfTracker);
                WritePerfData("ReadRnd_Parallel_2", test, new List<PerformanceRecord> { performanceRecord });

                performanceRecord = test.ReadParallelRandom(randomIds.Select(x => x.Id), perfTracker, 4);
                items = performanceRecord.ProcessedItems;
                totalDuration = performanceRecord.Duration;
                OutputResults("Read Rnd [4]", items, totalDuration, perfTracker);
                WritePerfData("ReadRnd_Parallel_4", test, new List<PerformanceRecord> { performanceRecord });

                performanceRecord = test.ReadParallelRandom(randomIds.Select(x => x.Id), perfTracker, 8);
                items = performanceRecord.ProcessedItems;
                totalDuration = performanceRecord.Duration;
                OutputResults("Read Rnd [8]", items, totalDuration, perfTracker);
                WritePerfData("ReadRnd_Parallel_8", test, new List<PerformanceRecord> { performanceRecord });

                performanceRecord = test.ReadParallelRandom(randomIds.Select(x => x.Id), perfTracker, 16);
                items = performanceRecord.ProcessedItems;
                totalDuration = performanceRecord.Duration;
                OutputResults("Read Rnd [16]", items, totalDuration, perfTracker);
                WritePerfData("ReadRnd_Parallel_16", test, new List<PerformanceRecord> { performanceRecord });

                if (test.CanHandleBigData == false)
                    continue;

                performanceRecords = test.WriteSequential(sequentialIdsLarge, perfTracker);
                items = performanceRecords.Sum(x => x.ProcessedItems);
                totalDuration = performanceRecords.Sum(x => x.Duration);
                OutputResults("Write Lrg Seq", items, totalDuration, perfTracker);
                WritePerfData("WriteLrgSeq", test, performanceRecords);

                performanceRecord = test.ReadSequential(perfTracker);
                items = performanceRecord.ProcessedItems;
                totalDuration = performanceRecord.Duration;
                OutputResults("Read Lrg Seq", items, totalDuration, perfTracker);
                WritePerfData("ReadLrgSeq", test, new List<PerformanceRecord> { performanceRecord });

                performanceRecords = test.WriteRandom(randomIdsLarge, perfTracker);
                items = performanceRecords.Sum(x => x.ProcessedItems);
                totalDuration = performanceRecords.Sum(x => x.Duration);
                OutputResults("Write Lrg Rnd", items, totalDuration, perfTracker);
                WritePerfData("WriteLrgRnd", test, performanceRecords);

                performanceRecord = test.ReadRandom(randomIdsLarge.Select(x => x.Id), perfTracker);
                items = performanceRecord.ProcessedItems;
                totalDuration = performanceRecord.Duration;
                OutputResults("Read Lrg Rnd", items, totalDuration, perfTracker);
                WritePerfData("ReadLrgSeq", test, new List<PerformanceRecord> { performanceRecord });
            }
        }


        private static void OutputResults(string name, long itemsCount, double duration, PerfTracker perfTracker)
        {
            Console.WriteLine("{0}:\t{1,10:#,#;;0} items in {2,10:#,#;;0} sec, {3,10:#,#} ops/s.", name, itemsCount, duration, itemsCount / (duration / 1000));
            writer.WriteLine("{0}:\t{1,10:#,#;;0} items in {2,10:#,#;;0} sec, {3,10:#,#} ops/s.", name, itemsCount, duration, itemsCount / (duration / 1000));
            writer.WriteLine(string.Join(", ", from f in perfTracker.Checkout() select f.ToString("##,###.00")));
        }

        private static void WritePerfData(string name, IStoragePerformanceTest test, IEnumerable<PerformanceRecord> writeSeq)
        {
            if (!EnableCsvOutput)
                return;

            using (var file = File.Open(name + "_" + test.GetType().Name + ".csv", FileMode.Create))
            using (var writer = new StreamWriter(file, Encoding.UTF8))
            {
                writer.WriteLine("Items,Time,Duration");
                foreach (var p in writeSeq)
                {
                    writer.WriteLine("{0},{1},{2}", p.ProcessedItems, p.Time, p.Duration);
                }
            }
        }

        private static IEnumerable<TestData> InitRandomNumbers(int count, int minValueSize, int maxValueSize)
        {
            var random = new RandomSequenceOfUnique(32);
            var enumerable = Enumerable.Range(0, count).Select(i => random.Next());
            var initRandomNumbers = InitValue(enumerable, minValueSize, maxValueSize);
            foreach (var initRandomNumber in initRandomNumbers)
            {
                yield return initRandomNumber;
            }
        }

        /// <summary>
        /// from https://github.com/preshing/RandomSequence/blob/master/randomsequence.h
        /// </summary>
        private class RandomSequenceOfUnique
        {
            uint _index;
            readonly uint _intermediateOffset;

            const uint Prime = 4294967291u;

            static uint PermuteQPR(uint x)
            {
                if (x >= Prime)
                    return x;  // The 5 integers out of range are mapped to themselves.
                uint residue = (uint)(((ulong)x * x) % Prime);
                return (x <= Prime / 2) ? residue : Prime - residue;
            }

            public RandomSequenceOfUnique(uint seedBase)
            {
                _index = PermuteQPR(PermuteQPR(seedBase) + 0x682f0161);
                _intermediateOffset = PermuteQPR(PermuteQPR(seedBase + 1) + 0x46790905);
            }

            public int Next()
            {
                return (int)PermuteQPR((PermuteQPR(_index++) + _intermediateOffset) ^ 0x5bf03635);
            }
        }

        private static IEnumerable<TestData> InitSequentialNumbers(int count, int minValueSize, int maxValueSize)
        {
            return InitValue(Enumerable.Range(0, count), minValueSize, maxValueSize);
        }

        private static IEnumerable<TestData> InitValue(IEnumerable<int> ids, int minValueSize, int maxValueSize)
        {
            var random = new Random(1337 ^ 13);

            foreach (var id in ids)
            {
                yield return new TestData { Id = id, ValueSize = random.Next(minValueSize, maxValueSize) };
            }
        }
    }
}
