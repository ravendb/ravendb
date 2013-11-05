using System;
using System.Collections.Generic;
using System.Linq;
using Performance.Comparison.SQLCE;
using Performance.Comparison.SQLite;
using Performance.Comparison.Voron;
using Voron.Impl;

namespace Performance.Comparison
{
	class Program
	{
		static void Main(string[] args)
		{
			var path = @"c:\temp\";


			var randomIds = InitRandomNumbers(Constants.WriteTransactions*Constants.ItemsPerTransaction);

			var performanceTests = new List<IStoragePerformanceTest>()
				{
					new SqlLiteTest(path),
					new SqlCeTest(path),
					new VoronTest(path, FlushMode.Full)
				};

			foreach (var test in performanceTests)
			{
				Console.WriteLine("Testing: " + test.StorageName);

				var writeSeq = test.WriteSequential();
				var items = writeSeq.Sum(x => x.ProcessedItems);
				double totalDuration = writeSeq.Sum(x => x.Duration);

				Console.WriteLine("Write sequential ->  {0} items in {1} sec, {2,10:#,#} ops / sec", items, totalDuration / 1000, items / (totalDuration / 1000));

				var readSeq = test.ReadSequential();
				items = readSeq.ProcessedItems;
				totalDuration = readSeq.Duration;

				Console.WriteLine("Read sequential ->  {0} items in {1} sec, {2,10:#,#} ops / sec", items, totalDuration / 1000, items / (totalDuration / 1000));

				var writeRandom = test.WriteRandom(randomIds);
				items = writeRandom.Sum(x => x.ProcessedItems);
				totalDuration = writeRandom.Sum(x => x.Duration);

				Console.WriteLine("Write random ->  {0} items in {1} sec, {2,10:#,#} ops / sec", items, totalDuration / 1000, items / (totalDuration / 1000));

				var readRandom = test.ReadRandom(randomIds);
				items = readRandom.ProcessedItems;
				totalDuration = readRandom.Duration;

				Console.WriteLine("Read random ->  {0} items in {1} sec, {2,10:#,#} ops / sec", items, totalDuration / 1000, items / (totalDuration / 1000));

				Console.WriteLine("---------------------------");
			}

			Console.ReadLine();
		}

		private static HashSet<int> InitRandomNumbers(int count)
		{
			var random = new Random(1337 ^ 13);
			var randomNumbers = new HashSet<int>();
			while (randomNumbers.Count < count)
			{
				randomNumbers.Add(random.Next(0, int.MaxValue));
			}
			return randomNumbers;
		}
	}
}
