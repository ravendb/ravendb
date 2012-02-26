using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using NLog;

namespace RavenDBStressTester
{
	class Program
	{
		private static string serverLocation;
		private static string dataLocation;

		private static readonly Logger Logger = LogManager.GetLogger("RavenDBStressTester");

		static void Main(string[] args)
		{
			serverLocation = args.SingleOrDefault(s => s.StartsWith("-exe=", StringComparison.OrdinalIgnoreCase));
			dataLocation = args.SingleOrDefault(s => s.StartsWith("-data=", StringComparison.OrdinalIgnoreCase));

			if (serverLocation == null || dataLocation == null || serverLocation.Length == 5 || dataLocation.Length == 6)
			{
				Console.WriteLine("Must receive -exe={server location} and -data={data location}");
				return;
			}

			serverLocation = serverLocation.Split('=')[1];
			dataLocation = dataLocation.Split('=')[1];
			if(Directory.Exists("Logs") == false)
			{
				Directory.CreateDirectory("Logs");
			}
			var textWriter = new StreamWriter("Logs\\LogForExcel.txt");
			textWriter.WriteLine("Test number, Time, Memory Min, Memory Max, Memory Average, Latency Time Min, Latency Time Max, Latency Time Average, Latency Docs Min, Latency Docs Max, Latency Docs Average");


			var p = Process.Start(Path.Combine(serverLocation, "Raven.Server.exe"));
			if (p == null)
				return;
			Thread.Sleep(1000);// wait for server to load

			var tester = new Tester(dataLocation, p);

			tester.ClearDatabase(serverLocation);

			Environment.SetEnvironmentVariable("TestId", "1");
			Logger.Info("Starting Test 1 - loading data without indexing");
			var testStartTime = DateTime.UtcNow;
			tester.AddData();
			Logger.Info("Test took {0}", (DateTime.UtcNow - testStartTime).ToString("hh\\:mm\\:ss\\.ff"));
			Logger.Info("Memory: Max: {0:#,#} MB Min: {1:#,#} MB Avg: {2:#,#} MB", ToMB(tester.MemoryUsage.Max()), ToMB(tester.MemoryUsage.Min()), ToMB(tester.MemoryUsage.Average()));
			textWriter.WriteLine("{0},{1},{2},{3},{4}"
				, 1, (DateTime.UtcNow - testStartTime).ToString("hh\\:mm\\:ss\\.ff") ,ToMB(tester.MemoryUsage.Min()), ToMB(tester.MemoryUsage.Max()), ToMB(tester.MemoryUsage.Average()));

			Environment.SetEnvironmentVariable("TestId", "2");
			Logger.Info("Starting Test 2 - Simple Index");
			testStartTime = DateTime.UtcNow;
			tester.CreateIndexSimple();
			tester.WaitForIndexesToBecomeNonStale();
			Logger.Info("Test took {0}", (DateTime.UtcNow - testStartTime).ToString("hh\\:mm\\:ss\\.ff"));
			Logger.Info("Memory: Max: {0:#,#} MB Min: {1:#,#} MB Avg: {2:#,#} MB", ToMB(tester.MemoryUsage.Max()), ToMB(tester.MemoryUsage.Min()), ToMB(tester.MemoryUsage.Average()));
			textWriter.WriteLine("{0},{1},{2},{3},{4}"
				, 2, (DateTime.UtcNow - testStartTime).ToString("hh\\:mm\\:ss\\.ff"), ToMB(tester.MemoryUsage.Min()), ToMB(tester.MemoryUsage.Max()), ToMB(tester.MemoryUsage.Average()));

			Environment.SetEnvironmentVariable("TestId", "3");
			Logger.Info("Starting Test 3 - Search Index");
			testStartTime = DateTime.UtcNow;
			tester.CreateIndexSearch();
			tester.WaitForIndexesToBecomeNonStale();
			Logger.Info("Test took {0}", (DateTime.UtcNow - testStartTime).ToString("hh\\:mm\\:ss\\.ff"));
			Logger.Info("Memory: Max: {0:#,#} MB Min: {1:#,#} MB Avg: {2:#,#} MB", ToMB(tester.MemoryUsage.Max()), ToMB(tester.MemoryUsage.Min()), ToMB(tester.MemoryUsage.Average()));
			textWriter.WriteLine("{0},{1},{2},{3},{4}"
				, 3, (DateTime.UtcNow - testStartTime).ToString("hh\\:mm\\:ss\\.ff"), ToMB(tester.MemoryUsage.Min()), ToMB(tester.MemoryUsage.Max()), ToMB(tester.MemoryUsage.Average()));

			Environment.SetEnvironmentVariable("TestId", "4");
			Logger.Info("Starting Test 4 - Index Entity Name");
			testStartTime = DateTime.UtcNow;
			tester.CreateIndexEntityName();
			tester.WaitForIndexesToBecomeNonStale();
			Logger.Info("Test took {0}", (DateTime.UtcNow - testStartTime).ToString("hh\\:mm\\:ss\\.ff"));
			Logger.Info("Memory: Max: {0:#,#} MB Min: {1:#,#} MB Avg: {2:#,#} MB", ToMB(tester.MemoryUsage.Max()), ToMB(tester.MemoryUsage.Min()), ToMB(tester.MemoryUsage.Average()));
			textWriter.WriteLine("{0},{1},{2},{3},{4}"
				, 4, (DateTime.UtcNow - testStartTime).ToString("hh\\:mm\\:ss\\.ff"), ToMB(tester.MemoryUsage.Min()), ToMB(tester.MemoryUsage.Max()), ToMB(tester.MemoryUsage.Average()));

			p.Kill();
			tester.ClearDatabase(serverLocation);

			p = Process.Start(Path.Combine(serverLocation, "Raven.Server.exe"));
			if (p == null)
				return;
			Thread.Sleep(1000);// wait for server to load

			tester = new Tester(dataLocation, p);

			Environment.SetEnvironmentVariable("TestId", "5");
			Logger.Info("Starting Test 5 - Create indexes and then add data");
			testStartTime = DateTime.UtcNow;
			tester.CreateAllIndexes();
			tester.AddDataAsync();
			tester.WaitForIndexesToBecomeNonStale();

			Logger.Info("Test took {0}", (DateTime.UtcNow - testStartTime).ToString(@"hh\:mm\:ss\.ff"));
			Logger.Info("Memory: Max: {0:#,#} MB Min: {1:#,#} MB Avg: {2:#,#} MB", ToMB(tester.MemoryUsage.Max()), ToMB(tester.MemoryUsage.Min()), ToMB(tester.MemoryUsage.Average()));
			int counter = 1;
			foreach (var latency in tester.Latencies())
			{

				textWriter.WriteLine("{0}.{1},{2},{3},{4},{5},{6:#,#.##},{7:#,#.##},{8:#,#.##},{9:#,#.##},{10:#,#.##},{11:#,#.##}"
				, 5, counter++, (DateTime.UtcNow - testStartTime).ToString("hh\\:mm\\:ss\\.ff"), ToMB(tester.MemoryUsage.Min()), ToMB(tester.MemoryUsage.Max()), ToMB(tester.MemoryUsage.Average())
				, latency.Item2.Min(), latency.Item2.Max(), latency.Item2.Average(), latency.Item3.Min(),latency.Item3.Max(), latency.Item3.Average());

				Logger.Info("Latencies {0}: Avg: {1:#,#} ms {2:#,#} docs, Max: {3:#,#} ms {4:#,#} docs, Min: {5:#,#} ms {6:#,#} docs", 
					latency.Item1, latency.Item2.Average(), latency.Item3.Average(),
					latency.Item2.Max(),
					latency.Item3.Max(),
					latency.Item2.Min(),
					latency.Item3.Min());
			}

			textWriter.Close();
			p.Kill();
		}

		private static string ToMB(long number)
		{
			return ((double)number / (1024 * 1024)).ToString("#,#.##");
		}

		private static string ToMB(double number)
		{
			return (number / (1024 * 1024)).ToString("#,#.##");
		}
	}
}
