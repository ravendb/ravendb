//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using NDesk.Options;
using NLog;

namespace Raven.Performance
{
	class Program
	{
		private string databaseLocation;
		private string dataLocation;
		private string buildNumber;
		private string logsLocation;

		private static readonly Logger Logger = LogManager.GetCurrentClassLogger();
		private OptionSet optionSet;

		static void Main(string[] args)
		{
			var program = new Program();
			program.Parse(args);
		}

		private void Parse(string[] args)
		{
			optionSet = new OptionSet
			                	{
			                		{"database|database-location=", "The folder that contains folders in the following format: RavenDB-Build-{build-number}.", value => databaseLocation = value},
			                		{"build|build-number=", "The build number to test.", value => buildNumber = value},
			                		{"data|data-location=", "The FreeDB data location.", value => dataLocation = value},
			                		{"logs|logs-location=", "The location where to put the logs.", value => logsLocation = value},
			                	};

			try
			{
				optionSet.Parse(args);
			}
			catch (Exception e)
			{
				PrintUsageAndExit(e);
			}

			MeasurePerformance();
		}

		private void MeasurePerformance()
		{
			var writer = new StreamWriter(GetLogFile(), false);
			writer.WriteLine("Test number, Time, Memory Min, Memory Max, Memory Average, Latency Time Min, Latency Time Max, Latency Time Average, Latency Docs Min, Latency Docs Max, Latency Docs Average");

			var p = Process.Start(FullDatabaseLocation);
			if (p == null)
				return;
			Thread.Sleep(1000); // wait for server to load

			var tester = new Tester(dataLocation, p);

			tester.ClearDatabase(FullDatabaseLocation);

			Environment.SetEnvironmentVariable("TestId", "1");
			Logger.Info("Starting Test 1 - loading data without indexing");
			var testStartTime = DateTime.UtcNow;
			tester.AddData();
			Logger.Info("Test took {0}", (DateTime.UtcNow - testStartTime).ToString("hh\\:mm\\:ss\\.ff"));
			Logger.Info("Memory: Max: {0:#,#} MB Min: {1:#,#} MB Avg: {2:#,#} MB", ToMB(tester.MemoryUsage.Max()), ToMB(tester.MemoryUsage.Min()), ToMB(tester.MemoryUsage.Average()));
			writer.WriteLine("{0},{1},{2},{3},{4}"
				, 1, (DateTime.UtcNow - testStartTime).ToString("hh\\:mm\\:ss\\.ff"), ToMB(tester.MemoryUsage.Min()), ToMB(tester.MemoryUsage.Max()), ToMB(tester.MemoryUsage.Average()));

			Environment.SetEnvironmentVariable("TestId", "2");
			Logger.Info("Starting Test 2 - Simple Index");
			testStartTime = DateTime.UtcNow;
			tester.CreateIndexSimple();
			tester.WaitForIndexesToBecomeNonStale();
			Logger.Info("Test took {0}", (DateTime.UtcNow - testStartTime).ToString("hh\\:mm\\:ss\\.ff"));
			Logger.Info("Memory: Max: {0:#,#} MB Min: {1:#,#} MB Avg: {2:#,#} MB", ToMB(tester.MemoryUsage.Max()), ToMB(tester.MemoryUsage.Min()), ToMB(tester.MemoryUsage.Average()));
			writer.WriteLine("{0},{1},{2},{3},{4}"
				, 2, (DateTime.UtcNow - testStartTime).ToString("hh\\:mm\\:ss\\.ff"), ToMB(tester.MemoryUsage.Min()), ToMB(tester.MemoryUsage.Max()), ToMB(tester.MemoryUsage.Average()));

			Environment.SetEnvironmentVariable("TestId", "3");
			Logger.Info("Starting Test 3 - Search Index");
			testStartTime = DateTime.UtcNow;
			tester.CreateIndexSearch();
			tester.WaitForIndexesToBecomeNonStale();
			Logger.Info("Test took {0}", (DateTime.UtcNow - testStartTime).ToString("hh\\:mm\\:ss\\.ff"));
			Logger.Info("Memory: Max: {0:#,#} MB Min: {1:#,#} MB Avg: {2:#,#} MB", ToMB(tester.MemoryUsage.Max()), ToMB(tester.MemoryUsage.Min()), ToMB(tester.MemoryUsage.Average()));
			writer.WriteLine("{0},{1},{2},{3},{4}"
				, 3, (DateTime.UtcNow - testStartTime).ToString("hh\\:mm\\:ss\\.ff"), ToMB(tester.MemoryUsage.Min()), ToMB(tester.MemoryUsage.Max()), ToMB(tester.MemoryUsage.Average()));

			Environment.SetEnvironmentVariable("TestId", "4");
			Logger.Info("Starting Test 4 - Index Entity Name");
			testStartTime = DateTime.UtcNow;
			tester.CreateIndexEntityName();
			tester.WaitForIndexesToBecomeNonStale();
			Logger.Info("Test took {0}", (DateTime.UtcNow - testStartTime).ToString("hh\\:mm\\:ss\\.ff"));
			Logger.Info("Memory: Max: {0:#,#} MB Min: {1:#,#} MB Avg: {2:#,#} MB", ToMB(tester.MemoryUsage.Max()), ToMB(tester.MemoryUsage.Min()), ToMB(tester.MemoryUsage.Average()));
			writer.WriteLine("{0},{1},{2},{3},{4}"
				, 4, (DateTime.UtcNow - testStartTime).ToString("hh\\:mm\\:ss\\.ff"), ToMB(tester.MemoryUsage.Min()), ToMB(tester.MemoryUsage.Max()), ToMB(tester.MemoryUsage.Average()));

			p.Kill();
			tester.ClearDatabase(FullDatabaseLocation);

			p = Process.Start(FullDatabaseLocation);
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
				writer.WriteLine("{0}.{1},{2},{3},{4},{5},{6:#,#.##},{7:#,#.##},{8:#,#.##},{9:#,#.##},{10:#,#.##},{11:#,#.##}"
				, 5, counter++, (DateTime.UtcNow - testStartTime).ToString("hh\\:mm\\:ss\\.ff"), ToMB(tester.MemoryUsage.Min()), ToMB(tester.MemoryUsage.Max()), ToMB(tester.MemoryUsage.Average())
				, latency.Item2.Min(), latency.Item2.Max(), latency.Item2.Average(), latency.Item3.Min(), latency.Item3.Max(), latency.Item3.Average());

				Logger.Info("Latencies {0}: Avg: {1:#,#} ms {2:#,#} docs, Max: {3:#,#} ms {4:#,#} docs, Min: {5:#,#} ms {6:#,#} docs",
					latency.Item1, latency.Item2.Average(), latency.Item3.Average(),
					latency.Item2.Max(),
					latency.Item3.Max(),
					latency.Item2.Min(),
					latency.Item3.Min());
			}

			writer.Close();
			writer.Dispose();
			p.Kill();
		}

		private string GetLogFile()
		{
			var fileName = string.Format(@"PerformanceLog-{0}-{1}.csv", buildNumber, DateTime.Now.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss"));
			if (Directory.Exists(logsLocation) == false)
			{
				Directory.CreateDirectory(logsLocation);
			}
			var buildFolder = Path.Combine(logsLocation, buildNumber);
			if (Directory.Exists(buildFolder) == false)
			{
				Directory.CreateDirectory(buildFolder);
			}
			return Path.Combine(buildFolder, fileName);
		}

		private string fullDatabaseLocation;
		private string FullDatabaseLocation
		{
			get
			{
				if (fullDatabaseLocation == null)
				{
					fullDatabaseLocation = Path.Combine(databaseLocation, string.Format("RavenDB-Build-{0}", buildNumber), "Server", "Raven.Server.exe");
					if (File.Exists(fullDatabaseLocation) == false)
					{
						throw new FileNotFoundException("RavenDB server cannot be found. Path lookup: " + fullDatabaseLocation);
					}
				}
				return fullDatabaseLocation;
			}
		}

		private static string ToMB(long number)
		{
			return ((double)number / (1024 * 1024)).ToString("#,#.##");
		}

		private static string ToMB(double number)
		{
			return (number / (1024 * 1024)).ToString("#,#.##");
		}

		private void PrintUsageAndExit(Exception e)
		{
			Console.WriteLine(e.Message);
			PrintUsageAndExit(-1);
		}

		private void PrintUsageAndExit(int exitCode)
		{
			Console.WriteLine(@"
Performance tester for RavenDB
----------------------------------------
Copyright (C) 2008 - {0} - Hibernating Rhinos
----------------------------------------
Command line options:", DateTime.UtcNow.Year);

			optionSet.WriteOptionDescriptions(Console.Out);
			Console.WriteLine();

			Environment.Exit(exitCode);
		}
	}
}