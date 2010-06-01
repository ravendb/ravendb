using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using log4net.Appender;
using log4net.Config;
using log4net.Core;
using log4net.Layout;
using Microsoft.Isam.Esent.Interop;
using Raven.Database;
using Raven.Database.Storage;
using Raven.Server;
using Raven.StackOverflow.Etl.Posts;
using Raven.StackOverflow.Etl.Users;
using Rhino.Etl.Core;
using System.Linq;

namespace Raven.StackOverflow.Etl
{
	class Program
	{
		private static readonly ConcurrentBag<Tuple<string, TimeSpan>> durations = new ConcurrentBag<Tuple<string, TimeSpan>>();

		static void Main()
		{
			const string path = @"C:\Users\Ayende\Downloads\Stack Overflow Data Dump - Mar 10\Content\Export-030110\032010 SO";

			ServicePointManager.DefaultConnectionLimit = int.MaxValue;

			Trace.Listeners.Add(new TextWriterTraceListener(Console.Out));
			Trace.WriteLine("Starting...");
			var sp = Stopwatch.StartNew();

			//GenerateDocumentsToFile(path);

			LoadIntoRaven();

			Console.WriteLine("Total execution time {0}", sp.Elapsed);
		}

		private static void LoadIntoRaven()
		{
			const string dataDirectory = @"C:\Work\ravendb\ETL\Raven.StackOverflow.Etl\bin\Debug\Data";
			if (Directory.Exists(dataDirectory))
				Directory.Delete(dataDirectory, true);

			RavenDbServer.EnsureCanListenToWhenInNonAdminContext(9090);
			using (var ravenDbServer = new RavenDbServer(new RavenConfiguration
			{
				DataDirectory = dataDirectory,
				Port = 9090,
				AnonymousUserAccessMode = AnonymousUserAccessMode.All
			}))
			{
				ExecuteAndWaitAll(
					LoadDataFor("Users*.json")
					//,LoadDataFor("Posts*.json")
					);
				ExecuteAndWaitAll(
					LoadDataFor("Badges*.json")
					//,LoadDataFor("Votes*.json"),
					//LoadDataFor("Comments*.json")
					);

				var indexing = Stopwatch.StartNew();
				Console.WriteLine("Waiting for indexing");
				while (ravenDbServer.Database.HasTasks)
				{
					Console.Write(".");
					Thread.Sleep(50);
				}
				Console.WriteLine();
				Console.WriteLine("Finishing indexing took: {0}", indexing.Elapsed);
			}


			foreach (var duration in durations.GroupBy(x => x.Item1))
			{
				Console.WriteLine("{0} {1}", duration.Key, duration.Average(x => x.Item2.TotalMilliseconds));
			}
		}

		private static void ExecuteAndWaitAll(params IEnumerable<Action>[] taskGenerators)
		{
			Parallel.ForEach(from generator in taskGenerators
							 from action in generator
							 select action,
							 new ParallelOptions { MaxDegreeOfParallelism = 10 },
							 action =>
							 {
								 try
								 {
									 action();
								 }
								 catch (WebException e)
								 {
									 var readToEnd = new StreamReader(e.Response.GetResponseStream()).ReadToEnd();
									 Console.WriteLine(readToEnd);
									 throw;
								 }
							 });

			//foreach (var act in from generator in taskGenerators
			//                              from action in generator
			//                              select action)
			//{
			//    act();
			//}
		}

		private static IEnumerable<Action> LoadDataFor(string searchPattern)
		{
			Console.WriteLine("Loading for {0}", searchPattern);
			var timeSpans = new List<TimeSpan>();
			foreach (var fileModifable in Directory.GetFiles(@"C:\Work\ravendb\ETL\Raven.StackOverflow.Etl\bin\Debug\Docs", searchPattern))
			{
				var file = fileModifable;
				yield return () =>
				{
					var sp = Stopwatch.StartNew();
					HttpWebResponse webResponse;
					while (true)
					{
						var httpWebRequest = (HttpWebRequest)WebRequest.Create("http://localhost:9090/bulk_docs");
						httpWebRequest.Method = "POST";
						using (var requestStream = httpWebRequest.GetRequestStream())
						{
							var readAllBytes = File.ReadAllBytes(file);
							requestStream.Write(readAllBytes, 0, readAllBytes.Length);
						}
						try
						{
							webResponse = (HttpWebResponse)httpWebRequest.GetResponse();
							webResponse.Close();
							break;
						}
						catch (WebException e)
						{
							webResponse = e.Response as HttpWebResponse;
							if (webResponse != null &&
								webResponse.StatusCode == HttpStatusCode.Conflict)
							{
								Console.WriteLine("{0} - {1} - {2} - {3}", Path.GetFileName(file), sp.Elapsed, webResponse.StatusCode,
									Thread.CurrentThread.ManagedThreadId);
								continue;
							}

							Console.WriteLine(new StreamReader(e.Response.GetResponseStream()).ReadToEnd());
							throw;
						}
					}
					var timeSpan = sp.Elapsed;
					timeSpans.Add(timeSpan);
					durations.Add(new Tuple<string, TimeSpan>(searchPattern, timeSpan));
					Console.WriteLine("{0} - {1} - {2} - {3}", Path.GetFileName(file), timeSpan, webResponse.StatusCode,
						Thread.CurrentThread.ManagedThreadId);
				};
			}
		}

		private static void GenerateDocumentsToFile(string path)
		{

			if (Directory.Exists("Docs"))
				Directory.Delete("Docs", true);
			Directory.CreateDirectory("Docs");

			var processes = new EtlProcess[]
			{
				new UsersProcess(path),
				new BadgesProcess(path),
				new PostsProcess(path),
				new VotesProcess(path),
				new CommentsProcess(path)
			};
			Parallel.ForEach(processes, GenerateJsonDocuments);
		}

		private static void WaitForIndexingToComplete(DocumentDatabase documentDatabase)
		{
			Console.WriteLine("Waiting for indexing to complete");
			var sp2 = Stopwatch.StartNew();
			while (documentDatabase.HasTasks)
			{
				documentDatabase.TransactionalStorage.Batch(actions =>
				{
					var indexesStat = actions.GetIndexesStats().First();
					Console.WriteLine("{0} - {1:#,#} - {2:#,#} - {3}", indexesStat.Name,
						indexesStat.IndexingSuccesses,
						actions.GetDocumentsCount(),
						sp2.Elapsed);

					actions.Commit(CommitTransactionGrbit.LazyFlush);
				});

				Thread.Sleep(1000);
			}
		}

		private static void GenerateJsonDocuments(EtlProcess process)
		{
			Console.WriteLine("Executing {0}", process);
			var sp = Stopwatch.StartNew();
			process.Execute();
			Console.WriteLine("Executed {0} in {1}", process, sp.Elapsed);
			var allErrors = process.GetAllErrors().ToArray();
			foreach (var exception in allErrors)
			{
				Console.WriteLine(exception);
			}
			if (allErrors.Length > 0)
			{
				throw new InvalidOperationException("Failed to execute process: " + process);
			}
		}
	}
}
