using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Threading;
using log4net.Appender;
using log4net.Config;
using log4net.Core;
using log4net.Layout;
using Raven.Database;
using Raven.Server;
using Raven.StackOverflow.Etl.Posts;
using Raven.StackOverflow.Etl.Users;
using Rhino.Etl.Core;
using System.Linq;

namespace Raven.StackOverflow.Etl
{
	class Program
	{
		static void Main(string[] args)
		{
			const string path = @"C:\Users\Ayende\Downloads\Stack Overflow Data Dump - Mar 10\Content\Export-030110\032010 SO";

			BasicConfigurator.Configure(new ConsoleAppender
			{
				Layout = new SimpleLayout(),
				Threshold = Level.Notice
			});
			Console.WriteLine("Starting...");
			var sp = Stopwatch.StartNew();

			//GenerateDocumentsToFile(path);

			const string dataDirectory = @"C:\Work\ravendb\ETL\Raven.StackOverflow.Etl\bin\Debug\Data";
			if (Directory.Exists(dataDirectory))
				Directory.Delete(dataDirectory, true);

			using (var ravenDbServer = new RavenDbServer(new RavenConfiguration
			{
				DataDirectory = dataDirectory,
				Port = 8080,
				AnonymousUserAccessMode = AnonymousUserAccessMode.All
			}))
			{
				LoadDataFor("Users*.json");
				LoadDataFor("Badges*.json");
				LoadDataFor("Posts*.json");
				LoadDataFor("Votes*.json");
				LoadDataFor("Comments*.json");

				var indexing = Stopwatch.StartNew();
				Console.WriteLine("Waiting for indexing");
				while(ravenDbServer.Database.HasTasks)
				{
					Console.Write(".");
					Thread.Sleep(50);
				}
				Console.WriteLine();
				Console.WriteLine("Finishing indexing took: {0}", indexing.Elapsed);
			}

			Console.WriteLine("Total execution time {0}", sp.Elapsed);
		}

		private static void LoadDataFor(string searchPattern)
		{
			var durations = new List<TimeSpan>();
			foreach (var file in Directory.GetFiles(@"C:\Work\ravendb\ETL\Raven.StackOverflow.Etl\bin\Debug\Docs", searchPattern).OrderBy(x => x))
			{
				var sp = Stopwatch.StartNew();
				var httpWebRequest = (HttpWebRequest)WebRequest.Create("http://localhost:8080/bulk_docs");
				httpWebRequest.Method = "POST";
				using(var requestStream = httpWebRequest.GetRequestStream())
				{
					var readAllBytes = File.ReadAllBytes(file);
					requestStream.Write(readAllBytes, 0, readAllBytes.Length);
				}
				HttpWebResponse webResponse;
				try
				{
					webResponse = (HttpWebResponse)httpWebRequest.GetResponse();
				}
				catch (WebException e)
				{
					Console.WriteLine(new StreamReader(e.Response.GetResponseStream()).ReadToEnd());
					Environment.Exit(1);
					return;
				}
				var timeSpan = sp.Elapsed;
				durations.Add(timeSpan);
				Console.WriteLine("{0} - {1} - {2}", Path.GetFileName(file), timeSpan, webResponse.StatusCode);
				webResponse.Close();
			}
			Console.WriteLine("For {0} took avg: {1}ms", searchPattern, durations.Average(x => x.TotalMilliseconds));
		}

		private static void GenerateDocumentsToFile(string path)
		{
			if (Directory.Exists("Docs"))
				Directory.Delete("Docs", true);
			Directory.CreateDirectory("Docs");


			Execute(new UsersProcess(path));
			Execute(new BadgesProcess(path));
			Execute(new PostsProcess(path));
			Execute(new VotesProcess(path));
			Execute(new CommentsProcess(path));
		}

		private static void WaitForIndexingToComplete(DocumentDatabase documentDatabase)
		{
			Console.WriteLine("Waiting for indexing to complete");
			var sp2 = Stopwatch.StartNew();
			while(documentDatabase.HasTasks)
			{
				documentDatabase.TransactionalStorage.Batch(actions =>
				{
					var indexesStat = actions.GetIndexesStats().First();
					Console.WriteLine("{0} - {1:#,#} - {2:#,#} - {3}", indexesStat.Name, 
						indexesStat.IndexingSuccesses, 
						actions.GetDocumentsCount(),
						sp2.Elapsed);

					actions.Commit();
				});

				Thread.Sleep(1000);
			}
		}

		private static void Execute(EtlProcess process)
		{
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
