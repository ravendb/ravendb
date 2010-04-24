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
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Server;
using Raven.StackOverflow.Etl.Generic;
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

			if (Directory.Exists("Data"))
				Directory.Delete("Data", true);

			using (new RavenDbServer(new RavenConfiguration
			{
				DataDirectory = "Data",
				Port = 8080,
				AnonymousUserAccessMode = AnonymousUserAccessMode.All
			}))
			{
				LoadDataFor("Users*.json");
				//LoadDataFor("Badges*.json");
				//LoadDataFor("Posts*.json");
				//LoadDataFor("Votes*.json");
				//LoadDataFor("Comments*.json");
			}

			Console.WriteLine("Total execution time {0}", sp.Elapsed);
		}

		private static void LoadDataFor(string searchPattern)
		{
			var durations = new List<TimeSpan>();
			foreach (var file in Directory.GetFiles("Docs", searchPattern).OrderBy(x=>x))
			{
				var sp = Stopwatch.StartNew();
				var httpWebRequest = (HttpWebRequest)WebRequest.Create("http://localhost:8080/bulk_docs");
				httpWebRequest.Method = "POST";
				using(var requestStream = httpWebRequest.GetRequestStream())
				{
					var readAllBytes = File.ReadAllBytes(file);
					requestStream.Write(readAllBytes, 0, readAllBytes.Length);
				}
				var webResponse = (HttpWebResponse)httpWebRequest.GetResponse();
				var timeSpan = sp.Elapsed;
				durations.Add(timeSpan);
				Console.WriteLine("{0} - {1} - {2}", Path.GetFileName(file), timeSpan, webResponse.StatusCode);
			}
			Console.WriteLine("For {0} took avg: {1}ms", searchPattern, durations.Average(x => x.TotalMilliseconds));
		}

		private static void GenerateDocumentsToFile(string path)
		{
			if (Directory.Exists("Docs"))
				Directory.Delete("Docs", true);
			Directory.CreateDirectory("Docs");


			Execute(new UsersProcess(path));
			//Execute(new BadgesProcess(path));
			//Execute(new PostsProcess(path));
			//Execute(new VotesProcess(path));
			//Execute(new CommentsProcess(path));
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
