using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using log4net.Appender;
using log4net.Config;
using log4net.Core;
using log4net.Layout;
using Newtonsoft.Json.Linq;
using Raven.Database;
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

			if (Directory.Exists("Data"))
				Directory.Delete("Data", true);

			Console.WriteLine("Starting...");
			var sp = Stopwatch.StartNew();
			using (var documentDatabase = new DocumentDatabase(new RavenConfiguration
			{
				DataDirectory = "Data",
			}))
			{
				Execute(new UsersProcess(path, documentDatabase));
				Execute(new BadgesProcess(path, documentDatabase));
				Execute(new PostsProcess(path, documentDatabase));
				Execute(new VotesProcess(path, documentDatabase));
				Execute(new CommentsProcess(path, documentDatabase));
			}
			Console.WriteLine("Total execution time {0}", sp.Elapsed);

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
				Debugger.Launch();
				throw new InvalidOperationException("Failed to execute process: " + process);
			}
		}
	}
}
