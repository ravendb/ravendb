using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Xml;
using NLog;
using NLog.Config;
using NLog.Layouts;
using NLog.Targets;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Indexes;
using System.Linq;
using Raven.Database.Server;
using Raven.Storage.Managed;
using Raven.Tests.Bugs.MultiMap;

namespace Raven.Tryouts
{
	internal class Program
	{
		private static void Main()
		{
			if (Directory.Exists("Logs"))
			{
				foreach (var file in Directory.GetFiles("Logs"))
				{
					File.Delete(file);
				}
			}
			SetupLogging();

			try
			{
				for (int i = 0; i < 1000; i++)
				{
					Environment.SetEnvironmentVariable("Run", i.ToString());
					Console.Clear();
					Console.WriteLine(i);
					new MultiMapReduce().JustQuerying();
				}
			}
			finally
			{
				LogManager.Flush();
			}
			return;

			using (var store = new DocumentStore
			{
				Url = "http://localhost:8080"
			}.Initialize())
			{
				store.DatabaseCommands.PutIndex("Disks/Search", new IndexDefinition
				{
					Map =
						@"
from disk in docs.Disks 
select new 
{ 
	Query = new[] { disk.Artist, disk.Title },
	disk.Tracks,
	DisId = disk.DiskIds
}",
					Indexes =
						{
							{"Query", FieldIndexing.Analyzed},
							{"Tracks", FieldIndexing.Analyzed}
						}
				});



				store.DatabaseCommands.PutIndex("Disks/Simple", new IndexDefinition
								{
									Map =
										@"
from disk in docs.Disks 
select new 
{ 
    disk.Artist,
    disk.Title
}"
								});

				new RavenDocumentsByEntityName().Execute(store);

				var sp = Stopwatch.StartNew();
				while (true)
				{
					var statistics = store.DatabaseCommands.GetStatistics();
					if (statistics.StaleIndexes.Length == 0)
						break;

					Console.Clear();
					foreach (var stat in statistics.Indexes.Where(x => statistics.StaleIndexes.Contains(x.Name)))
					{
						Console.WriteLine("{0}: {1:#,#}  ", stat.Name, stat.IndexingAttempts);
					}

					Console.WriteLine("{0:#,#}",statistics.CurrentNumberOfItemsToIndexInSingleBatch);
					Console.Write(sp.Elapsed);
					Thread.Sleep(2500);
				}

				Console.WriteLine(sp.Elapsed);
			}
		}

		private static void SetupLogging()
		{
			HttpEndpointRegistration.RegisterHttpEndpointTarget();

			using (var stream = typeof(Program).Assembly.GetManifestResourceStream("Raven.Tryouts.DefaultLogging.config"))
			using (var reader = XmlReader.Create(stream))
			{
				LogManager.Configuration = new XmlLoggingConfiguration(reader, "default-config");
			}

		}
	}
}