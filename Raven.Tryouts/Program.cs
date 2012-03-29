using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Xml;
using NLog;
using NLog.Config;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Database.Extensions;
using Raven.Database.Server;

namespace etobi.CrashTest2
{
	class Program
	{
		static void Main(string[] args)
		{
			if (Directory.Exists("Logs"))
			{
			}
			SetupLogging();
			if(Directory.Exists("Data"))
			{
				Environment.SetEnvironmentVariable("Run", "Second");
			}
			else
			{
				Environment.SetEnvironmentVariable("Run", "First");
			}
			MakeBackupOfDataDirectory();

			using (var documentStore = new EmbeddableDocumentStore
			{
				Configuration =
				{
					DataDirectory = "Data",
					//ResetIndexOnUncleanShutdown = true
				},
			})
			{
				documentStore.Initialize();
				CreateIndexes(documentStore);
				Console.WriteLine("Started...");

				// don't count hilo !
				var expectedNumberOfDocs = documentStore.DocumentDatabase.Statistics.CountOfDocuments - 1;
				if (expectedNumberOfDocs < 0) expectedNumberOfDocs = 0;
				Console.WriteLine("Expecting {0} docs", expectedNumberOfDocs);
				
				while (documentStore.DocumentDatabase.Statistics.StaleIndexes.Length > 0)
				{
					Console.WriteLine("Waiting for stale results...");
					Thread.Sleep(1000);
				}

				var haveError = false;

				// Now check the number of docs returned by the indexes
				using (var s = documentStore.OpenSession())
				{
					foreach (var index in documentStore.DocumentDatabase.Statistics.Indexes)
					{
						RavenQueryStatistics stats;
						s.Query<Foo>(index.Name).Statistics(out stats).Customize(x => x.WaitForNonStaleResults()).FirstOrDefault();
						if (expectedNumberOfDocs != stats.TotalResults)
						{
							haveError = true;
							Console.WriteLine("Index {0} is missing documents (expected={1} actual={2}", index.Name, expectedNumberOfDocs, stats.TotalResults);
						}
					}
					
				}

				if (haveError)
				{
					Console.ReadLine();
					return;
				}

				// Now create lots of docs!
				//for (var x = 0; x < 20; x++ )
				int count = 0;
				while (true)
				{
					count++;
					using (var s = documentStore.OpenSession())
					{
						for (int i = 0; i < 1024; i++)
						{
							s.Store(new Foo { Property = Guid.NewGuid().ToString() });
						}
						Console.WriteLine("Inserting new docs..");
						s.SaveChanges();
					}
					if(count == 10)
					{
						Thread.Sleep(100);// give it time to do indexing
						Process.GetCurrentProcess().Kill();
					}
					//while (documentStore.DocumentDatabase.Statistics.StaleIndexes.Length > 0)
					//{
					//    Console.WriteLine("Waiting for stale results...");
					//    Thread.Sleep(1000);
					//}
				}
			}
		}

		private static void CreateIndexes(EmbeddableDocumentStore documentStore)
		{
			documentStore.DocumentDatabase.PutIndex(new RavenDocumentsByEntityName().IndexName,
				new RavenDocumentsByEntityName().CreateIndexDefinition());
			for (int i = 0; i < 10; i++)
			{
				documentStore.DatabaseCommands.PutIndex("index_" + i, new IndexDefinitionBuilder<Foo>
				{
					Map = docs =>
						  from doc in docs
						  select new
						  {
							  doc.Property,
						  }
				}, true);
			}
		}

		private static void MakeBackupOfDataDirectory()
		{
			if (Directory.Exists("Data.after_power_failure"))
			{
				IOExtensions.DeleteDirectory("Data.after_power_failure");
			}

			if (Directory.Exists("Data"))
			{
				new Microsoft.VisualBasic.Devices.Computer().FileSystem.CopyDirectory("Data", "Data.after_power_failure");
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

	public class Foo
	{
		public string Id { get; set; }
		public string Property { get; set; }
	}
}