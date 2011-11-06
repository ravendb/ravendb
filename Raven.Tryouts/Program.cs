using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Database.Extensions;
using NLog;
using Raven.Tryouts.Json;

namespace etobi.EmbeddedTest
{
	class Program
	{
		private static readonly Logger Log = LogManager.GetCurrentClassLogger();
		private static bool _stop;
		private static readonly Random Random = new Random();
		private static readonly ConcurrentBag<Exception> Exceptions = new ConcurrentBag<Exception>();

		static void Main()
		{
			PerfTest.RunPerfTest();
		    return;
            try
			{
				IOExtensions.DeleteDirectory("Data");
				using (var documentStore = new EmbeddableDocumentStore())
				{
					documentStore.Configuration.MemoryCacheLimitPercentage = 5;
					documentStore.Configuration.MemoryCacheLimitCheckInterval = TimeSpan.FromSeconds(10);
					documentStore.Configuration.MemoryCacheLimitMegabytes = 32;
					documentStore.Configuration.Settings["Raven/Esent/CacheSizeMax"] = "32";
					documentStore.Configuration.Settings["Raven/Esent/MaxVerPages"] = "64";
					documentStore.Configuration.MaxNumberOfItemsToIndexInSingleBatch = 128;

					var threads = new List<Thread>();

					InitDatabase(documentStore);
					for (var i = 0; i < 10; i++)
					{
						var thread = new Thread(x => InsertAndQueryLoop(documentStore));
						thread.Start();
						threads.Add(thread);
					}

					while (!_stop)
					{
						Thread.Sleep(5000);
						GC.Collect(2);
						GC.WaitForPendingFinalizers();
						
						Console.WriteLine("NumberOfDocs={0}, StaleIndexes={1}, Memory={2:#,#} kb",
							documentStore.DocumentDatabase.Statistics.CountOfDocuments,
							documentStore.DocumentDatabase.Statistics.StaleIndexes.Count(),
							GC.GetTotalMemory(false) / 1024);
					}

					foreach (var thread in threads)
					{
						thread.Join();
					}

					foreach (var exception in Exceptions)
					{
						Console.WriteLine(exception);
					}
				}
			}
			catch (Exception ex)
			{
				Log.ErrorException("Bad things have happened!", ex);
				throw;
			}
			Console.WriteLine("Program stopped");
			Console.ReadLine();
		}

		private static void InsertAndQueryLoop(IDocumentStore documentStore)
		{
			try
			{
				while (!_stop)
				{
					var dataToQueryFor = new List<string>();

					for (var i = 0; i < 10; i++)
					{
						using (var session = documentStore.OpenSession())
						{
							session.Query<Foo>("index" + Random.Next(0, 5))
								.Where(x => x.Data == "dont care")
								.FirstOrDefault();

							var foo = new Foo { Id = Guid.NewGuid().ToString(), Data = Guid.NewGuid().ToString() };
							dataToQueryFor.Add(foo.Data);
							session.Store(foo);
							session.SaveChanges();
						}
					}

					using (var session = documentStore.OpenSession())
					{
						session.Query<Foo>("index" + Random.Next(0, 5))
							.Customize(x => x.WaitForNonStaleResults(TimeSpan.MaxValue))
							.Where(x => x.Data == dataToQueryFor[Random.Next(0, 10)])
							.FirstOrDefault();
					}
					Thread.Sleep(Random.Next(50, 200));
				}
			}
			catch (Exception ex)
			{
				Exceptions.Add(ex);
				Log.ErrorException("Houston, we have a problem", ex);
				_stop = true;
			}
		}

		private static void InitDatabase(EmbeddableDocumentStore documentStore)
		{
			documentStore.Configuration.DataDirectory = "Data";
			documentStore.Configuration.DefaultStorageTypeName = "esent";
			documentStore.Initialize();

			new RavenDocumentsByEntityName().Execute(documentStore);

			for (var i = 0; i < 5; i++)
			{
				documentStore.DatabaseCommands.PutIndex("index" + i,
					new IndexDefinitionBuilder<Foo>
					{
						Map = docs => from doc in docs select new { doc.Data }
					});
			}
		}
	}

	public class Foo
	{
		public string Id { get; set; }
		public string Data { get; set; }
	}
	
}
