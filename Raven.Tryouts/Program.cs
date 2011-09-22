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

namespace etobi.EmbeddedTest
{
	namespace etobi.EmbeddedTest
	{
		class Program
		{
			private static readonly Logger Log = LogManager.GetCurrentClassLogger();
			private static bool _stop;
			private static readonly Random Random = new Random();
			private static ConcurrentBag<Exception> _exceptions = new ConcurrentBag<Exception>();

			static void Main()
			{
				try
				{
					IOExtensions.DeleteDirectory("Data");
					using (var documentStore = new EmbeddableDocumentStore())
					{
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
							Console.WriteLine("NumberOfDocs={0}, StaleIndexes={1}",
								documentStore.DocumentDatabase.Statistics.CountOfDocuments,
								documentStore.DocumentDatabase.Statistics.StaleIndexes.Count());
						}

						foreach (var thread in threads)
						{
							thread.Join();
						}

						foreach (var exception in _exceptions)
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
					_exceptions.Add(ex);
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

}
