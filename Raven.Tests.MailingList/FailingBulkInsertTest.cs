using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class FailingBulkInsertTest : RavenTestBase
	{
		[Fact]
		public void CanBulkInsert()
		{
			var bulkInsertSize = 20000;
			using (var store = NewDocumentStore(requestedStorage:"esent"))
			{
				new SampleData_Index().Execute(store);
				using (var bulkInsert = store.BulkInsert())
				{
					for (int i = 0; i < bulkInsertSize; i++)
					{
						bulkInsert.Store(new SampleData
						{
							Name = "New Data" + i
						});
					}
				}
				Assert.Equal(bulkInsertSize + 1, store.DatabaseCommands.GetStatistics().CountOfDocuments);
				using (var session = store.OpenSession())
				{
					var result = session.Query<SampleData, SampleData_Index>()
										.Customize(customization => customization.WaitForNonStaleResults(TimeSpan.FromMinutes(5)))
										.Count();
					Assert.Equal(bulkInsertSize, result);
				}
			}
		}

		[Fact]
		public void CanBulkInsertConcurrently()
		{
			var bulkInsertSize = 5000;
			using (var store = NewDocumentStore(requestedStorage: "esent"))
			{
				new SampleData_Index().Execute(store);

				var t1 = Task.Factory.StartNew(() =>
				{
					using (var bulkInsert = store.BulkInsert())
					{
						for (int i = 0; i < bulkInsertSize/2; i++)
						{
							bulkInsert.Store(new SampleData
							{
								Name = "New Data" + i
							});
						}
					}
				});

				var t2 = Task.Factory.StartNew(() =>
				{
					using (var bulkInsert = store.BulkInsert())
					{
						for (int i = bulkInsertSize / 2; i < bulkInsertSize; i++)
						{
							bulkInsert.Store(new SampleData
							{
								Name = "New Data" + i
							});
						}
					}
				});

				int insertsFromSession = 0;
				var t3 = Task.Factory.StartNew(() =>
				{
					var index = bulkInsertSize;
					for (int i = 0; i < 20; i++)
					{
						using (var session = store.OpenSession())
						{
							for (int j = 0; j < 50; j++)
							{
								session.Store(new SampleData
								{
									Name = "New Data" + index
								});
								index++;
								insertsFromSession++;
							}
							session.SaveChanges();
						}
					}
				});

				Task.WaitAll(t1, t2, t3);


				var totalInsertSize = bulkInsertSize + insertsFromSession;

				Assert.Equal(totalInsertSize + 1, store.DatabaseCommands.GetStatistics().CountOfDocuments);
				using (var session = store.OpenSession())
				{
					var result = session.Query<SampleData, SampleData_Index>()
										.Customize(customization => customization.WaitForNonStaleResults(TimeSpan.FromMinutes(5)))
										.Count();
					Assert.Equal(totalInsertSize, result);
				}
			}
		}

		[Fact]
		public void CanBulkInsert_LowLevel()
		{
			using (var store = NewDocumentStore(requestedStorage:"esent"))
			{
				store.DocumentDatabase.Documents.BulkInsert(new BulkInsertOptions(), YieldDocumentBatch(store), Guid.NewGuid());

				WaitForIndexing(store);

                var queryResultWithIncludes = store.DocumentDatabase.Queries.Query("Raven/DocumentsByEntityName", new IndexQuery(), CancellationToken.None);

				Assert.Equal(12, queryResultWithIncludes.TotalResults);
			}
		}

		private IEnumerable<IEnumerable<JsonDocument>> YieldDocumentBatch(EmbeddableDocumentStore store)
		{
			for (int i = 0; i < 3; i++)
			{
				Task.Factory.StartNew(() =>
				{
					store.DocumentDatabase.Documents.Put("test/" + i, null, new RavenJObject(), new RavenJObject { { "Raven-Entity-Name", "Test" } }, null);
				}).Wait();

				yield return YieldDocuments(i);

				Task.Factory.StartNew(() =>
				{
					store.DocumentDatabase.Documents.Put("test/" + i, null, new RavenJObject(), new RavenJObject { { "Raven-Entity-Name", "Test" } }, null);
				}).Wait();

				// note this is called inside bulk insert batch - make sure that this will be run in a separate thread to avoid batch nesting 
				// what would reuse an esent session and return invalid information about index staleness
				Task.Factory.StartNew(() => WaitForIndexing(store), TaskCreationOptions.LongRunning).Wait(); 
			}
		}

		private IEnumerable<JsonDocument> YieldDocuments(int b)
		{
			for (int i = 0; i < 3; i++)
			{
				yield return new JsonDocument
				{
					DataAsJson = new RavenJObject { { "Name", "Test" } },
					Key = "sample/" + b + "/" + i,
					Metadata = { { "Raven-Entity-Name", "SampleDatas" } },
				};
			}
		}

		public class SampleData
		{
			public string Name { get; set; }
		}

		public class SampleData_Index : AbstractIndexCreationTask<SampleData>
		{
			public SampleData_Index()
			{
				Map = docs => from doc in docs
							  select new
								  {
									  doc.Name
								  };
			}
		}
	}
}

