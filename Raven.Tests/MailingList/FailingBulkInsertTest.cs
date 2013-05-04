using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Tasks;
using Raven.Json.Linq;
using Raven.Tests.Helpers;
using Xunit;
using Task = System.Threading.Tasks.Task;

namespace Raven.Tests.MailingList
{
	public class FailingBulkInsertTest : RavenTestBase
	{
		[Fact]
		public void CanBulkInsert()
		{
			var bulkInsertSize = 20000;
			using (var store = NewDocumentStore())
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
										.Customize(customization => customization.WaitForNonStaleResults())
										.Count();
					Assert.Equal(bulkInsertSize, result);
				}
			}
		}

		[Fact]
		public void CanBulkInsert_LowLevel()
		{
			using (var store = NewDocumentStore())
			{
				store.DocumentDatabase.BulkInsert(new BulkInsertOptions(), YieldDocumentBatch(store));

				WaitForIndexing(store);

				var queryResultWithIncludes = store.DocumentDatabase.Query("Raven/DocumentsByEntityName", new IndexQuery());

				Assert.Equal(15, queryResultWithIncludes.TotalResults);
			}
		}

		private IEnumerable<IEnumerable<JsonDocument>> YieldDocumentBatch(EmbeddableDocumentStore store)
		{
			for (int i = 0; i < 3; i++)
			{
				Task.Factory.StartNew(() =>
				{
					store.DocumentDatabase.Put("test/" + i, null, new RavenJObject(), new RavenJObject { { "Raven-Entity-Name", "Test" } }, null);
				}).Wait();

				yield return YieldDocuments(i);

				Task.Factory.StartNew(() =>
				{
					store.DocumentDatabase.Put("test/" + i, null, new RavenJObject(), new RavenJObject { { "Raven-Entity-Name", "Test" } }, null);
				}).Wait();
				WaitForIndexing(store);
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

