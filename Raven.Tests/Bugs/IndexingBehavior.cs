using System;
using System.Threading;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Exceptions;
using Raven.Database.Indexing;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class IndexingBehavior : AbstractDocumentStorageTest 
	{
		private readonly DocumentDatabase db;

		public IndexingBehavior()
		{
			db =
				new DocumentDatabase(new RavenConfiguration
				{DataDirectory = "raven.db.test.esent", RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true});
		}

		public override void Dispose()
		{
			db.Dispose();
			base.Dispose();
		}

		[Fact]
		public void CanDeleteIndex()
		{
			db.PutIndex("test", new IndexDefinition
			{
				Map = "from doc in docs select new { doc.Name}"
			});

			db.DeleteIndex("test");
			Assert.Null(db.GetIndexDefinition("test"));
		}


		[Fact]
		public void CanGetIndexingErrorsInStats()
		{
			db.PutIndex("test", new IndexDefinition
			{
				Map = "from doc in docs select new { doc.User.Name }"
			});

			for (int i = 0; i < 15; i++)
			{
				db.Put("a" + i, null, new JObject(), new JObject(), null);
			}

			Assert.Empty(db.Statistics.Errors); 

			db.SpinBackgroundWorkers();

			for (int i = 0; i < 50; i++)
			{
				bool isIndexStale = false;
				db.TransactionalStorage.Batch(actions =>
				{
					isIndexStale = actions.Tasks.IsIndexStale("test", null);
				});
				if (isIndexStale == false)
					break;
				Thread.Sleep(100);
			}

			Assert.NotEmpty(db.Statistics.Errors);
		}

		[Fact]
		public void AfterEnoughFailuresIndexWillBeDisabled()
		{
			db.PutIndex("test", new IndexDefinition
			{
				Map = "from doc in docs select new { doc.User.Name }"
			});

			for (int i = 0; i < 15; i++)
			{
				db.Put("a"+i, null, new JObject(), new JObject(),null);
			}

			db.SpinBackgroundWorkers();

			for (int i = 0; i < 50; i++)
			{
				bool isIndexStale = false;
				db.TransactionalStorage.Batch(actions =>
				{
					isIndexStale = actions.Tasks.IsIndexStale("test",null);
				});
				if (isIndexStale == false)
					break;
				Thread.Sleep(100);
			}

			Assert.Throws<IndexDisabledException>(() => db.Query("test", new IndexQuery { Query = "Name:Ayende" }));
		}
	}
}