//-----------------------------------------------------------------------
// <copyright file="IndexingBehavior.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Embedded;
using Raven.Json.Linq;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Exceptions;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class IndexingBehavior : RavenTest 
	{
		private readonly EmbeddableDocumentStore store;
		private readonly DocumentDatabase db;

		public IndexingBehavior()
		{
			store = NewDocumentStore();
			db = store.DocumentDatabase;
			db.PutIndex(new RavenDocumentsByEntityName().IndexName, new RavenDocumentsByEntityName().CreateIndexDefinition());
		}

		public override void Dispose()
		{
			store.Dispose();
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
				Map = "from doc in docs select new { User = ((string)null).ToString() }"
			});

			for (int i = 0; i < 15; i++)
			{
				db.Put("a" + i, null, new RavenJObject(), new RavenJObject(), null);
			}

			bool isIndexStale = false;
			for (int i = 0; i < 50; i++)
			{
				db.TransactionalStorage.Batch(actions =>
				{
					isIndexStale = actions.Staleness.IsIndexStale("test", null, null);
				});
				if (isIndexStale == false)
					break;
				Thread.Sleep(100);
			}
			Assert.False(isIndexStale);
			Assert.NotEmpty(db.Statistics.Errors);
		}

		[Fact]
		public void AfterEnoughFailuresIndexWillBeDisabled()
		{
			db.PutIndex("test", new IndexDefinition
			{
				Map = "from doc in docs select new { User = ((string)null).ToString() }"
			});

			for (int i = 0; i < 150; i++)
			{
				db.Put("a"+i, null, new RavenJObject(), new RavenJObject(),null);
			}

			for (int i = 0; i < 50; i++)
			{
				bool isIndexStale = false;
				db.TransactionalStorage.Batch(actions =>
				{
					isIndexStale = actions.Staleness.IsIndexStale("test", null, null);
				});
				if (isIndexStale == false)
					break;
				Thread.Sleep(100);
			}

			Assert.Throws<IndexDisabledException>(() =>
			{
				var queryResult = db.Query("test", new IndexQuery { Query = "User:Ayende" });
			});
		}

		[Fact]
		public void AfterDeletingAndStoringTheDocumentIsIndexed()
		{
			db.PutIndex(@"DocsByProject", new IndexDefinition
			{
				Map = @"from doc in docs select new{ doc.Something}"
			});

			db.Put("foos/1", null, RavenJObject.Parse("{'Something':'something'}"),
			  RavenJObject.Parse("{'Raven-Entity-Name': 'Foos'}"), null);

			var document = db.Get("foos/1", null);
			db.Delete("foos/1", document.Etag, null);

			db.Put("foos/1", null, RavenJObject.Parse("{'Something':'something'}"),
			RavenJObject.Parse("{'Raven-Entity-Name': 'Foos'}"), null);

			QueryResult queryResult;
			do
			{
				queryResult = db.Query("Raven/DocumentsByEntityName", new IndexQuery
				{
					Query = "Tag:[[Foos]]",
					PageSize = 10
				});
			} while (queryResult.IsStale);

			Assert.Equal(1, queryResult.TotalResults);
		}
	}
}
