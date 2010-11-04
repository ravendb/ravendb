using System;
using System.Threading;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Raven.Tests.Storage;
using Xunit;
using System.Linq;

namespace Raven.Tests.Indexes
{
	public class Statistics : AbstractDocumentStorageTest
	{
		private readonly DocumentDatabase db;

		public Statistics()
		{
			db = new DocumentDatabase(new RavenConfiguration {DataDirectory = "raven.db.test.esent", RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true});
			db.SpinBackgroundWorkers();

			db.PutIndex("pagesByTitle2",
			            new IndexDefinition
			            {
							Map = @"
                    from doc in docs
                    where doc.type == ""page""
                    select new {  f = 2 / doc.size };
                "
			            });
		}

		#region IDisposable Members

		public override void Dispose()
		{
			db.Dispose();
			base.Dispose();
		}

		#endregion

		[Fact]
		public void Can_get_stats_for_indexing_without_any_indexing()
		{
			Assert.Equal(1, db.Statistics.Indexes.Count(x=>x.Name.StartsWith("Raven") == false));
			Assert.True(db.Statistics.Indexes.Any(x => x.Name == "pagesByTitle2"));
			Assert.Equal(0, db.Statistics.Indexes.First((x => x.Name == "pagesByTitle2")).IndexingAttempts);
		}

		[Fact]
		public void Can_get_stats_for_indexing()
		{
			db.Put("1", Guid.Empty,
			       JObject.Parse(
			       	@"{
                type: 'page', 
                some: 'val', 
                other: 'var', 
                content: 'this is the content', 
                title: 'hello world', 
                size: 1,
                '@metadata': {'@id': 1}
            }"),
                   new JObject(), null);

			QueryResult docs;
			do
			{
				docs = db.Query("pagesByTitle2", new IndexQuery
				{
					Query = "f:val",
					Start = 0,
					PageSize = 10
				});
				if (docs.IsStale)
					Thread.Sleep(100);
			} while (docs.IsStale);

		    var indexStats = db.Statistics.Indexes.First(x=>x.Name == "pagesByTitle2");
		    Assert.Equal("pagesByTitle2", indexStats.Name);
			Assert.Equal(1, indexStats.IndexingAttempts);
			Assert.Equal(1, indexStats.IndexingSuccesses);
		}

		[Fact]
		public void Can_get_stats_for_indexing_including_errors()
		{
			db.Put("1", Guid.Empty,
			       JObject.Parse(
			       	@"{
                type: 'page', 
                some: 'val', 
                other: 'var', 
                content: 'this is the content', 
                title: 'hello world', 
                size: 0,
                '@metadata': {'@id': 1}
            }"),
                   new JObject(), null);
			db.Put("2", Guid.Empty,
			       JObject.Parse(
			       	@"{
                type: 'page', 
                some: 'val', 
                other: 'var', 
                content: 'this is the content', 
                title: 'hello world', 
                size: 1,
                '@metadata': {'@id': 1}
            }"),
                   new JObject(), null);

			QueryResult docs;
			do
			{
				docs = db.Query("pagesByTitle2", new IndexQuery
				{
					Query = "f:val",
					Start = 0,
					PageSize = 10
				});
				if (docs.IsStale)
					Thread.Sleep(100);
			} while (docs.IsStale);

		    var indexStats = db.Statistics.Indexes.First(x=>x.Name == "pagesByTitle2");
		    Assert.Equal("pagesByTitle2", indexStats.Name);
			Assert.Equal(2, indexStats.IndexingAttempts);
			Assert.Equal(1, indexStats.IndexingErrors);
			Assert.Equal(1, indexStats.IndexingSuccesses);
		}

		[Fact]
		public void Can_get_details_about_indexing_errors()
		{
			db.Put("1", Guid.Empty,
			       JObject.Parse(
			       	@"{
                type: 'page', 
                some: 'val', 
                other: 'var', 
                content: 'this is the content', 
                title: 'hello world', 
                size: 0,
                '@metadata': {'@id': 1}
            }"),
                   new JObject(), null);

			QueryResult docs;
			do
			{
				docs = db.Query("pagesByTitle2", new IndexQuery
				{
					Query = "f:val",
					Start = 0,
					PageSize = 10
				});
				if (docs.IsStale)
					Thread.Sleep(100);
			} while (docs.IsStale);

			Assert.Equal("1", db.Statistics.Errors[0].Document);
			Assert.Equal("pagesByTitle2", db.Statistics.Errors[0].Index);
			Assert.Contains("Attempted to divide by zero.", db.Statistics.Errors[0].Error);
		}
	}
}
