using System;
using Raven.Abstractions.Indexing;
using Raven.Database;
using Raven.Database.Config;
using Xunit;
using System.Linq;

namespace Raven.Tests.Storage
{
	public class SimilarIndexNames : RavenTest
	{
		private readonly DocumentDatabase db;

		public SimilarIndexNames()
		{
			db = new DocumentDatabase(new RavenConfiguration
			{
				RunInMemory= true
			});
		}


		public override void Dispose()
		{
			db.Dispose();
			base.Dispose();
		}

		[Fact]
		public void Index_with_similar_names_update_first()
		{
			db.PutIndex("Leases/SearchIndex",
						new IndexDefinition
						{
							Map = @"
	from doc in docs
	where doc.type == ""page""
	select new { Key = doc.title, Value = doc.content, Size = doc.size };
"
						});

			db.PutIndex("Leases/SearchIndex2",
						new IndexDefinition
						{
							Map = @"
	from doc in docs
	where doc.type == ""page""
	select new { Key = doc.title, Value = doc.content, Size = doc.size };
"
						});

			var one = new Guid(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1);
			var time = DateTime.Today;


			db.TransactionalStorage.Batch(accessor => accessor.Indexing.UpdateLastIndexed("Leases/SearchIndex", one, time));

			db.TransactionalStorage.Batch(accessor =>
			{
				var stats = accessor.Indexing.GetIndexesStats().Where(x => x.Name == "Leases/SearchIndex").First();

				Assert.Equal(one, stats.LastIndexedEtag);

				Assert.Equal(time, stats.LastIndexedTimestamp);

				stats = accessor.Indexing.GetIndexesStats().Where(x => x.Name == "Leases/SearchIndex2").First();

				Assert.Equal(Guid.Empty, stats.LastIndexedEtag);

				Assert.Equal(DateTime.MinValue, stats.LastIndexedTimestamp);
			});
		}

		[Fact]
		public void Index_with_similar_names_update_second()
		{
			db.PutIndex("Leases/SearchIndex",
						new IndexDefinition
						{
							Map = @"
	from doc in docs
	where doc.type == ""page""
	select new { Key = doc.title, Value = doc.content, Size = doc.size };
"
						});

			db.PutIndex("Leases/SearchIndex2",
						new IndexDefinition
						{
							Map = @"
	from doc in docs
	where doc.type == ""page""
	select new { Key = doc.title, Value = doc.content, Size = doc.size };
"
						});

			var one = new Guid(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1);
			var time = DateTime.Today;


			db.TransactionalStorage.Batch(accessor => accessor.Indexing.UpdateLastIndexed("Leases/SearchIndex2", one, time));

			db.TransactionalStorage.Batch(accessor =>
			{
				var stats = accessor.Indexing.GetIndexesStats().Where(x => x.Name == "Leases/SearchIndex2").First();

				Assert.Equal(one, stats.LastIndexedEtag);

				Assert.Equal(time, stats.LastIndexedTimestamp);

				stats = accessor.Indexing.GetIndexesStats().Where(x => x.Name == "Leases/SearchIndex").First();

				Assert.Equal(Guid.Empty, stats.LastIndexedEtag);

				Assert.Equal(DateTime.MinValue, stats.LastIndexedTimestamp);
			});
		}
	}
}
