using System;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Database;
using Raven.Database.Config;
using Raven.Tests.Common;

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
			}, null);
		}


		public override void Dispose()
		{
			db.Dispose();
			base.Dispose();
		}

		[Fact]
		public void Index_with_similar_names_update_first()
		{
			db.Indexes.PutIndex("Leases/SearchIndex",
						new IndexDefinition
						{
							Map = @"
	from doc in docs
	where doc.type == ""page""
	select new { Key = doc.title, Value = doc.content, Size = doc.size };
"
						});

			db.Indexes.PutIndex("Leases/SearchIndex2",
						new IndexDefinition
						{
							Map = @"
	from doc in docs
	where doc.type == ""page""
	select new { Key = doc.title, Value = doc.content, Size = doc.size };
"
						});
		    int searchIndex1 = db.IndexDefinitionStorage.GetIndexDefinition("Leases/SearchIndex").IndexId;
		    int searchIndex2 = db.IndexDefinitionStorage.GetIndexDefinition("Leases/SearchIndex2").IndexId;

			var one = Etag.Parse(new Guid(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1).ToByteArray());
			var time = DateTime.Today;


			db.TransactionalStorage.Batch(accessor => accessor.Indexing.UpdateLastIndexed(searchIndex1, one, time));

			db.TransactionalStorage.Batch(accessor =>
			{
				var stats = accessor.Indexing.GetIndexesStats().Where(x => x.Id ==searchIndex1).First();

				Assert.Equal(one, stats.LastIndexedEtag);

				Assert.Equal(time, stats.LastIndexedTimestamp);

				stats = accessor.Indexing.GetIndexesStats().Where(x => x.Id == searchIndex2).First();

				Assert.Equal(Etag.Empty, stats.LastIndexedEtag);

				Assert.Equal(DateTime.MinValue, stats.LastIndexedTimestamp);
			});
		}

		[Fact]
		public void Index_with_similar_names_update_second()
		{
			db.Indexes.PutIndex("Leases/SearchIndex",
						new IndexDefinition
						{
							Map = @"
	from doc in docs
	where doc.type == ""page""
	select new { Key = doc.title, Value = doc.content, Size = doc.size };
"
						});

			db.Indexes.PutIndex("Leases/SearchIndex2",
						new IndexDefinition
						{
							Map = @"
	from doc in docs
	where doc.type == ""page""
	select new { Key = doc.title, Value = doc.content, Size = doc.size };
"
						});

		    int searchIndex1 = db.IndexDefinitionStorage.GetIndexDefinition("Leases/SearchIndex").IndexId;
		    int searchIndex2 = db.IndexDefinitionStorage.GetIndexDefinition("Leases/SearchIndex2").IndexId;

			var one = Etag.Parse(new Guid(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1).ToByteArray());
			var time = DateTime.Today;


			db.TransactionalStorage.Batch(accessor => accessor.Indexing.UpdateLastIndexed(searchIndex2, one, time));

			db.TransactionalStorage.Batch(accessor =>
			{
				var stats = accessor.Indexing.GetIndexesStats().Where(x => x.Id ==searchIndex2).First();

				Assert.Equal(one, stats.LastIndexedEtag);

				Assert.Equal(time, stats.LastIndexedTimestamp);

				stats = accessor.Indexing.GetIndexesStats().Where(x => x.Id == searchIndex1).First();

				Assert.Equal(Etag.Empty, stats.LastIndexedEtag);

				Assert.Equal(DateTime.MinValue, stats.LastIndexedTimestamp);
			});
		}
	}
}
