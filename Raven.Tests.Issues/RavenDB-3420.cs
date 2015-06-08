using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Jint.Parser;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Shard;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3420 : RavenTestBase
	{
		[Fact]
		public void BulkInsertSharded()
		{
			var server1 = GetNewServer(8079);
			var server2 = GetNewServer(8078);
			var shards = new Dictionary<string, IDocumentStore>
			{
				{"Shard1", new DocumentStore {Url = server1.Configuration.ServerUrl}},
				{"Shard2", new DocumentStore {Url = server2.Configuration.ServerUrl}},
			};

			var shardStrategy = new ShardStrategy(shards);
			shardStrategy.ShardingOn<Profile>(x => x.Location);

			using (var shardedDocumentStore = new ShardedDocumentStore(shardStrategy))
			{
				shardedDocumentStore.Initialize();
				var entity1 = new Profile {Id = "bulk1", Name = "Hila", Location = "Shard1"};
				var entity2 = new Profile {Name = "Jay", Location = "Shard2"};
				var entity3 = new Profile { Name = "Jay", Location = "Shard1" };
				using (var bulkInsert = shardedDocumentStore.ShardedBulkInsert())
				{
					bulkInsert.Store(entity1);
					bulkInsert.Store(entity2);
					bulkInsert.Store(entity3);
				}
			}
			using (var store1 = new DocumentStore{Url = server1.SystemDatabase.Configuration.ServerUrl,}.Initialize())
			{
				using (var session = store1.OpenSession())
				{
					var docs = session.Load<Profile>("Shard1/bulk1");
					var docs2 = session.Load<Profile>("Shard1/profiles/2");

					var totalDocs = session.Query<Profile>().ToList();

					Assert.Equal("Shard1", docs.Location);
					Assert.Equal("Shard1", docs2.Location);
					Assert.Equal(2, totalDocs.Count);
				}
			}
			using (var store2 = new DocumentStore{Url = server2.SystemDatabase.Configuration.ServerUrl,}.Initialize())
			{
				using (var session = store2.OpenSession())
				{
					var docs = session.Load<Profile>("Shard2/profiles/1");
					var totalDocs = session.Query<Profile>().ToList();

					Assert.Equal("Shard2", docs.Location);
					Assert.Equal(1, totalDocs.Count);	
				}
			}
		}
		public class Profile
		{
			public string Id { get; set; }

			public string Name { get; set; }

			public string Location { get; set; }
		}
	}
}
