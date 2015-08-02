using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Shard;
using Raven.Database.FileSystem.Extensions;
using Raven.Server;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3726 : RavenTestBase
	{
		public class Profile
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public string Location { get; set; }
		}

		public class HybridShardingResolutionStrategy : DefaultShardResolutionStrategy
		{
			private readonly HashSet<Type> sharedTypes;
			private readonly string defaultShard;

			public HybridShardingResolutionStrategy(IEnumerable<string> shardIds, ShardStrategy shardStrategy,
				IEnumerable<Type> sharedTypes, string defaultShard)
				: base(shardIds, shardStrategy)
			{
				this.sharedTypes = new HashSet<Type>(sharedTypes);
				this.defaultShard = defaultShard;
			}

			public override string GenerateShardIdFor(object entity, object owner)
			{
				if (!sharedTypes.Contains(entity.GetType()))
					return ShardIds.FirstOrDefault(x => x == defaultShard);

				return base.GenerateShardIdFor(entity, owner);
			}
		}

		public class ProfileIndex : AbstractIndexCreationTask
		{
			public override IndexDefinition CreateIndexDefinition()
			{
				return new IndexDefinition
				{
					Map = @"from profile in docs select new { profile.Id, profile.Name, profile.Location };"
				};
			}
		}

		[Fact]
		public async Task Test()
		{
			var server1 = GetNewServer(8079);
			var server2 = GetNewServer(8078);

			var shard1 = new DocumentStore {Url = server1.Configuration.ServerUrl};
			var shard2 = new DocumentStore {Url = server2.Configuration.ServerUrl};
			var shards = new Dictionary<string, IDocumentStore>
			{
				{"Shard1", shard1},
				{"Shard2", shard2},
			};


			var shardStrategy = new ShardStrategy(shards);
			shardStrategy.ShardResolutionStrategy = new HybridShardingResolutionStrategy(shards.Keys, shardStrategy, new Type[0], "Shard1");
			shardStrategy.ShardingOn<Profile>(x => x.Location);

			using (var shardedDocumentStore = new ShardedDocumentStore(shardStrategy))
			{
				shardedDocumentStore.Initialize();
				new ProfileIndex().Execute(shardedDocumentStore);

				var _facets = new List<Facet>
				{
					new Facet {Name = "Name", Mode = FacetMode.Default}
				};
				var profile = new Profile {Name = "Test", Location = "Shard1"};

				using (var documentSession = shard1.OpenSession())
				{
					documentSession.Store(new FacetSetup {Id = "facets/TestFacets", Facets = _facets});
					documentSession.SaveChanges();
				}
				using (var documentSession = shard2.OpenSession())
				{
					documentSession.Store(new FacetSetup {Id = "facets/TestFacets", Facets = _facets});
					documentSession.SaveChanges();
				}


				using (var documentSession = shardedDocumentStore.OpenSession())
				{
					documentSession.Store(profile, profile.Id);
					documentSession.SaveChanges();
				}
				using (var session = shardedDocumentStore.OpenSession())
				{
					var prof = session.Load<Profile>(profile.Id);
					Assert.Equal(prof.Id, profile.Id);
				}
				WaitForIndexing(shard1);
				WaitForIndexing(shard2);
				using (var documentSession = shardedDocumentStore.OpenAsyncSession())
				{
					var query = documentSession.Query<Profile>("ProfileIndex").Where(x => x.Name == "Test");
					var res = await query.ToFacetsAsync(new FacetSetup {Id = "facets/TestFacets", Facets = _facets}.Id);
					Assert.Equal(1, res.Results.Count);
				}
			}
		}
	}
}