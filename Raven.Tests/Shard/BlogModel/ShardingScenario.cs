using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Shard;
using Raven.Client.Shard.ShardStrategy;
using Raven.Client.Shard.ShardStrategy.ShardAccess;
using Raven.Client.Shard.ShardStrategy.ShardResolution;
using Raven.Server;

namespace Raven.Tests.Shard.BlogModel
{
	public abstract class ShardingScenario : RavenTest, IDisposable
	{
		
		protected readonly ShardedDocumentStore shardedDocumentStore;
		protected readonly Dictionary<string, RavenDbServer> servers;

		protected override void CreateDefaultIndexes(IDocumentStore documentStore)
		{
		}
		
		public ShardingScenario()
		{
			servers = new Dictionary<string, RavenDbServer>
			          {
			          	{"Users", GetNewServer(8079, "shard1")},
			          	{"Blogs", GetNewServer(8078, "shard2")},
			          	{"Posts01", GetNewServer(8077, "shard3")},
			          	{"Posts02", GetNewServer(8076, "shard4")},
			          	{"Posts03", GetNewServer(8075, "shard5")}
			          };

			var shards = new Shards
			             {
			             	new DocumentStore {Identifier = "Users", Url = "http://localhost:8079"}, 
							new DocumentStore {Identifier = "Blogs", Url = "http://localhost:8078"}, 
							new DocumentStore {Identifier = "Posts01", Url = "http://localhost:8077"}, 
							new DocumentStore {Identifier = "Posts02", Url = "http://localhost:8076"}, 
							new DocumentStore {Identifier = "Posts03", Url = "http://localhost:8075"}
			             };

			foreach (var shard in shards)
			{
				shard.Initialize();
			}

			shardedDocumentStore = new ShardedDocumentStore(new ShardStrategy
			                                                {
			                                                	ShardAccessStrategy = new SequentialShardAccessStrategy(),
																ShardSelectionStrategy = new BlogShardSelectionStrategy(3),
																ShardResolutionStrategy = new BlogShardResolutionStrategy(3)
			                                                }, shards);
		}

		public void Dispose()
		{
			shardedDocumentStore.Dispose();
			foreach (var ravenDbServer in servers)
			{
				ravenDbServer.Value.Dispose();
			}
		}
	}
}