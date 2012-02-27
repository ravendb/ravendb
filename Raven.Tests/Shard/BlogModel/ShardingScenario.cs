using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Shard;
using Raven.Client.Shard.ShardStrategy;
using Raven.Client.Shard.ShardStrategy.ShardAccess;
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

		protected ShardingScenario()
		{
			RavenDbServer users = null;
			RavenDbServer blogs = null;
			RavenDbServer posts1 = null;
			RavenDbServer posts2 = null;
			RavenDbServer posts3 = null;
			try
			{
				users = GetNewServer(8079, "shard1");
				blogs = GetNewServer(8078, "shard2");
				posts1 = GetNewServer(8077, "shard3");
				posts2 = GetNewServer(8076, "shard4");
				posts3 = GetNewServer(8075, "shard5");
			}
			catch (Exception)
			{
				if (users != null)
					users.Dispose();
				if (blogs != null)
					blogs.Dispose();
				if (posts1 != null)
					posts1.Dispose();
				if (posts2 != null)
					posts2.Dispose();
				if (posts3 != null)
					posts3.Dispose();
				throw;
			}

			servers = new Dictionary<string, RavenDbServer>
			{
				{"Users", users},
				{"Blogs", blogs},
				{"Posts01", posts1},
				{"Posts02", posts2},
				{"Posts03", posts3}
			};

			var shards = new Shards
			{
				new DocumentStore {Identifier = "Users", Url = "http://localhost:8079"},
				new DocumentStore {Identifier = "Blogs", Url = "http://localhost:8078"},
				new DocumentStore {Identifier = "Blogs", Url = "http://localhost:8078"},
				new DocumentStore {Identifier = "Posts01", Url = "http://localhost:8077"},
				new DocumentStore {Identifier = "Posts02", Url = "http://localhost:8076"},
				new DocumentStore {Identifier = "Posts03", Url = "http://localhost:8075"}
			};


			foreach (var shard in shards)
			{
				shard.Conventions.FailoverBehavior = FailoverBehavior.FailImmediately;
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
