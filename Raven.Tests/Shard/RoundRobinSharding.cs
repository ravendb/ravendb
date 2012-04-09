// -----------------------------------------------------------------------
//  <copyright file="RoundRobinSharding.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Shard;
using Raven.Server;
using Xunit;
using Raven.Client.Linq;
using System.Linq;

namespace Raven.Tests.Shard
{
	public class RoundRobinSharding : RavenTest
	{
		private readonly RavenDbServer[] servers;
		private readonly ShardedDocumentStore store;

		public RoundRobinSharding()
		{
			servers = new[]
			{
				GetNewServer(8078),
				GetNewServer(8077),
				GetNewServer(8076)
			};

			var shards = new Dictionary<string, IDocumentStore>
			{
				{"one", new DocumentStore{Url = "http://localhost:8078"}},
				{"two", new DocumentStore{Url = "http://localhost:8077"}},
				{"tri", new DocumentStore{Url = "http://localhost:8076"}},
			};


			var shardStrategy = new ShardStrategy(shards)
				.ShardingOn<Post>()
				.ShardingOn<PostComments>(x => x.PostId);

			store = new ShardedDocumentStore(shardStrategy);
			store.Initialize();
		}

		public override void Dispose()
		{
			foreach (var ravenDbServer in servers)
			{
				ravenDbServer.Dispose();
			}
			store.Dispose();
			base.Dispose();
		}

		[Fact]
		public void SavingTwoPostsWillGoToTwoDifferentServers()
		{
			using(var session = store.OpenSession())
			{
				var p1 = new Post();
				session.Store(p1);
				var p2 = new Post();
				session.Store(p2);

				session.SaveChanges();

				Assert.Equal("tri/posts/1", p1.Id);
				Assert.Equal("two/posts/2", p2.Id);
			}
		}

		[Fact]
		public void WillGetGoodLocalityOfReference()
		{
			using (var session = store.OpenSession())
			{
				var p1 = new Post();
				session.Store(p1);
				var p2 = new Post();
				session.Store(p2);

				var pc1 = new PostComments
				{
					PostId = p1.Id
				};
				session.Store(pc1);

				var pc2 = new PostComments
				{
					PostId = p2.Id
				};
				session.Store(pc2);

				session.SaveChanges();

				Assert.Equal("tri/posts/1", p1.Id);
				Assert.Equal("two/posts/2", p2.Id);

				Assert.Equal("tri/PostComments/1", pc1.Id);
				Assert.Equal("two/PostComments/2", pc2.Id);
			}
		}
		

		public class Post
		{
			public string Id { get; set; }
			public DateTime PublishedAt { get; set; }

		}

		public class PostComments
		{
			public string Id { get; set; }
			public string PostId { get; set; }
			public string[] Comments { get; set; }
		}
	}
}