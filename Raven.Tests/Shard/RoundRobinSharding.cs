// -----------------------------------------------------------------------
//  <copyright file="RoundRobinSharding.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Connection;
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
		private readonly Dictionary<string, RavenDbServer> servers;
		private readonly ShardedDocumentStore store;
		private Dictionary<string, IDocumentStore> documentStores;

		public RoundRobinSharding()
		{
			servers = new Dictionary<string, RavenDbServer>
			{
				{"one",GetNewServer(8078)},
				{"two", GetNewServer(8077)},
				{"tri", GetNewServer(8076)}
			};

			documentStores = new Dictionary<string, IDocumentStore>
			{
				{"one", new DocumentStore{Url = "http://localhost:8078"}},
				{"two", new DocumentStore{Url = "http://localhost:8077"}},
				{"tri", new DocumentStore{Url = "http://localhost:8076"}},
			};

			foreach (var documentStore in documentStores)
			{
				documentStore.Value.Conventions.FailoverBehavior = FailoverBehavior.FailImmediately;
			}


			var shardStrategy = new ShardStrategy(documentStores)
				.ShardingOn<Post>()
				.ShardingOn<PostComments>(x => x.PostId);

			store = new ShardedDocumentStore(shardStrategy);
			store.Initialize();
		}

		public override void Dispose()
		{
			foreach (var ravenDbServer in servers)
			{
				ravenDbServer.Value.Dispose();
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
		public void WhenQueryingWillGoToTheRightServer()
		{
			using (var session = store.OpenSession())
			{
				var p1 = new Post();
				session.Store(p1);
				var pc1 = new PostComments {PostId = p1.Id};
				session.Store(pc1);
				session.SaveChanges();
			}

			foreach (var ravenDbServer in servers)
			{
				ravenDbServer.Value.Server.ResetNumberOfRequests();
			}

			using (var session = store.OpenSession())
			{
				var posts = session.Query<PostComments>().Where(x => x.PostId == "tri/posts/1").ToList();
				Assert.NotEmpty(posts);
			}
			Assert.Equal(0, servers["one"].Server.NumberOfRequests);
			Assert.Equal(0, servers["two"].Server.NumberOfRequests);
			Assert.Equal(1, servers["tri"].Server.NumberOfRequests);
		}

		[Fact]
		public void WhenQueryingWillGoToTheRightServer_UsingQueryById()
		{
			store.Conventions.AllowQueriesOnId = true;
			using (var session = store.OpenSession())
			{
				var p1 = new Post();
				session.Store(p1);
				var pc1 = new PostComments { PostId = p1.Id };
				session.Store(pc1);
				session.SaveChanges();
			}

			foreach (var ravenDbServer in servers)
			{
				ravenDbServer.Value.Server.ResetNumberOfRequests();
			}

			using (var session = store.OpenSession())
			{
				var posts = session.Query<Post>().Where(x => x.Id == "tri/posts/1").ToList();
				Assert.NotEmpty(posts);
			}
			Assert.Equal(0, servers["one"].Server.NumberOfRequests);
			Assert.Equal(0, servers["two"].Server.NumberOfRequests);
			Assert.Equal(1, servers["tri"].Server.NumberOfRequests);
		}

		[Fact]
		public void WhenQueryingWillGoToTheRightServer_Loading()
		{
			using (var session = store.OpenSession())
			{
				var p1 = new Post();
				session.Store(p1);
				var pc1 = new PostComments { PostId = p1.Id };
				session.Store(pc1);
				session.SaveChanges();
			}

			foreach (var ravenDbServer in servers)
			{
				ravenDbServer.Value.Server.ResetNumberOfRequests();
			}

			using (var session = store.OpenSession())
			{
				Assert.NotNull(session.Load<Post>("tri/posts/1"));
				Assert.NotNull(session.Load<PostComments>("tri/PostComments/1"));
			}
			Assert.Equal(0, servers["one"].Server.NumberOfRequests);
			Assert.Equal(0, servers["two"].Server.NumberOfRequests);
			Assert.Equal(2, servers["tri"].Server.NumberOfRequests);
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