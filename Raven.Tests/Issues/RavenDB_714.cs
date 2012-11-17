// -----------------------------------------------------------------------
//  <copyright file="RavenDB_714.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Tests.Issues
{
	using System.Collections.Generic;

	using Raven.Client;
	using Raven.Client.Document;
	using Raven.Client.Shard;
	using Raven.Server;
	using Raven.Tests.MailingList;

	using Xunit;

	public class RavenDB_714 : RavenTest
	{
		private readonly RavenDbServer[] servers;
		private readonly ShardedDocumentStore shardedDocumentStore;

		private readonly IList<string> shardNames = new List<string>
		{
			"1",
			"2",
			"3"
		};

		public RavenDB_714()
		{
			servers = new[]
			{
				GetNewServer(8079),
				GetNewServer(8078),
				GetNewServer(8077),
			};

			shardedDocumentStore = new ShardedDocumentStore(new ShardStrategy(new Dictionary<string, IDocumentStore>
			{
				{shardNames[0], CreateDocumentStore(8079)},
				{shardNames[1], CreateDocumentStore(8078)},
				{shardNames[2], CreateDocumentStore(8077)}
			}));

			shardedDocumentStore.Initialize();
		}

		private static IDocumentStore CreateDocumentStore(int port)
		{
			return new DocumentStore
			{
				Url = string.Format("http://localhost:{0}/", port),
				Conventions =
				{
					FailoverBehavior = FailoverBehavior.FailImmediately
				}
			};
		}

		[Fact]
		public void LazyStartsWithForShardedDocumentStore()
		{
			using (var session = shardedDocumentStore.OpenSession())
			{
				session.Store(new User { Id = "customers/1234/users/1" });
				session.Store(new User { Id = "customers/1234/users/2" });
				session.Store(new User { Id = "customers/1234/users/3" });
				session.SaveChanges();
			}

			using (var session = shardedDocumentStore.OpenSession())
			{
				session.Store(new User { Id = "customers/1234/users/4" });
				session.Store(new User { Id = "customers/1234/users/5" });
				session.Store(new User { Id = "customers/1234/users/6" });
				session.SaveChanges();
			}

			using (var session = shardedDocumentStore.OpenSession())
			{
				var users = session.Advanced.Lazily.LoadStartingWith<User>("customers/1234/users");

				Assert.Equal(6, users.Value.Length);
			}
		}

		[Fact]
		public void LazyStartsWithForDocumentStore()
		{
			using (var server = this.GetNewServer(8071))
			using (var store = new DocumentStore { Url = "http://localhost:8071" })
			{
				store.Initialize();

				using (var session = store.OpenSession())
				{
					session.Store(new User { Id = "customers/1234/users/1" });
					session.Store(new User { Id = "customers/1234/users/2" });
					session.Store(new User { Id = "customers/1234/users/3" });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var users = session.Advanced.Lazily.LoadStartingWith<User>("customers/1234/users");

					Assert.Equal(3, users.Value.Length);
				}
			}
		}

		[Fact]
		public void StartsWithForDocumentStore()
		{
			using (var server = this.GetNewServer(8071))
			using (var store = new DocumentStore { Url = "http://localhost:8071" })
			{
				store.Initialize();

				using (var session = store.OpenSession())
				{
					session.Store(new User { Id = "customers/1234/users/1" });
					session.Store(new User { Id = "customers/1234/users/2" });
					session.Store(new User { Id = "customers/1234/users/3" });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var users = session.Advanced.LoadStartingWith<User>("customers/1234/users");

					Assert.Equal(3, users.Length);
				}
			}
		}

		public override void Dispose()
		{
			shardedDocumentStore.Dispose();
			foreach (var server in servers)
			{
				server.Dispose();
			}
			base.Dispose();
		}
	}
}