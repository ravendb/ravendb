namespace Raven.Tests.Issues
{
	using System.Collections.Generic;

	using Raven.Client;
	using Raven.Client.Document;
	using Raven.Client.Shard;
	using Raven.Server;

	using Xunit;

	public class RavenDB_579 : RavenTest
	{
		private readonly RavenDbServer[] servers;
		private readonly ShardedDocumentStore documentStore;

		public class Person
		{
			public string Id { get; set; }

			public string FirstName { get; set; }

			public string LastName { get; set; }

			public string MiddleName { get; set; }
		}

		public RavenDB_579()
		{
			servers = new[]
			{
				GetNewServer(8079),
				GetNewServer(8078),
				GetNewServer(8077),
			};

			documentStore = new ShardedDocumentStore(new ShardStrategy(new Dictionary<string, IDocumentStore>
			{
				{"1", CreateDocumentStore(8079)},
				{"2", CreateDocumentStore(8078)},
				{"3", CreateDocumentStore(8077)}
			}));
			documentStore.Initialize();
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
		public void OneShardPerSaveChangesStrategy()
		{
			using (var session = documentStore.OpenSession())
			{
				var entity1 = new Person { Id = "1", FirstName = "William", MiddleName = "Edgard", LastName = "Smith" };
				session.Store(entity1);
				var entity2 = new Person { Id = "2", FirstName = "William", MiddleName = "Edgard", LastName = "Smith" };
				session.Store(entity2);
				session.SaveChanges();

				Assert.Equal("2/1", entity1.Id);
				Assert.Equal("2/2", entity2.Id);

				var entity3 = new Person { Id = "3", FirstName = "William", MiddleName = "Edgard", LastName = "Smith" };
				session.Store(entity3);
				var entity4 = new Person { Id = "4", FirstName = "William", MiddleName = "Edgard", LastName = "Smith" };
				session.Store(entity4);
				session.SaveChanges();

				Assert.Equal("3/3", entity3.Id);
				Assert.Equal("3/4", entity4.Id);
			}

			using (var session = documentStore.OpenSession())
			{
				var entity1 = new Person { Id = "1", FirstName = "William", MiddleName = "Edgard", LastName = "Smith" };
				session.Store(entity1);
				var entity2 = new Person { Id = "2", FirstName = "William", MiddleName = "Edgard", LastName = "Smith" };
				session.Store(entity2);
				session.SaveChanges();

				Assert.Equal("1/1", entity1.Id);
				Assert.Equal("1/2", entity2.Id);
			}
		}

		[Fact]
		public void OneShardPerSaveChangesStrategyAsync()
		{
			using (var session = documentStore.OpenAsyncSession())
			{
				var entity1 = new Person { Id = "1", FirstName = "William", MiddleName = "Edgard", LastName = "Smith" };
				session.Store(entity1);
				var entity2 = new Person { Id = "2", FirstName = "William", MiddleName = "Edgard", LastName = "Smith" };
				session.Store(entity2);
				session.SaveChangesAsync().Wait();

				Assert.Equal("2/1", entity1.Id);
				Assert.Equal("2/2", entity2.Id);

				var entity3 = new Person { Id = "3", FirstName = "William", MiddleName = "Edgard", LastName = "Smith" };
				session.Store(entity3);
				var entity4 = new Person { Id = "4", FirstName = "William", MiddleName = "Edgard", LastName = "Smith" };
				session.Store(entity4);
				session.SaveChangesAsync().Wait();

				Assert.Equal("3/3", entity3.Id);
				Assert.Equal("3/4", entity4.Id);
			}

			using (var session = documentStore.OpenAsyncSession())
			{
				var entity1 = new Person { Id = "1", FirstName = "William", MiddleName = "Edgard", LastName = "Smith" };
				session.Store(entity1);
				var entity2 = new Person { Id = "2", FirstName = "William", MiddleName = "Edgard", LastName = "Smith" };
				session.Store(entity2);
				session.SaveChangesAsync().Wait();

				Assert.Equal("1/1", entity1.Id);
				Assert.Equal("1/2", entity2.Id);
			}
		}

		public override void Dispose()
		{
			documentStore.Dispose();
			foreach (var server in servers)
			{
				server.Dispose();
			}
			base.Dispose();
		}
	}
}