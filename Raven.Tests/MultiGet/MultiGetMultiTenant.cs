using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Tests.Bugs;
using Xunit;
using Raven.Client.Linq;
using Raven.Client.Extensions;

namespace Raven.Tests.MultiGet
{
	public class MultiGetMultiTenant : RemoteClientTest
	{
		[Fact]
		public void CanUseLazyWithMultiTenancy()
		{
			using (var server = GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				store.DatabaseCommands.EnsureDatabaseExists("test");

				using (var session = store.OpenSession("test"))
				{
					session.Store(new User { Name = "oren" });
					session.Store(new User { Name = "ayende" });
					session.Store(new User());
					session.SaveChanges();
				}
				using (var session = store.OpenSession("test"))
				{
					var result1 = session.Advanced.Lazily.Load<User>("users/1");
					var result2 = session.Advanced.Lazily.Load<User>("users/2");
					Assert.NotNull(result1.Value);
					Assert.NotNull(result2.Value);
				}
			}
		}


		[Fact]
		public void CanCacheLazyQueryResults()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079", DefaultDatabase = "test"}.Initialize())
			{
				store.DatabaseCommands.EnsureDatabaseExists("test");
				using (var session = store.OpenSession())
				{
					session.Store(new User { Name = "oren" });
					session.Store(new User());
					session.Store(new User { Name = "ayende" });
					session.Store(new User());
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					session.Query<User>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.Name == "test")
						.ToList();
				}
				using (var session = store.OpenSession())
				{
					var result1 = session.Query<User>().Where(x => x.Name == "oren").Lazily();
					var result2 = session.Query<User>().Where(x => x.Name == "ayende").Lazily();
					Assert.NotEmpty(result2.Value);

					Assert.Equal(1, session.Advanced.NumberOfRequests);
					Assert.NotEmpty(result1.Value);
					Assert.Equal(1, session.Advanced.NumberOfRequests);
					Assert.Equal(0, store.JsonRequestFactory.NumberOfCachedRequests);
				}

				using (var session = store.OpenSession())
				{
					var result1 = session.Query<User>().Where(x => x.Name == "oren").Lazily();
					var result2 = session.Query<User>().Where(x => x.Name == "ayende").Lazily();
					Assert.NotEmpty(result2.Value);


					Assert.Equal(1, session.Advanced.NumberOfRequests);
					Assert.NotEmpty(result1.Value);
					Assert.Equal(1, session.Advanced.NumberOfRequests);

					Assert.Equal(2, store.JsonRequestFactory.NumberOfCachedRequests);
				}
			}
		}

		[Fact]
		public void CanAggressivelyCacheLoads()
		{
			using (var server = GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079", DefaultDatabase = "test"}.Initialize())
			{
				store.DatabaseCommands.EnsureDatabaseExists("test");
				using (var session = store.OpenSession())
				{
					session.Store(new User());
					session.Store(new User());
					session.SaveChanges();
				}

				WaitForAllRequestsToComplete(server);
				server.Server.ResetNumberOfRequests();

				for (int i = 0; i < 5; i++)
				{
					using (var session = store.OpenSession())
					{
						using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
						{
							session.Advanced.Lazily.Load<User>("users/1");
							session.Advanced.Lazily.Load<User>("users/2");

							session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();
						}
					}
				}

				WaitForAllRequestsToComplete(server);
				Assert.Equal(1, server.Server.NumberOfRequests);
			}
		}


	}
}