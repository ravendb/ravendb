// -----------------------------------------------------------------------
//  <copyright file="RavenDB_406.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Threading;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Client.Indexes;
using Raven.Tests.Linq;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_406 : RavenTest
	{
		[Fact]
		public void LoadResultShoudBeUpToDateEvenIfAggresiveCacheIsEnabled()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User()
					{
						Id = "users/1",
						Name = "John"
					});
					session.SaveChanges();
				}

				using (store.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
				{
					// make sure that object is cached
					using (var session = store.OpenSession())
					{
						store.Changes().Task.Result.WaitForAllPendingSubscriptions();

						var users = session.Load<User>(new[] {"users/1"});

						Assert.Equal("John", users[0].Name);
					}

					// change object
					using (var session = store.OpenSession())
					{
						session.Store(new User()
						{
							Id = "users/1",
							Name = "Adam"
						});
						session.SaveChanges();
					}


					Assert.True(SpinWait.SpinUntil(() =>store.JsonRequestFactory.NumberOfCacheResets > 0, 10000));

					using (var session = store.OpenSession())
					{
						var users = session.Load<User>(new[] { "users/1" });

						Assert.Equal("Adam", users[0].Name);
					}
				}
			}
		}

		[Fact]
		public void QueryResultShoudBeUpToDateEvenIfAggresiveCacheIsEnabled()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				new RavenDocumentsByEntityName().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new User()
					{
						Id = "users/1",
						Name = "John"
					});
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (store.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
				{
					// make sure that object is cached
					using (var session = store.OpenSession())
					{
						var users = session.Query<User>()
							.ToList(); 

						Assert.Equal("John", users[0].Name);
					}

					// change object
					using (var session = store.OpenSession())
					{
						session.Store(new User()
						{
							Id = "users/1",
							Name = "Adam"
						});
						session.SaveChanges();
					}


					Assert.True(SpinWait.SpinUntil(() => store.JsonRequestFactory.NumberOfCacheResets > 0, 10000));

					using (var session = store.OpenSession())
					{
						var users = session.Query<User>().ToList();

						Assert.Equal("Adam", users[0].Name);
					}
				}
			}
		}

		[Fact]
		public void CacheClearingShouldTakeIntoAccountTenantDatabases()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079"}.Initialize())
			{
				store.DatabaseCommands.EnsureDatabaseExists("Northwind_1");
				store.DatabaseCommands.EnsureDatabaseExists("Northwind_2");

				using (var session = store.OpenSession("Northwind_1"))
				{
					session.Store(new User()
					{
						Id = "users/1",
						Name = "John"
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession("Northwind_2"))
				{
					session.Store(new User()
					{
						Id = "users/1",
						Name = "John"
					});
					session.SaveChanges();
				}

				using (store.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
				{
					// make sure that object is cached
					using (var session = store.OpenSession("Northwind_1"))
					{
						store.Changes().Task.Result.WaitForAllPendingSubscriptions();

						var users = session.Load<User>(new[] { "users/1" });

						Assert.Equal("John", users[0].Name);
					}

					using (var session = store.OpenSession("Northwind_2"))
					{
						store.Changes().Task.Result.WaitForAllPendingSubscriptions();

						var users = session.Load<User>(new[] { "users/1" });

						Assert.Equal("John", users[0].Name);
					}

					// change object on Northwind_1 ONLY
					using (var session = store.OpenSession("Northwind_1"))
					{
						session.Store(new User()
						{
							Id = "users/1",
							Name = "Adam"
						});
						session.SaveChanges();
					}


					Assert.True(SpinWait.SpinUntil(() => store.JsonRequestFactory.NumberOfCacheResets > 0, 10000));

					using (var session = store.OpenSession("Northwind_1"))
					{
						var users = session.Load<User>(new[] { "users/1" });

						Assert.Equal("Adam", users[0].Name);
					}

					using (var session = store.OpenSession("Northwind_2"))
					{
						var users = session.Load<User>(new[] { "users/1" });

						Assert.Equal("John", users[0].Name);
					}
				}
			}
		}

		[Fact]
		public void ShouldServeFromCacheIfThereWasNoChange()
		{
			using (var server = GetNewServer())
			using (var store = new DocumentStore {Url = "http://localhost:8079"}.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User()
					{
						Id = "users/1",
						Name = "John"
					});

					session.SaveChanges();
				}

				using (store.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
				{
					// make sure that object is cached
					using (var session = store.OpenSession())
					{
						var user = session.Load<User>(new[] {"users/1"}).First();
						Assert.Equal("John", user.Name);
					}

					// change object
					using (var session = store.OpenSession())
					{
						session.Store(new User()
						{
							Id = "users/1",
							Name = "Adam"
						});

						session.SaveChanges();
					}

					Assert.True(SpinWait.SpinUntil(() => store.JsonRequestFactory.NumberOfCacheResets > 0, 10000));

					server.Server.ResetNumberOfRequests();

					using (var session = store.OpenSession())
					{
						var user = session.Load<User>(new[] {"users/1"}).First(); // will create a request
						Assert.Equal("Adam", user.Name);
					}

					using (var session = store.OpenSession())
					{
						var user = session.Load<User>(new[] { "users/1" }).First(); // will be taken from a cache
						Assert.Equal("Adam", user.Name);
					}

					WaitForAllRequestsToComplete(server);

					Assert.Equal(1, server.Server.NumberOfRequests);
				}
			}
		}
	}
}