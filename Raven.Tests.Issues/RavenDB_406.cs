// -----------------------------------------------------------------------
//  <copyright file="RavenDB_406.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Client.Indexes;
using Raven.Database.Server.Controllers;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_406 : RavenTest
	{
		[Fact]
		public void LoadResultShouldBeUpToDateEvenIfAggressiveCacheIsEnabled()
		{
			using (var store = NewRemoteDocumentStore())
			{
				store.Conventions.ShouldSaveChangesForceAggressiveCacheCheck = false;
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
					
					store.GetObserveChangesAndEvictItemsFromCacheTask().Wait();

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
		public void QueryResultShouldBeUpToDateEvenIfAggressiveCacheIsEnabled()
		{
			using (var store = NewRemoteDocumentStore())
			{
				store.Conventions.ShouldSaveChangesForceAggressiveCacheCheck = false;
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
						var users = session.Query<User>().Customize(x => x.WaitForNonStaleResults())
							.ToList(); 

						Assert.Equal("John", users[0].Name);
					}

					((DocumentStore)store).GetObserveChangesAndEvictItemsFromCacheTask().Wait();
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
						var users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).ToList();

						Assert.Equal(1, users.Count);
						Assert.Equal("Adam", users[0].Name);
					}
				}
			}
		}

		[Fact]
		public void CacheClearingShouldTakeIntoAccountTenantDatabases()
		{
			using (var store = NewRemoteDocumentStore())
			{
				store.Conventions.ShouldSaveChangesForceAggressiveCacheCheck = false;

				store.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists("Northwind_1");
				store.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists("Northwind_2");

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
                        store.Changes("Northwind_1").Task.Result.WaitForAllPendingSubscriptions();

						var users = session.Load<User>(new[] { "users/1" });

						Assert.Equal("John", users[0].Name);
					}

					((DocumentStore)store).GetObserveChangesAndEvictItemsFromCacheTask().Wait();
					using (var session = store.OpenSession("Northwind_2"))
					{
                        store.Changes("Northwind_2").Task.Result.WaitForAllPendingSubscriptions();

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
			using (var store = NewRemoteDocumentStore(true, server))
			{
				store.Conventions.ShouldSaveChangesForceAggressiveCacheCheck = false;
				using (var session = store.OpenSession())
				{
					session.Store(new User
					{
						Id = "users/1",
						Name = "John"
					});

					session.SaveChanges();
				}
				WaitForIndexing(store);
				
				using (store.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
				{
					var canProceedEvent = new CountdownEvent(2);
					var changes = store.Changes();

					store.GetObserveChangesAndEvictItemsFromCacheTask().Wait();
					// make sure that object is cached
					using (var session = store.OpenSession())
					{
						var user = session.Load<User>(new[] {"users/1"}).First();
						Assert.Equal("John", user.Name);
					}

					changes.ForDocument("users/1")
						   .Subscribe(documentChangeNotification => canProceedEvent.Signal());

					changes.ForAllIndexes()
						   .Subscribe(indexChangeNotification => canProceedEvent.Signal());

					// change object
					using (var session = store.OpenSession())
					{
						session.Store(new User
						{
							Id = "users/1",
							Name = "Adam"
						});

						session.SaveChanges();
					}

					WaitForAllRequestsToComplete(server);

					//wait for indexing to complete and for document change notification to arrive
					Assert.True(canProceedEvent.Wait(Debugger.IsAttached ? TimeSpan.FromMinutes(30) : TimeSpan.FromSeconds(10)));

					server.Server.ResetNumberOfRequests();

					var documentRequestMatches = 0;
					server.Server.RequestManager.BeforeRequest += (sender, args) =>
					{
						//record as document request only requests that actually related to documents
						//(Load<T> makes request to QueriesController - and we want to count _only_ those requests)
						if (args.Controller is QueriesController) 
							Interlocked.Increment(ref documentRequestMatches);
					};

					using (var session = store.OpenSession())
					{
						var user = session.Load<User>(new[] { "users/1" }).First(); // will create a request
						Assert.Equal("Adam", user.Name); 
					}

					using (var session = store.OpenSession())
					{
						var user = session.Load<User>(new[] { "users/1" }).First(); // will be taken from a cache
						Assert.Equal("Adam", user.Name);
						
					}

					WaitForAllRequestsToComplete(server);
					Assert.Equal(1, documentRequestMatches);					
				}
			}
		}
	}
}