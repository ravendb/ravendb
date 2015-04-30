using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Database.Server;
using Raven.Database.Server.Security;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Tests.Common;
using Xunit;
namespace Raven.Tests.Issues
{
	public class RavenDB_3383 : RavenTest
	{
		protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
		{
			configuration.AnonymousUserAccessMode = AnonymousUserAccessMode.None;
			Authentication.EnableOnce();
		}

		[Fact]
		public void Aggressively_Cached_Load_Should_Be_Invalidated_Upon_Change()
		{
			using (var server = GetNewServer(enableAuthentication: true))
			{
				var serverUrl = ConfigureApiKeys(server);

				using (var docStore = new DocumentStore
				{
					Url = serverUrl + "databases/Foo",
					ApiKey = "Foo/ThisIsMySecret2",
					Conventions =
					{
						ShouldAggressiveCacheTrackChanges = true
					}

				})
				{
					docStore.Initialize();

					DeleteAllCars(docStore);

					StoreCars(docStore);
				
					using (var session = docStore.OpenSession("Foo"))
					{
						var car = GetAggressivelyCachedCar(session);
						Assert.True(car.Brand.Equals("Volvo", StringComparison.OrdinalIgnoreCase));
					}

					using (var session = docStore.OpenSession())
					{
						docStore.GetObserveChangesAndEvictItemsFromCacheTask().Wait();
						var car = session.Load<Car>("car/2");
						car.Brand = "BMW";
						session.SaveChanges();
					}

					using (var session = docStore.OpenSession("Foo"))
					{
						var car = GetAggressivelyCachedCar(session);
						Assert.True(car.Brand.Equals("BMW", StringComparison.OrdinalIgnoreCase)); 

					}
				}
			}
		}
		[Fact]
		public void Aggressively_Cached_Load_Should_Be_Invalidated_Upon_Change_With_DefaultDatabase()
		{
			using (var server = GetNewServer(enableAuthentication: true))
			{
				var serverUrl = ConfigureApiKeys(server);

				using (var docStore = new DocumentStore
				{
					Url = serverUrl,
					DefaultDatabase = "Foo",
					ApiKey = "Foo/ThisIsMySecret2",
					Conventions =
					{
						ShouldAggressiveCacheTrackChanges = true
					}

				})
				{
					docStore.Initialize();

					DeleteAllCars(docStore);

					StoreCars(docStore);

					using (var session = docStore.OpenSession("Foo"))
					{
						var car = GetAggressivelyCachedCar(session);
						Assert.True(car.Brand.Equals("Volvo", StringComparison.OrdinalIgnoreCase));
					}


					int numberOfCacheResets = docStore.JsonRequestFactory.NumberOfCacheResets;

					using (var session = docStore.OpenSession())
					{
						docStore.GetObserveChangesAndEvictItemsFromCacheTask().Wait();
						var car = session.Load<Car>("car/2");
						car.Brand = "BMW";
						session.SaveChanges();
					}

					Assert.True(SpinWait.SpinUntil(() => docStore.JsonRequestFactory.NumberOfCacheResets > numberOfCacheResets, 10000));

					using (var session = docStore.OpenSession("Foo"))
					{
						var car = GetAggressivelyCachedCar(session);
						Assert.True(car.Brand.Equals("BMW", StringComparison.OrdinalIgnoreCase));

					}
				}
			}
		}
		private static string ConfigureApiKeys(RavenDbServer server)
		{
			server.SystemDatabase.Documents.Put("Raven/ApiKeys/sysadmin", null, RavenJObject.FromObject(new ApiKeyDefinition
			{
				Name = "sysadmin",
				Secret = "ThisIsMySecret",
				Enabled = true,
				Databases = new List<ResourceAccess>
				{
					new ResourceAccess {TenantId = Constants.SystemDatabase, Admin = true},
				}
			}), new RavenJObject(), null);


			server.SystemDatabase.Documents.Put("Raven/ApiKeys/Foo", null, RavenJObject.FromObject(new ApiKeyDefinition
			{
				Name = "Foo",
				Secret = "ThisIsMySecret2",
				Enabled = true,
				Databases = new List<ResourceAccess>

				{
					new ResourceAccess {TenantId = "Foo", Admin = true},
				}
			}), new RavenJObject(), null);

			var serverUrl = server.SystemDatabase.ServerUrl;

			using (var store = new DocumentStore()
			{
				Url = serverUrl,
				ApiKey = "sysadmin/ThisIsMySecret"
			})
			{
				store.Initialize();
				store
					.DatabaseCommands
					.GlobalAdmin
					.CreateDatabase(new DatabaseDocument
					{
						Id = "Foo",
						Settings =
						{
							{"Raven/DataDir", "Foo"}
						}
					});
				store.DatabaseCommands.EnsureDatabaseExists("Foo");
			}
			return serverUrl;
		}

		private static Car GetAggressivelyCachedCar(IDocumentSession session)
		{
			using (session.Advanced.DocumentStore.AggressivelyCache())
			{
				return session.Load<Car>("car/2");
			}
		}

		private static void DeleteAllCars(IDocumentStore docStore)
		{
			using (var session = docStore.OpenSession())
			{
				var cars = session.Query<Car>().Customize(x => x.WaitForNonStaleResultsAsOfNow(TimeSpan.FromSeconds(30))).ToList();

				foreach (var car in cars)
				{
					session.Delete(car);
				}

				session.SaveChanges();
			}
		}

		private static void StoreCars(IDocumentStore docStore)
		{
			using (var session = docStore.OpenSession())
			{
				session.Store(new Car { Id = "car/1", Brand = "Audi" });
				session.Store(new Car { Id = "car/2", Brand = "Volvo" });
				session.Store(new Car { Id = "car/3", Brand = "BMW" });
				session.Store(new Car { Id = "car/4", Brand = "Toyota" });
				session.Store(new Car { Id = "car/5", Brand = "Abarth" });
				session.Store(new Car { Id = "car/6", Brand = "Alfa Romeo" });
				session.Store(new Car { Id = "car/7", Brand = "Aston Martin" });

				session.SaveChanges();
			}
		}
	}
	public class Car
	{
		public string Id { get; set; }
		public string Brand { get; set; }
	}
}
