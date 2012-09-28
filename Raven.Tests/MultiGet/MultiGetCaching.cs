using System;
using System.Linq;
using System.Threading;
using Raven.Client;
using Raven.Client.Linq;
using Raven.Client.Document;
using Raven.Server;
using Raven.Tests.Linq;
using Xunit;

namespace Raven.Tests.MultiGet
{
	public class MultiGetCaching : RemoteClientTest
	{
		[Fact]
		public void CanAggressivelyCacheLoads()
		{
			using (var server = GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
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

		[Fact]
		public void CanAggressivelyCachePartOfMultiGet_SimpleFirst()
		{
			using (var server = GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User());
					session.Store(new User());
					session.SaveChanges();
				}

				WaitForAllRequestsToComplete(server);
				server.Server.ResetNumberOfRequests();

				using (var session = store.OpenSession())
				{
					using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
					{
						session.Load<User>(new[] { "users/1" });
					}
				}
				using (var session = store.OpenSession())
				{
					using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
					{
						session.Advanced.Lazily.Load<User>(new[] { "users/1" }); 
						session.Advanced.Lazily.Load<User>("users/2");

						session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();
					}
				}

				WaitForAllRequestsToComplete(server);
				Assert.Equal(2, server.Server.NumberOfRequests);
				Assert.Equal(1, store.JsonRequestFactory.NumberOfCachedRequests);
			}
		}



		[Fact]
		public void CanAggressivelyCachePartOfMultiGet_DirectLoad()
		{
			using (var server = GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User());
					session.Store(new User());
					session.SaveChanges();
				}

				WaitForAllRequestsToComplete(server);
				server.Server.ResetNumberOfRequests();

				using (var session = store.OpenSession())
				{
					using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
					{
						session.Load<User>("users/1");
					}
				}
				using (var session = store.OpenSession())
				{
					using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
					{
						session.Advanced.Lazily.Load<User>("users/1");
						session.Advanced.Lazily.Load<User>("users/2");

						session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();
					}
				}

				WaitForAllRequestsToComplete(server);
				Assert.Equal(2, server.Server.NumberOfRequests);
				Assert.Equal(1, store.JsonRequestFactory.NumberOfCachedRequests);
			}
		}

		[Fact]
		public void CanAggressivelyCachePartOfMultiGet_BatchFirst()
		{
			using (var server = GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User());
					session.Store(new User());
					session.SaveChanges();
				}

				WaitForAllRequestsToComplete(server);
				server.Server.ResetNumberOfRequests();

			
				using (var session = store.OpenSession())
				{
					using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
					{
						session.Advanced.Lazily.Load<User>(new[] { "users/1" });

						session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();
					}
				}

				using (var session = store.OpenSession())
				{
					using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
					{
						session.Load<User>(new[] { "users/1" });
					}
				}

				WaitForAllRequestsToComplete(server);
				Assert.Equal(1, server.Server.NumberOfRequests);
				Assert.Equal(1, store.JsonRequestFactory.NumberOfCachedRequests);
			}
		}


		[Fact]
		public void CanCacheLazyQueryResults()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
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
		public void CanCacheLazyQueryAndMultiLoadResults()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
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
					var items = session.Advanced.Lazily.Load<User>("users/2", "users/4");
					var result1 = session.Query<User>().Where(x => x.Name == "oren").Lazily();
					var result2 = session.Query<User>().Where(x => x.Name == "ayende").Lazily();
					Assert.NotEmpty(result2.Value);
					Assert.NotEmpty(items.Value);
					Assert.Equal(1, session.Advanced.NumberOfRequests);
					Assert.NotEmpty(result1.Value);
					Assert.Equal(1, session.Advanced.NumberOfRequests);
					Assert.Equal(0, store.JsonRequestFactory.NumberOfCachedRequests);
				}

				using (var session = store.OpenSession())
				{
					var items = session.Advanced.Lazily.Load<User>("users/2", "users/4"); 
					var result1 = session.Query<User>().Where(x => x.Name == "oren").Lazily();
					var result2 = session.Query<User>().Where(x => x.Name == "ayende").Lazily();
					Assert.NotEmpty(result2.Value);
					Assert.NotEmpty(items.Value);
					Assert.Equal(1, session.Advanced.NumberOfRequests);
					Assert.NotEmpty(result1.Value);
					Assert.Equal(1, session.Advanced.NumberOfRequests);

					Assert.Equal(3, store.JsonRequestFactory.NumberOfCachedRequests);
				}
			}
		}


		[Fact]
		public void CanMixCachingForBatchAndNonBatched_BatchedFirst()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
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
				}

				using (var session = store.OpenSession())
				{
					session.Query<User>().Where(x => x.Name == "oren").ToArray();
					session.Query<User>().Where(x => x.Name == "ayende").ToArray();

					Assert.Equal(2, store.JsonRequestFactory.NumberOfCachedRequests);
				}
			}
		}

		[Fact]
		public void CanMixCachingForBatchAndNonBatched_IndividualFirst()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
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
					session.Query<User>().Where(x => x.Name == "oren").ToArray();
					session.Query<User>().Where(x => x.Name == "ayende").ToArray();
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
	}
}