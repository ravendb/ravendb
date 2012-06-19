using System;
using System.Linq;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class AggressiveCaching : RemoteClientTest
	{
		public AggressiveCaching()
		{
			using (var server = GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079",
				Conventions =
				{
					FailoverBehavior = FailoverBehavior.FailImmediately
				}
			}.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User());
					session.SaveChanges();
				}

				WaitForAllRequestsToComplete(server);
				server.Server.ResetNumberOfRequests();
			}
		}

		[Fact]
		public void CanAggressivelyCacheLoads()
		{
			using (var server = GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079",
				Conventions =
				{
					FailoverBehavior = FailoverBehavior.FailImmediately
				}
			}.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User());
					session.SaveChanges();
				}

				WaitForAllRequestsToComplete(server);
				server.Server.ResetNumberOfRequests();

				for (var i = 0; i < 5; i++)
				{
					using (var session = store.OpenSession())
					{
						using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
						{
							session.Load<User>("users/1");
						}
					}
				}

				WaitForAllRequestsToComplete(server);
				Assert.Equal(1, server.Server.NumberOfRequests);
			}
		}

		[Fact]
		public void CanAggressivelyCacheQueries()
		{
			using (var server = GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079",
				Conventions =
				{
					FailoverBehavior = FailoverBehavior.FailImmediately
				}
			}.Initialize())
			{
				using (var session = store.OpenSession())
				{
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
							session.Query<User>().ToList();
						}
					}

				}

				WaitForAllRequestsToComplete(server);
				Assert.Equal(1, server.Server.NumberOfRequests);
			}
		}

		// TODO: NOTE: I think this test is not complete, since the assertion here is exactly the same as in CanAggressivelyCacheQueries.
		[Fact]
		public void WaitForNonStaleResultsIgnoresAggressiveCaching()
		{
			using (var server = GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079",
				Conventions =
					{
						FailoverBehavior = FailoverBehavior.FailImmediately
					}
			}.Initialize())
			{
				using (var session = store.OpenSession())
				{
					using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
					{
						session.Query<User>()
						.Customize(x => x.WaitForNonStaleResults())
							.ToList();
					}
				}

				WaitForAllRequestsToComplete(server);
				Assert.NotEqual(1, server.Server.NumberOfRequests);
			}
		}
	}
}
