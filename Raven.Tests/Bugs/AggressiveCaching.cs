using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Server;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class AggressiveCaching : RemoteClientTest
	{
		private readonly RavenDbServer server;
		private readonly IDocumentStore store;

		public AggressiveCaching()
		{
			server = GetNewServer();
			store = new DocumentStore
			{
				Url = "http://localhost:8079",
				Conventions = {FailoverBehavior = FailoverBehavior.FailImmediately}
			}.Initialize();

			using (var session = store.OpenSession())
			{
				session.Store(new User());
				session.SaveChanges();
			}

			WaitForAllRequestsToComplete(server);
			server.Server.ResetNumberOfRequests();
		}

		public override void Dispose()
		{
			store.Dispose();
			server.Dispose();
			base.Dispose();
		}

		[Fact]
		public void CanAggressivelyCacheLoads()
		{
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

		[Fact]
		public void CanAggressivelyCacheQueries()
		{
			for (var i = 0; i < 5; i++)
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

		// TODO: NOTE: I think this test is not complete, since the assertion here is exactly the same as in CanAggressivelyCacheQueries.
		[Fact]
		public void WaitForNonStaleResultsIgnoresAggressiveCaching()
		{
			for (var i = 0; i < 5; i++)
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

			}

			WaitForAllRequestsToComplete(server);
			Assert.NotEqual(1, server.Server.NumberOfRequests);
		}
	}
}