using System;
using log4net.Appender;
using log4net.Config;
using log4net.Filter;
using log4net.Layout;
using Raven.Client.Document;
using Raven.Http;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class AggresiveCaching : RemoteClientTest
	{
		[Fact]
		public void CanAggresivelyCacheLoads()
		{
			using(var server = GetNewServer())
			using(var store = new DocumentStore{Url = "http://localhost:8080"}.Initialize())
			{
				using(var session = store.OpenSession())
				{
					session.Store(new User());
					session.SaveChanges();
				}

				server.Server.ResetNumberOfRequests();

				for (int i = 0; i < 5; i++)
				{
					using (var session = store.OpenSession())
					{
						using (session.Advanced.DocumentStore.AggresivelyCacheFor(TimeSpan.FromMinutes(5)))
						{
							session.Load<User>("users/1");
						}
					}

				}
				Assert.Equal(1, server.Server.NumberOfRequests);
			}
		}

		[Fact]
		public void CanAggresivelyCacheQueries()
		{
			using (var server = GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8080" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User());
					session.SaveChanges();
				}

				server.Server.ResetNumberOfRequests();

				for (int i = 0; i < 5; i++)
				{
					using (var session = store.OpenSession())
					{
						using (session.Advanced.DocumentStore.AggresivelyCacheFor(TimeSpan.FromMinutes(5)))
						{
							session.Query<User>().ToList();
						}
					}

				}
				Assert.Equal(1, server.Server.NumberOfRequests);
			}
		}

		[Fact]
		public void WaitForUnstaleResultIgnoresAggresiveCaching()
		{
			using (var server = GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8080" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User());
					session.SaveChanges();
				}

				server.Server.ResetNumberOfRequests();

				for (int i = 0; i < 5; i++)
				{
					using (var session = store.OpenSession())
					{
						using (session.Advanced.DocumentStore.AggresivelyCacheFor(TimeSpan.FromMinutes(5)))
						{
							session.Query<User>()
								.Customize(x=>x.WaitForNonStaleResults())
								.ToList();
						}
					}

				}
				Assert.NotEqual(1, server.Server.NumberOfRequests);
			}
		}
	}
}