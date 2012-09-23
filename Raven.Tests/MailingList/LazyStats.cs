using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class CanSearchLazily : RavenTest
	{
		[Fact]
		public void CanGetTotalResultsFromStatisticsOnLazySearchAgainstDynamicIndex()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				new UserByFirstName().Execute(store);
				using (var session = store.OpenSession())
				{
					session.Store(new User
					{
						FirstName = "Ayende"
					});
					session.SaveChanges();
				}
				using (var session = store.OpenSession())
				{
					session.Query<User>()
						.Customize(x => x.WaitForNonStaleResults())
						.Take(15).ToList();
					RavenQueryStatistics stats;

					var query = session.Query<User>().Statistics(out stats).Where(x => x.FirstName == "Ayende");

					var results = query.Take(8).Lazily();

					var enumerable = results.Value; //force evaluation
					Assert.Equal(1, enumerable.Count());
					Assert.Equal(DateTime.Now.Year, stats.IndexTimestamp.Year);
					Assert.True(stats.TotalResults > 0);
				}
			}
		}

		[Fact]
		public void CanGetTotalResultsFromStatisticsOnLazySearchAgainstDynamicIndex_Embedded()
		{
			using (var store = NewDocumentStore())
			{
				new UserByFirstName().Execute(store);
				using (var session = store.OpenSession())
				{
					session.Store(new User
					{
						FirstName = "Ayende"
					});
					session.SaveChanges();
				}
				using (var session = store.OpenSession())
				{
					session.Query<User>()
						.Customize(x => x.WaitForNonStaleResults())
						.Take(15).ToList();
					RavenQueryStatistics stats;

					var query = session.Query<User>().Statistics(out stats).Where(x => x.FirstName == "Ayende");

					var results = query.Take(8).Lazily();

					var enumerable = results.Value; //force evaluation
					Assert.Equal(1, enumerable.Count());
					Assert.Equal(DateTime.Now.Year, stats.IndexTimestamp.Year);
					Assert.True(stats.TotalResults > 0);
				}
			}
		}


		[Fact]
		public void CanGetTotalResultsFromStatisticsOnLazySearchAgainstDynamicIndex_NonLazy()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				new UserByFirstName().Execute(store);
				using (var session = store.OpenSession())
				{
					session.Store(new User
					{
						FirstName = "Ayende"
					});
					session.SaveChanges();
				}
				using (var session = store.OpenSession())
				{
					session.Query<User>()
						.Customize(x => x.WaitForNonStaleResults())
						.Take(15).ToList();
					RavenQueryStatistics stats;

					var query = session.Query<User>().Statistics(out stats).Where(x => x.FirstName == "Ayende");

					var results = query.Take(8).ToList();

					Assert.Equal(1, results.Count());
					Assert.Equal(DateTime.Now.Year, stats.IndexTimestamp.Year);
					Assert.True(stats.TotalResults > 0);
				}
			}
		}


		[Fact]
		public void CanGetTotalResultsFromStatisticsOnLazySearchAgainstStaticIndex()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				new UserByFirstName().Execute(store);
				using (var session = store.OpenSession())
				{
					session.Store(new User
					{
						FirstName = "Ayende"
					});
					session.SaveChanges();
				}
				using (var session = store.OpenSession())
				{
						session.Query<User, UserByFirstName>()
							.Customize(x => x.WaitForNonStaleResults())
							.Take(15).ToList();

					RavenQueryStatistics stats;

					var query = session.Query<User, UserByFirstName>().Statistics(out stats).Where(x => x.FirstName == "Ayende");

					var results = query.Take(8).Lazily();

					var enumerable = results.Value;//force evaluation
					Assert.Equal(1, enumerable.Count());
					Assert.Equal(DateTime.Now.Year, stats.IndexTimestamp.Year);
					Assert.True(stats.TotalResults > 0);
				}
			}
		}

		[Fact]
		public void CanGetTotalResultsFromStatisticsOnLazySearchAgainstStaticIndex_NonLazy()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				new UserByFirstName().Execute(store);
				using (var session = store.OpenSession())
				{
					session.Store(new User
					{
						FirstName = "Ayende"
					});
					session.SaveChanges();
				}
				using (var session = store.OpenSession())
				{
					session.Query<User, UserByFirstName>()
						.Customize(x => x.WaitForNonStaleResults())
						.Take(15).ToList();

					RavenQueryStatistics stats;

					var query = session.Query<User, UserByFirstName>().Statistics(out stats).Where(x => x.FirstName == "Ayende");

					var results = query.Take(8).ToList();

					Assert.Equal(1, results.Count());
					Assert.Equal(DateTime.Now.Year, stats.IndexTimestamp.Year);
					Assert.True(stats.TotalResults > 0);
				}
			}
		}

		
	}

	public class User
	{
		public string Id { get; set; }
		public string FirstName { get; set; }
		public string LastName { get; set; }
	}

	public class UserByFirstName : AbstractIndexCreationTask<User>
	{
		public UserByFirstName()
		{
			Map = users => from user in users
						   select new { user.FirstName };
		}
	}
}