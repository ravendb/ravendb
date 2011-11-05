using System;
using System.Linq;
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
			CanGetTotalResultsFromStatisticsOnLazySearchAgainstAnIndex(false);
		}

		[Fact]
		public void CanGetTotalResultsFromStatisticsOnLazySearchAgainstStaticIndex()
		{
			CanGetTotalResultsFromStatisticsOnLazySearchAgainstAnIndex(true);
		}

		private void CanGetTotalResultsFromStatisticsOnLazySearchAgainstAnIndex(bool staticIndex)
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8080" }.Initialize())
			{
				new UserByFirstName().Execute(store);

				using (var session = store.OpenSession())
				{
					var names = session.Query<User>().Customize(x=>x.WaitForNonStaleResults())
						.Take(15).ToList();

					RavenQueryStatistics stats;

					var query = staticIndex == false
							? session.Query<User>().Statistics(out stats).Where(x => x.FirstName == "test")
							: session.Query<User, UserByFirstName>().Statistics(out stats).Where(x => x.FirstName == "test");

					var results = query.Take(8).Lazily();

					Assert.Equal(DateTime.Now.Year, stats.IndexTimestamp.Year);
					Assert.True(stats.TotalResults > 0, "The stats should return total results");
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