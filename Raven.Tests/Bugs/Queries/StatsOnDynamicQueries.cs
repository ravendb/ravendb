using Raven.Client.Linq;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs.Queries
{
	public class StatsOnDynamicQueries : RavenTest
	{
		[Fact]
		public void WillGiveStats()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User
					{
						Age = 15,
						Email = "ayende"
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					RavenQueryStatistics stats;
					session.Query<User>()
						.Statistics(out stats)
						.Where(x=>x.Email == "ayende")
						.ToArray();

					Assert.NotEqual(0, stats.TotalResults);
				}
			}
		}

		[Fact]
		public void WillGiveStatsForLuceneQuery()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User
					{
						Age = 15,
						Email = "ayende"
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					RavenQueryStatistics stats;
					var query = session.Advanced.LuceneQuery<User>()
						.Statistics(out stats)
						.Where("Email:ayende");

					var result = query.ToArray();
					Assert.NotEqual(0, stats.TotalResults);
					Assert.Equal(stats.TotalResults, query.QueryResult.TotalResults);
				}
			}
		}
	}
}