using Raven.Client.Linq;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs.Queries
{
	public class StatsOnDynamicQueries : LocalClientTest
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

	}
}