using Raven.Client;
using Raven.Client.Linq;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class OptimizedSimpleQueries : RavenTest
	{
		[Fact]
		public void WillUseRavenDocumentsByEntityName()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					RavenQueryStatistics stats;
					session.Query<User>().Statistics(out stats).ToArray();

					Assert.Equal("Raven/DocumentsByEntityName", stats.IndexName);
				}
			}
		}
	}
}