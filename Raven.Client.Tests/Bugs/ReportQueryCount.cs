using Raven.Client.Linq;
using Xunit;
using System.Linq;

namespace Raven.Client.Tests.Bugs
{
    public class ReportQueryCount : LocalClientTest
    {
        [Fact]
        public void CanFindOutWhatTheQueryTotalCountIs()
        {
            using(var store = NewDocumentStore())
            {
                using(var s = store.OpenSession())
                {
                    RavenQueryStatistics stats;
                    s.Query<User>()
                        .Statistics(out stats)
                        .Where(x => x.Name == "ayende")
                        .ToArray();

                    Assert.Equal(0, stats.TotalResults);
                    Assert.False(stats.IsStale);
                }
            }
        }
    }
}