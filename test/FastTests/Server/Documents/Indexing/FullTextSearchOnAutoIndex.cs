using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Xunit;

namespace FastTests.Server.Documents.Indexing
{
    public class FullTextSearchOnAutoIndex : RavenTestBase
    {
        public class User
        {
            public string Name;
        }

        [Fact]
        public async Task CanUseFullTextSearchInAutoIndex()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenAsyncSession())
                {
                    await s.StoreAsync(new User
                    {
                        Name = "Ayende Rahien"
                    });

                    await s.SaveChangesAsync();
                }

                using (var s = store.OpenAsyncSession())
                {
                    QueryStatistics stats;

                    var count = await s.Query<User>()
                        .Statistics(out stats)
                        .Search(u => u.Name, "Ayende")
                        .CountAsync();

                    Assert.Equal(1, count);
                    Assert.Equal("Auto/Users/ByAnalyzed(Name)", stats.IndexName);
                }

                using (var s = store.OpenAsyncSession())
                {
                    QueryStatistics stats;

                    var count = await s.Query<User>()
                        .Statistics(out stats)
                        .Where(u => u.Name == "Ayende")
                        .CountAsync();

                    Assert.Equal(0, count);
                    Assert.Equal("Auto/Users/ByName", stats.IndexName);
                }
            }
        }
    }
}
