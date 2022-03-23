using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace FastTests.Issues
{
    public class RavenDB_16469 : RavenTestBase
    {
        public RavenDB_16469(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanUseLongCount(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User
                    {
                        Name = "Ayende Rahien"
                    });

                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    QueryStatistics stats;

                    var longCount = s.Query<User>()
                        .Statistics(out stats)
                        .Search(u => u.Name, "Ayende")
                        .LongCount();

                    WaitForUserToContinueTheTest(store);

                    Assert.Equal(1, longCount);
                    Assert.Equal("Auto/Users/BySearch(Name)", stats.IndexName);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task CanUseLongCountAsync(Options options)
        {
            using (var store = GetDocumentStore(options))
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

                    var count = await
                        s.Query<User>()
                            .Statistics(out stats)
                            .Search(u => u.Name, "Ayende")
                            .LongCountAsync();

                    Assert.Equal(1, count);
                    Assert.Equal("Auto/Users/BySearch(Name)", stats.IndexName);
                }
            }
        }

    }
}
