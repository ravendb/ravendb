using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Documents.Indexing
{
    public class FullTextSearchOnAutoIndex : RavenTestBase
    {
        public FullTextSearchOnAutoIndex(ITestOutputHelper output) : base(output)
        {
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
                    Assert.Equal("Auto/Users/BySearch(Name)", stats.IndexName);
                }

                using (var s = store.OpenAsyncSession())
                {
                    QueryStatistics stats;

                    var count = await s.Query<User>()
                        .Statistics(out stats)
                        .Where(u => u.Name == "Ayende")
                        .CountAsync();

                    Assert.Equal(0, count);
                    Assert.Equal("Auto/Users/BySearch(Name)", stats.IndexName);
                }
            }
        }

        [Fact]
        public async Task CanUseFullTextSearchInAutoMapReduceIndex()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenAsyncSession())
                {
                    await s.StoreAsync(new User
                    {
                        Name = "Ayende Rahien"
                    });

                    await s.StoreAsync(new User
                    {
                        Name = "Ayende Rahien"
                    });

                    await s.SaveChangesAsync();
                }

                using (var s = store.OpenSession())
                {
                    QueryStatistics stats;

                    var results = s.Query<User>()
                        .Statistics(out stats)
                        .Customize(x => x.WaitForNonStaleResults())
                        .GroupBy(x => x.Name)
                        .Select(x => new
                        {
                            Name = x.Key,
                            Count = x.Count(),
                        })
                        .Search(x => x.Name, "Ayende")
                        .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal(2, results[0].Count);
                    Assert.Equal("Ayende Rahien", results[0].Name);

                    Assert.Equal("Auto/Users/ByCountReducedBySearch(Name)", stats.IndexName);
                }

                using (var s = store.OpenSession())
                {
                    QueryStatistics stats;

                    var results = s.Query<User>()
                        .Statistics(out stats)
                        .Customize(x => x.WaitForNonStaleResults())
                        .GroupBy(x => x.Name)
                        .Select(x => new
                        {
                            Name = x.Key,
                            Count = x.Count(),
                        })
                        .Where(x => x.Name == "Ayende")
                        .ToList();

                    Assert.Equal(0, results.Count);

                    Assert.Equal("Auto/Users/ByCountReducedBySearch(Name)", stats.IndexName);
                }
            }
        }
    }
}
