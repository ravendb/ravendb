using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_2854 : RavenTestBase
    {
        public RavenDB_2854(ITestOutputHelper output) : base(output)
        {
        }

        private class Dog
        {
            public bool Cute { get; set; }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanGetCountWithoutGettingAllTheData(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenSession())
                {
                    var count = s.Query<Dog>().Count(x => x.Cute);
                    Assert.Equal(0, count);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanGetCountWithoutGettingAllTheDataAsync(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenAsyncSession())
                {
                    var count = await s.Query<Dog>().Where(x => x.Cute).CountAsync();
                    Assert.Equal(0, count);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanGetCountWithoutGettingAllTheDataLazy(Options options)
        {
            using (var store = GetDocumentStore(options))
            {

                using (var s = store.OpenSession())
                {
                    var countCute = s.Query<Dog>().Where(x => x.Cute).CountLazily();
                    var countNotCute = s.Query<Dog>().Where(x => x.Cute == false).CountLazily();
                    Assert.Equal(0, countNotCute.Value);
                    Assert.Equal(0, countCute.Value);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanGetCountWithoutGettingAllTheDataLazyAsync(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenAsyncSession())
                {

                    var countCute = s.Query<Dog>().Where(x => x.Cute).CountLazilyAsync();
                    var countNotCute = s.Query<Dog>().Where(x => x.Cute == false).CountLazilyAsync();

                    Assert.Equal(0, await countNotCute.Value);
                    Assert.Equal(0, await countCute.Value);
                }
            }
        }

    }
}
