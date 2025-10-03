using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12266 : RavenTestBase
    {
        public RavenDB_12266(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public void Test(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Item()
                    {
                        Numbers = new int[] { 1, 2 }
                    });

                    session.Store(new Item()
                    {
                        Numbers = new int[] { 3 }
                    });

                    session.SaveChanges();

                    var results = session.Query<Item>().OrderBy(x => x.Numbers).ToList();

                    Assert.Equal(2, results.Count);
                }
            }
        }

        private class Item
        {
            public string Id { get; set; }

            public int[] Numbers { get; set; }
        }
    }
}
