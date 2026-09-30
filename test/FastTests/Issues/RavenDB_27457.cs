using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_27457 : RavenTestBase
    {
        public RavenDB_27457(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void TwoUpperBoundsOnTheSameFieldMustNotBeMergedIntoBetween(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new Items_ByN().Execute(store);

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                        session.Store(new Item { N = i }, "items/" + i);

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    Assert.Equal(3, Count(session, "N < 3"));
                    Assert.Equal(3, Count(session, "N < 3 and N < 5"));
                    Assert.Equal(3, Count(session, "N < 5 and N < 3"));
                    Assert.Equal(4, Count(session, "N <= 3 and N <= 5"));

                    // two lower bounds were never merged - they must stay that way
                    Assert.Equal(1, Count(session, "N > 6 and N > 8"));

                    // an actual range is still merged into a between query
                    Assert.Equal(2, Count(session, "N > 3 and N < 6"));
                }
            }

            static int Count(IDocumentSession session, string where)
            {
                return session.Advanced.RawQuery<Item>($"from index 'Items/ByN' where {where}").ToList().Count;
            }
        }

        private class Item
        {
            public int N { get; set; }
        }

        private class Items_ByN : AbstractIndexCreationTask<Item>
        {
            public Items_ByN()
            {
                Map = items => from item in items
                               select new { item.N };
            }
        }
    }
}
