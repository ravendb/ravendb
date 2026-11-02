using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_27458 : RavenTestBase
    {
        public RavenDB_27458(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void NullMustNotSatisfyLessThanWhenTheComparisonIsAPostFilter(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new Items_ByTAndN().Execute(store);

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < 100; i++)
                        bulk.Store(new Item { T = "x", N = i % 10 });

                    for (int i = 0; i < 30; i++)
                        bulk.Store(new Item { T = "x", N = null });

                    for (int i = 0; i < 10; i++)
                        bulk.Store(new Item { T = "y", N = 1 });
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    // the clause on T makes the comparison run as a post-filter over the entries
                    Assert.Equal(30, Count(session, "T = 'x' and N < 3"));
                    Assert.Equal(40, Count(session, "T = 'x' and N <= 3"));

                    // a null keeps failing > and keeps being found by equality
                    Assert.Equal(20, Count(session, "T = 'x' and N > 7"));
                    Assert.Equal(30, Count(session, "T = 'x' and N = null"));

                    // between and the standalone range were correct before the fix as well
                    Assert.Equal(30, Count(session, "T = 'x' and N between 2 and 4"));
                    Assert.Equal(40, Count(session, "N < 3"));
                }
            }

            static int Count(IDocumentSession session, string where)
            {
                return session.Advanced.RawQuery<Item>($"from index 'Items/ByTAndN' where {where}").ToList().Count;
            }
        }

        private class Item
        {
            public string T { get; set; }

            public int? N { get; set; }
        }

        private class Items_ByTAndN : AbstractIndexCreationTask<Item>
        {
            public Items_ByTAndN()
            {
                Map = items => from item in items
                               select new { item.T, item.N };
            }
        }
    }
}
