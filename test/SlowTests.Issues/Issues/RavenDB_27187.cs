using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_27187 : RavenTestBase
    {
        public RavenDB_27187(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {
            public int Num { get; set; }
        }

        private class Items_ByNum : AbstractIndexCreationTask<Item>
        {
            public Items_ByNum()
            {
                Map = items => from i in items
                               select new
                               {
                                   i.Num
                               };
            }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void OrderByAsStringMustBeHonoredWhenAFilterOnTheSameFieldDrivesTheQuery(Options options)
        {
            using var store = GetDocumentStore(options);

            using (var session = store.OpenSession())
            {
                session.Store(new Item { Num = 2 });
                session.Store(new Item { Num = 10 });
                session.Store(new Item { Num = 100 });
                session.SaveChanges();
            }

            new Items_ByNum().Execute(store);
            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                // Lexicographically "10" < "100" < "2", which is a different order than the numeric 2, 10, 100.
                // The range filter is on the very field we order by, and that must not turn the requested string
                // ordering into the numeric order of the scanned range.
                var byString = session.Advanced
                    .RawQuery<Item>("from index \"Items/ByNum\" where Num between $low and $high order by Num as string")
                    .AddParameter("low", 1)
                    .AddParameter("high", 1000)
                    .ToList();

                Assert.Equal(new[] { 10, 100, 2 }, byString.Select(x => x.Num));

                // Control: without a filter competing for the sort field the string ordering already worked.
                var noFilter = session.Advanced
                    .RawQuery<Item>("from index \"Items/ByNum\" order by Num as string")
                    .ToList();

                Assert.Equal(new[] { 10, 100, 2 }, noFilter.Select(x => x.Num));

                // Control: an explicitly numeric ordering must stay numeric.
                var byLong = session.Advanced
                    .RawQuery<Item>("from index \"Items/ByNum\" where Num between $low and $high order by Num as long")
                    .AddParameter("low", 1)
                    .AddParameter("high", 1000)
                    .ToList();

                Assert.Equal(new[] { 2, 10, 100 }, byLong.Select(x => x.Num));
            }
        }

        private class DoubleItem
        {
            public double Num { get; set; }
        }

        private class DoubleItems_ByNum : AbstractIndexCreationTask<DoubleItem>
        {
            public DoubleItems_ByNum()
            {
                Map = items => from i in items
                               select new
                               {
                                   i.Num
                               };
            }
        }

        private void StoreDoubleItems(Options options, out IDocumentStore store)
        {
            store = GetDocumentStore(options);

            using (var session = store.OpenSession())
            {
                session.Store(new DoubleItem { Num = 1.1 });
                session.Store(new DoubleItem { Num = 1.8 });
                session.Store(new DoubleItem { Num = 1.5 });
                session.SaveChanges();
            }

            new DoubleItems_ByNum().Execute(store);
            Indexes.WaitForIndexing(store);
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void OrderByAsDoubleMustBeHonoredWhenAnIntegerRangeOnTheSameFieldDrivesTheQuery(Options options)
        {
            StoreDoubleItems(options, out var store);
            using (store)
            using (var session = store.OpenSession())
            {
                // The range terms are integers while the ordering is by the double value: the integer tree groups
                // 1.1/1.5/1.8 under a single term, so walking it says nothing about their double order.
                var byRange = session.Advanced
                    .RawQuery<DoubleItem>("from index \"DoubleItems/ByNum\" where Num between $low and $high order by Num as double")
                    .AddParameter("low", 1)
                    .AddParameter("high", 2)
                    .ToList();

                Assert.Equal(3, byRange.Count);
                Assert.Equal(new[] { 1.1, 1.5, 1.8 }, byRange.Select(x => x.Num));
            }
        }
    }
}
