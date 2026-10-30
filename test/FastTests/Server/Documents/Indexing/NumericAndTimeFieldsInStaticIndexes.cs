using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Server.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Documents.Indexing
{
    public class NumericAndTimeFieldsInStaticIndexes : RavenTestBase
    {
        public NumericAndTimeFieldsInStaticIndexes(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {
            public int Count { get; set; }
            public long Big { get; set; }
            public double Price { get; set; }
            public DateTime At { get; set; }
            public DateTimeOffset AtOffset { get; set; }
            public TimeSpan Span { get; set; }
        }

        private class Items_ByValues : AbstractIndexCreationTask<Item, Items_ByValues.Result>
        {
            public class Result
            {
                public int Count { get; set; }
                public long Big { get; set; }
                public double Price { get; set; }
                public double Total { get; set; }
                public DateTime At { get; set; }
                public DateTimeOffset AtOffset { get; set; }
                public TimeSpan Span { get; set; }
            }

            public Items_ByValues()
            {
                Map = items => from i in items
                               select new Result
                               {
                                   Count = i.Count,
                                   Big = i.Big,
                                   Price = i.Price,
                                   Total = i.Price * i.Count,
                                   At = i.At,
                                   AtOffset = i.AtOffset,
                                   Span = i.Span
                               };
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void RangeQueriesAndSortingOnNumericAndTimeValuesProducedByMap(Options options)
        {
            using var store = GetDocumentStore(options);

            var start = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            using (var session = store.OpenSession())
            {
                for (var i = 0; i < 10; i++)
                {
                    session.Store(new Item
                    {
                        Count = i,
                        Big = long.MaxValue - i,
                        Price = i + 0.5,
                        At = start.AddDays(i),
                        AtOffset = new DateTimeOffset(start.AddDays(i)).ToOffset(TimeSpan.FromHours(2)),
                        Span = TimeSpan.FromMinutes(i * 10)
                    });
                }

                session.SaveChanges();
            }

            new Items_ByValues().Execute(store);
            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                Assert.Equal(4, Query(session).Count(x => x.Count > 5));
                Assert.Equal(3, Query(session).Count(x => x.Big < long.MaxValue - 6));
                Assert.Equal(5, Query(session).Count(x => x.Price >= 5.5));
                Assert.Equal(4, Query(session).Count(x => x.Total > 30)); // 6*6.5, 7*7.5, 8*8.5, 9*9.5
                Assert.Equal(2, Query(session).Count(x => x.At > start.AddDays(7)));
                Assert.Equal(1, Query(session).Count(x => x.At == start.AddDays(3)));
                Assert.Equal(6, Query(session).Count(x => x.AtOffset >= new DateTimeOffset(start.AddDays(4))));
                Assert.Equal(4, Query(session).Count(x => x.Span > TimeSpan.FromMinutes(50)));

                var byTotalDescending = Query(session).OrderByDescending(x => x.Total).Select(x => x.Count).ToList();
                Assert.Equal(Enumerable.Range(0, 10).Reverse(), byTotalDescending);

                var bySpan = Query(session).OrderBy(x => x.Span).Select(x => x.Count).ToList();
                Assert.Equal(Enumerable.Range(0, 10), bySpan);
            }

            IRavenQueryable<Items_ByValues.Result> Query(Raven.Client.Documents.Session.IDocumentSession session) =>
                session.Query<Items_ByValues.Result, Items_ByValues>().Customize(x => x.WaitForNonStaleResults());
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void RangeAndTimeFieldNamesAreBuiltOnceFromFieldName()
        {
            var field = new IndexField { Name = "Price" };

            var names = field.RangeAndTimeNames;
            Assert.Equal("Price" + Constants.Documents.Indexing.Fields.RangeFieldSuffixDouble, names.RangeDouble);
            Assert.Equal("Price" + Constants.Documents.Indexing.Fields.RangeFieldSuffixLong, names.RangeLong);
            Assert.Equal("Price" + Constants.Documents.Indexing.Fields.TimeFieldSuffix, names.Time);
            Assert.Same(names, field.RangeAndTimeNames);
        }
    }
}
