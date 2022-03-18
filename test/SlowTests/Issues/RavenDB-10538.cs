using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10538 : RavenTestBase
    {
        public RavenDB_10538(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void FacetWithNullableDateTime()
        {
            using (var store = GetDocumentStore())
            {
                new FooIndex().Execute(store);
                var now = DateTime.Now;

                using (var session = store.OpenSession())
                {
                    session.Store(new Foo());
                    session.Store(new Foo { DateIn = now.AddMonths(-4) });
                    session.Store(new Foo { DateIn = now.AddMonths(-10) });
                    session.Store(new Foo { DateIn = now.AddMonths(-13) });
                    session.Store(new Foo { DateIn = now.AddYears(-2) });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var facets = new List<FacetBase>
                    {
                        new RangeFacet<Foo>
                        {
                            Ranges =
                            {
                                c => c.DateIn < now - TimeSpan.FromDays(365)
                            }
                        }
                    };

                    var query = session.Query<Foo, FooIndex>()
                        .AggregateBy(facets);

                   Assert.Equal("from index 'FooIndex' select facet(DateIn < $p0)", query.ToString());

                    var facetResult = query.Execute(); 

                    Assert.True(facetResult.TryGetValue("DateIn", out var value));

                    var oneYearAgo = DateTime.SpecifyKind(now - TimeSpan.FromDays(365), DateTimeKind.Unspecified);
                    Assert.Equal($"DateIn < {oneYearAgo:o}", value.Values[0].Range);
                    Assert.Equal(2, value.Values[0].Count);
                }
            }

        }

        [Fact]
        public async Task TestGeneratedFacetsTest()
        {
            using (var store = GetDocumentStore())
            {
                new DocsIndex().Execute(store);
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 1; i <= 10; i++)
                    {
                        await session.StoreAsync(new Doc { Id = "doc-" + i, DateVal = new DateTime(2018, 1, i) });
                    }
                    await session.SaveChangesAsync();

                    Indexes.WaitForIndexing(store);

                    var query = session.Query<Doc, DocsIndex>()
                        .AggregateBy(builder => builder.ByRanges(
                            x => x.DateVal < new DateTime(2018, 1, 1),
                            x => x.DateVal >= new DateTime(2018, 1, 1) && x.DateVal < new DateTime(2018, 1, 2),
                            x => x.DateVal >= new DateTime(2018, 1, 2) && x.DateVal < new DateTime(2018, 1, 3),
                            x => x.DateVal >= new DateTime(2018, 1, 3) && x.DateVal < new DateTime(2018, 1, 4),
                            x => x.DateVal >= new DateTime(2018, 1, 4) && x.DateVal < new DateTime(2018, 1, 5),
                            x => x.DateVal >= new DateTime(2018, 1, 5)
                            ));
                    var results = await query.ExecuteAsync();
                    var counts = results.First().Value.Values.Select(x => x.Count).ToArray();

                    Assert.Equal(new[] { 0, 1, 1, 1, 1, 6 }, counts); // works for both DateTime? and DateTime

                    var bounds = Enumerable.Range(1, 5).Select(day => new DateTime(2018, 1, day)).ToArray();

                    var ranges = new Expression<Func<Doc, bool>>[] { x => x.DateVal < bounds[0] }
                        .Concat(bounds.Zip(bounds.Skip(1), (start, end) => (Expression<Func<Doc, bool>>)(x => x.DateVal >= start && x.DateVal < end)))
                        .Concat(new Expression<Func<Doc, bool>>[] { x => x.DateVal >= bounds.Last() })
                        .ToArray();

                    var generatedQuery = session.Query<Doc, DocsIndex>()
                        .AggregateBy(builder => builder.ByRanges(
                            ranges[0],
                            ranges.Skip(1).ToArray()
                            ));
                    var generatedResults = await query.ExecuteAsync(); // throws for DateTime?
                    var generatedCounts = results.First().Value.Values.Select(x => x.Count).ToArray();

                    Assert.Equal(new[] { 0, 1, 1, 1, 1, 6 }, generatedCounts);
                }
            }
        }

        private class Foo
        {
            public DateTime? DateIn { get; set; }
            public int Age { get; set; }
        }

        private class FooIndex : AbstractIndexCreationTask<Foo>
        {
            public FooIndex()
            {
                Map = docs => from foo in docs select new { foo.DateIn, foo.Age };
            }
        }

        private class Doc
        {
            public string Id { get; set; }
            public DateTime? DateVal { get; set; } // generated query works for DateTime, manual implementation works for both DateTime? and DateTime
        }

        private class DocsIndex : AbstractIndexCreationTask<Doc>
        {
            public DocsIndex()
            {
                Map = docs =>
                    from doc in docs
                    select new
                    {
                        doc.Id,
                        doc.DateVal,
                    };
            }
        }
    }
}

