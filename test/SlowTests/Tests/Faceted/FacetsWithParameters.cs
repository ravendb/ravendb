
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Xunit;

namespace SlowTests.Tests.Faceted
{
    public class FacetsWithParameters : RavenTestBase
    {
        private class Foo
        {
            public DateTime? DateIn { get; set; }
            public DateTime? DateIn2 { get; set; }
            public int Age { get; set; }
        }

        private class FooIndex : AbstractIndexCreationTask<Foo>
        {
            public FooIndex()
            {
                Map = docs => from foo in docs select new { foo.DateIn, foo.DateIn2, foo.Age };
            }
        }

        private static void StoreSampleData(DocumentStore store, DateTime now)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Foo());
                session.Store(new Foo { DateIn = now.AddMonths(-4) });
                session.Store(new Foo { DateIn = now.AddMonths(-10) });
                session.Store(new Foo { DateIn = now.AddMonths(-13) });
                session.Store(new Foo { DateIn = now.AddYears(-2) });

                session.SaveChanges();
            }
        }

        private static void AssertResults(IReadOnlyDictionary<string, FacetResult> facetResult, string key, DateTime now)
        {
            Assert.True(facetResult.TryGetValue(key, out var value));

            var oneYearAgo = DateTime.SpecifyKind(now - TimeSpan.FromDays(365), DateTimeKind.Unspecified);
            var sixMonthsAgo = DateTime.SpecifyKind(now - TimeSpan.FromDays(180), DateTimeKind.Unspecified);

            Assert.Equal($"{key} < {oneYearAgo:o}", value.Values[0].Range);
            Assert.Equal(2, value.Values[0].Count);
            Assert.Equal($"{key} >= {oneYearAgo:o} and {key} < {sixMonthsAgo:o}", value.Values[1].Range);
            Assert.Equal(1, value.Values[1].Count);
            Assert.Equal($"{key} >= {sixMonthsAgo:o}", value.Values[2].Range);
            Assert.Equal(1, value.Values[2].Count);
        }


        [Fact]
        public void FacetShouldUseParameters_WithFacetBaseList()
        {
            using (var store = GetDocumentStore())
            {
                new FooIndex().Execute(store);

                var now = DateTime.Now;
                StoreSampleData(store, now);
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var facets = new List<FacetBase>
                    {
                        new RangeFacet<Foo>
                        {
                            Ranges =
                            {
                                c => c.DateIn < now - TimeSpan.FromDays(365), //2 hits
                                c => c.DateIn >= now - TimeSpan.FromDays(365) && c.DateIn < now - TimeSpan.FromDays(180), //1 hit
                                c => c.DateIn >= now - TimeSpan.FromDays(180) //1 hit
                            }
                        }
                    };

                    var query = session.Query<Foo, FooIndex>()
                        .Where(x => x.DateIn != null && x.Age < 90)
                        .AggregateBy(facets);

                    Assert.Equal("from index 'FooIndex' where (DateIn != $p0 and Age < $p1) select " +
                                 "facet(DateIn < $p2, DateIn >= $p3 and DateIn < $p4, DateIn >= $p5)"
                                , query.ToString());

                    var facetResult = query.Execute();

                    AssertResults(facetResult, "DateIn", now);

                }
            }
        }

        [Fact]
        public void FacetShouldUseParameters_WithTypedRangeFacetList()
        {
            using (var store = GetDocumentStore())
            {
                new FooIndex().Execute(store);

                var now = DateTime.Now;
                StoreSampleData(store, now);
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {

                    var facets = new List<RangeFacet<Foo>>
                    {
                        new RangeFacet<Foo>
                        {
                            Ranges =
                            {
                                c => c.DateIn < now - TimeSpan.FromDays(365), 
                                c => c.DateIn >= now - TimeSpan.FromDays(365) && c.DateIn < now - TimeSpan.FromDays(180), 
                                c => c.DateIn >= now - TimeSpan.FromDays(180) 
                            }
                        }
                    };

                    var query = session.Query<Foo, FooIndex>()
                        .Where(x => x.DateIn != null && x.Age < 90)
                        .AggregateBy(facets);

                    Assert.Equal("from index 'FooIndex' where (DateIn != $p0 and Age < $p1) select " +
                                 "facet(DateIn < $p2, DateIn >= $p3 and DateIn < $p4, DateIn >= $p5)"
                                , query.ToString());

                    var facetResult = query.Execute();

                    AssertResults(facetResult, "DateIn", now);

                }
            }
        }

        [Fact]
        public void FacetShouldUseParameters_WithIFacetBuilder()
        {
            using (var store = GetDocumentStore())
            {
                new FooIndex().Execute(store);

                var now = DateTime.Now;
                StoreSampleData(store, now);
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session
                        .Query<Foo, FooIndex>()
                        .Where(x => x.DateIn != null && x.Age < 90)
                        .AggregateBy(builder => builder
                            .ByRanges(
                                c => c.DateIn < now - TimeSpan.FromDays(365),
                                c => c.DateIn >= now - TimeSpan.FromDays(365) && c.DateIn < now - TimeSpan.FromDays(180),
                                c => c.DateIn >= now - TimeSpan.FromDays(180)));


                    Assert.Equal("from index 'FooIndex' where (DateIn != $p0 and Age < $p1) select " +
                                 "facet(DateIn < $p2, DateIn >= $p3 and DateIn < $p4, DateIn >= $p5)"
                                , query.ToString());

                    var facetResult = query.Execute();

                    AssertResults(facetResult, "DateIn", now);
                }
            }
        }

        [Fact]
        public void TypedRangeFacetWithUntypedRangeFacetList_ShouldNotUseParameters()
        {
            using (var store = GetDocumentStore())
            {
                new FooIndex().Execute(store);

                var now = DateTime.Now;
                var oneYearAgo = DateTime.SpecifyKind(now - TimeSpan.FromDays(365), DateTimeKind.Unspecified);
                var sixMonthsAgo = DateTime.SpecifyKind(now - TimeSpan.FromDays(180), DateTimeKind.Unspecified);

                StoreSampleData(store, now);
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var facets = new List<RangeFacet>
                    {
                        //this facet will not use query-parameters when excecuting a query, due to the
                        //implicit conversion that exists between RangeFacet<Foo> and RangeFacet

                        new RangeFacet<Foo>
                        {
                            Ranges =
                            {
                                c => c.DateIn < now - TimeSpan.FromDays(365),
                                c => c.DateIn >= now - TimeSpan.FromDays(365) && c.DateIn < now - TimeSpan.FromDays(180),
                                c => c.DateIn >= now - TimeSpan.FromDays(180)
                            }
                        }
                    };

                    var query = session.Query<Foo, FooIndex>()
                        .Where(x => x.DateIn != null && x.Age < 90)
                        .AggregateBy(facets);


                    Assert.Equal("from index 'FooIndex' where (DateIn != $p0 and Age < $p1) " +
                                 $"select facet(DateIn < '{oneYearAgo:o}', DateIn >= '{oneYearAgo:o}' and DateIn < '{sixMonthsAgo:o}'," +
                                 $" DateIn >= '{sixMonthsAgo:o}')"
                                , query.ToString());

                    var facetResult = query.Execute();

                    AssertResults(facetResult, "DateIn", now);

                }
            }
        }

        [Fact]
        public void FacetSetupDocument_ShouldNotUseParameters()
        {
            using (var store = GetDocumentStore())
            {
                new FooIndex().Execute(store);

                var now = DateTime.Now;
                var oneYearAgo = DateTime.SpecifyKind(now - TimeSpan.FromDays(365), DateTimeKind.Unspecified);
                var sixMonthsAgo = DateTime.SpecifyKind(now - TimeSpan.FromDays(180), DateTimeKind.Unspecified);

                StoreSampleData(store, now);
                WaitForIndexing(store);

                using (var s = store.OpenSession())
                {
                    var facets = new List<RangeFacet>
                    {
                        new RangeFacet<Foo>
                        {
                            Ranges =
                            {
                                c => c.DateIn < now - TimeSpan.FromDays(365),
                                c => c.DateIn >= now - TimeSpan.FromDays(365) && c.DateIn < now - TimeSpan.FromDays(180),
                                c => c.DateIn >= now - TimeSpan.FromDays(180)
                            }
                        }
                    };

                    var facetSetupDoc = new FacetSetup { Id = "facets/FooFacets", RangeFacets = facets };
                    s.Store(facetSetupDoc);
                    s.SaveChanges();

                    var rangeFacet = facetSetupDoc.RangeFacets[0];

                    Assert.Equal($"DateIn < '{oneYearAgo:o}'", rangeFacet.Ranges[0]);
                    Assert.Equal($"DateIn >= '{oneYearAgo:o}' and DateIn < '{sixMonthsAgo:o}'", rangeFacet.Ranges[1]);
                    Assert.Equal($"DateIn >= '{sixMonthsAgo:o}'", rangeFacet.Ranges[2]);
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Foo, FooIndex>()
                        .Where(x => x.DateIn != null && x.Age < 90)
                        .AggregateUsing("facets/FooFacets");

                    Assert.Equal("from index 'FooIndex' where (DateIn != $p0 and Age < $p1) " +
                                 "select facet(id('facets/FooFacets'))"
                                , query.ToString());

                    var facetResult = query.Execute();

                    AssertResults(facetResult, "DateIn", now);
                }

                using (var session = store.OpenSession())
                {
                    var facets = session.Load<FacetSetup>("facets/FooFacets").RangeFacets;

                    var query = session.Query<Foo, FooIndex>()
                        .Where(x => x.DateIn != null && x.Age < 90)
                        .AggregateBy(facets);

                    Assert.Equal("from index 'FooIndex' where (DateIn != $p0 and Age < $p1) " +
                                 $"select facet(DateIn < '{oneYearAgo:o}', DateIn >= '{oneYearAgo:o}' and DateIn < '{sixMonthsAgo:o}', DateIn >= '{sixMonthsAgo:o}')"
                                , query.ToString());

                    var facetResult = query.Execute();

                    AssertResults(facetResult, "DateIn", now);
                }
            }
        }

        [Fact]
        public async Task TwoDifferentAsyncQueriesThatAreUsingTheSameFacetWithParametersShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                new FooIndex().Execute(store);

                var now = DateTime.Now;
                StoreSampleData(store, now);
                WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var facets = new List<RangeFacet<Foo>>
                    {
                        new RangeFacet<Foo>
                        {
                            Ranges =
                            {
                                c => c.DateIn < now - TimeSpan.FromDays(365), 
                                c => c.DateIn >= now - TimeSpan.FromDays(365) && c.DateIn < now - TimeSpan.FromDays(180), 
                                c => c.DateIn >= now - TimeSpan.FromDays(180) 
                            }
                        }
                    };

                    var query1 = session.Query<Foo, FooIndex>()
                        .Where(x => x.DateIn != null && x.Age < 90)
                        .AggregateBy(facets);

                    var query2 = session.Query<Foo, FooIndex>()
                        .Where(x => x.DateIn != null)
                        .AggregateBy(facets);

                    Assert.Equal("from index 'FooIndex' where (DateIn != $p0 and Age < $p1) select " +
                                    "facet(DateIn < $p2, DateIn >= $p3 and DateIn < $p4, DateIn >= $p5)"
                                , query1.ToString());

                    Assert.Equal("from index 'FooIndex' where DateIn != $p0 select " +
                                 "facet(DateIn < $p1, DateIn >= $p2 and DateIn < $p3, DateIn >= $p4)"
                                , query2.ToString());

                    var facetResult1 = await query1.ExecuteAsync();
                    var facetResult2 = await query2.ExecuteAsync();

                    AssertResults(facetResult1, "DateIn", now);
                    AssertResults(facetResult2, "DateIn", now);

                }
            }
        }

        [Fact]
        public void QueryUsingMultipuleFacetsWithParametersShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                new FooIndex().Execute(store);

                var now = DateTime.Now;
                using (var session = store.OpenSession())
                {
                    session.Store(new Foo());
                    session.Store(new Foo { DateIn = now.AddMonths(-4), DateIn2 = now.AddMonths(-4) });
                    session.Store(new Foo { DateIn = now.AddMonths(-10), DateIn2 = now.AddMonths(-10) });
                    session.Store(new Foo { DateIn = now.AddMonths(-13), DateIn2 = now.AddMonths(-13) });
                    session.Store(new Foo { DateIn = now.AddYears(-2), DateIn2 = now.AddYears(-2) });

                    session.SaveChanges();
                }
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    //new List<RangeFacet>
                    var facets = new List<RangeFacet<Foo>>
                    {
                        new RangeFacet<Foo>
                        {
                            Ranges =
                            {
                                c => c.DateIn < now - TimeSpan.FromDays(365),
                                c => c.DateIn >= now - TimeSpan.FromDays(365) && c.DateIn < now - TimeSpan.FromDays(180),
                                c => c.DateIn >= now - TimeSpan.FromDays(180)
                            }
                        },
                        new RangeFacet<Foo>
                        {
                            Ranges =
                            {
                                c => c.DateIn2 < now - TimeSpan.FromDays(365),
                                c => c.DateIn2 >= now - TimeSpan.FromDays(365) && c.DateIn2 < now - TimeSpan.FromDays(180),
                                c => c.DateIn2 >= now - TimeSpan.FromDays(180)
                            }
                        }
                    };

                    var query = session.Query<Foo, FooIndex>()
                        .Where(x => x.DateIn != null && x.Age < 90)
                        .AggregateBy(facets);

                    Assert.Equal("from index 'FooIndex' where (DateIn != $p0 and Age < $p1) select " +
                                 "facet(DateIn < $p2, DateIn >= $p3 and DateIn < $p4, DateIn >= $p5), " +
                                 "facet(DateIn2 < $p6, DateIn2 >= $p7 and DateIn2 < $p8, DateIn2 >= $p9)"
                                , query.ToString());

                    var facetResult = query.Execute();

                    Assert.Equal(2, facetResult.Count);

                    AssertResults(facetResult, "DateIn", now);
                    AssertResults(facetResult, "DateIn2", now);

                }
            }
        }
    }


}
