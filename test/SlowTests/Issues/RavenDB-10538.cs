using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10538 : RavenTestBase
    {
        private class Foo
        {
            public DateTime? DateIn { get; set; }
        }

        private class FooIndex : AbstractIndexCreationTask<Foo>
        {
            public FooIndex()
            {
                Map = docs => from foo in docs select new { foo.DateIn };
            }
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

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var facets = new List<RangeFacet>
                    {
                        new RangeFacet<Foo>
                        {
                            Ranges = { c => c.DateIn < now - TimeSpan.FromDays(365)}
                        }
                    };

                    var query = session.Query<Foo, FooIndex>().AggregateBy(facets);

                    var oneYearAgo = DateTime.SpecifyKind(now - TimeSpan.FromDays(365), DateTimeKind.Unspecified);

                    Assert.Equal($"from index 'FooIndex' select facet(DateIn < '{oneYearAgo:o}')", query.ToString());

                    var facetResult = query.Execute();

                    Assert.True(facetResult.TryGetValue("DateIn", out var value));
                    Assert.Equal($"DateIn < {oneYearAgo:o}", value.Values[0].Range);
                    Assert.Equal(2, value.Values[0].Count);

                }

            }

        }
    }
}

