// -----------------------------------------------------------------------
//  <copyright file="LazyFacets.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.Tests.Faceted
{
    public class LazyFacets : RavenTestBase
    {
        [Fact]
        public void Default_operator_not_honoured_remote_store_ToFacetsLazy()
        {
            using (var store = GetDocumentStore())
            {
                var facetSetup = new FacetSetup
                {
                    Id = "Facets",
                    Facets = new List<Facet>()
                    {
                        new Facet() {FieldName = "Facet1"},
                    },
                };

                using (var session = store.OpenSession())
                {
                    foreach (var foo in session.Query<Foo>())
                    {
                        session.Delete(foo);
                    }

                    session.Store(facetSetup);
                    session.Store(new Foo() { Facet1 = "term1" });
                    session.Store(new Foo() { Facet1 = "term2" });
                    session.SaveChanges();
                }

                new Foos().Execute(store);

                WaitForIndexing(store);


                using (var session = store.OpenSession())
                {
                    var facetResults = session.Advanced.DocumentQuery<Foo, Foos>()
                        .UsingDefaultOperator(QueryOperator.And)
                        .WhereEquals("Facet1", "term1")
                        .WhereEquals("Facet1", "term2")
                        .AggregateUsing("Facets")
                        .Execute();

                    Assert.Equal(facetResults["Facet1"].Values.Count, 0);

                    QueryStatistics stats;
                    var query = session.Advanced.DocumentQuery<Foo, Foos>()
                        .Statistics(out stats)
                        .UsingDefaultOperator(QueryOperator.And)
                        .WhereEquals("Facet1", "term1")
                        .WhereEquals("Facet1", "term2");

                    facetResults = query
                        .AggregateUsing("Facets")
                        .ExecuteLazy().Value;

                    Assert.Equal(facetResults["Facet1"].Values.Count, 0);
                }
            }
        }

        private class Foos : AbstractIndexCreationTask<Foo>
        {
            public Foos()
            {
                Map = foos => from foo in foos
                              select new { foo.Facet1 };
            }
        }

        private class Foo
        {
            public string Id { get; set; }
            public string Facet1 { get; set; }
        }
    }
}
