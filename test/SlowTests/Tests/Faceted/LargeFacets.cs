// -----------------------------------------------------------------------
//  <copyright file="LargeFacets.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Xunit;

namespace SlowTests.Tests.Faceted
{
    public class LargeFacets : RavenTestBase
    {
        private class Item
        {
            public string Category;
            public bool Active;
            public int Age;
        }

        private class Index : AbstractIndexCreationTask<Item>
        {
            public Index()
            {
                Map = items =>
                    from item in items
                    select new { item.Active, item.Category, item.Age };

            }
        }

        [Fact]
        public void CanGetSameResult()
        {
            using (var store = GetDocumentStore())
            {
                new Index().Execute(store);

                using (var s = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        s.Store(new Item
                        {
                            Active = i % 2 == 0,
                            Age = i,
                            Category = "cat/" + (i + 1)
                        });
                    }
                    s.SaveChanges();
                }

                WaitForIndexing(store);

                using (var s = store.OpenSession())
                {
                    var facetResultsA = s.Query<Item, Index>()
                        .Where(x => x.Active)
                        .AggregateBy(x => x.ByField(y => y.Category))
                        .Execute();

                    var facet = new RangeFacet()
                    {
                        Ranges = Enumerable.Range(0, 2048).Select(x => $"Age >= {x} AND Age <= {x + 1}").ToList()
                    };

                    var facetResultsB = s.Query<Item, Index>()
                        .Where(x => x.Active)
                        .AggregateBy(x => x.ByField(y => y.Category))
                        .AndAggregateBy(facet)
                        .Execute();

                    Assert.Equal(facetResultsA["Category"].Values.Count, facetResultsB["Category"].Values.Count);

                    var facetResultsC = s.Query<Item, Index>()
                        .Where(x => x.Active)
                        .AggregateBy(x => x.ByField(y => y.Category))
                        .AndAggregateBy(facet)
                        .ExecuteLazy().Value;

                    Assert.Equal(facetResultsA["Category"].Values.Count, facetResultsC["Category"].Values.Count);
                }
            }
        }
    }
}
