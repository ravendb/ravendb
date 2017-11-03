// -----------------------------------------------------------------------
//  <copyright file="LargeFacets.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
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
                        .AggregateBy(x => x.Category)
                        .Execute();

                    var facetResultsB = s.Query<Item, Index>()
                        .Where(x => x.Active)
                        .AggregateBy(x => x.Category)
                        .AndAggregateOn(x => x.Age, f => f.WithRanges(null))
                        .Execute();

                    throw new NotImplementedException();

                    /*
                        .ToFacets(new Facet[]
                        {
                            new Facet
                            {
                                Name = "Category"
                            },
                            new Facet
                            {
                                Name = "Age",
                                Ranges = Enumerable.Range(0,2048).Select(x=> "["+ x + " TO " + (x+1)+"]").ToList()
                            }
                        });
                        */

                    Assert.Equal(facetResultsA["Category"].Values.Count, facetResultsB["Category"].Values.Count);

                    var facetResultsC = s.Query<Item, Index>()
                        .Where(x => x.Active)
                        .AggregateBy(x => x.Category)
                        .AndAggregateOn(x => x.Age, f => f.WithRanges(null))
                        .ExecuteLazy().Value;

                    /*
                    .ToFacetsLazy(new Facet[]
                    {
                        new Facet
                        {
                            Name = "Category"
                        },
                        new Facet
                        {
                            Name = "Age",
                            Ranges = Enumerable.Range(0,2048).Select(x=> "["+ x + " TO " + (x+1)+"]").ToList()
                        }
                    }).Value;
                    */

                    Assert.Equal(facetResultsA["Category"].Values.Count, facetResultsC["Category"].Values.Count);
                }
            }
        }
    }
}
