// -----------------------------------------------------------------------
//  <copyright file="LargeFacets.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Client;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Indexes;
using Xunit;

namespace SlowTests.Tests.Faceted
{
    public class LargeFacets : RavenNewTestBase
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

                Sort(x => x.Age, SortOptions.NumericLong);
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
                        .ToFacets(new Facet[]
                        {
                            new Facet<Item>
                            {
                                Name = x => x.Category
                            }
                        });

                    var facetResultsB = s.Query<Item, Index>()
                        .Where(x => x.Active)
                        .ToFacets(new Facet[]
                        {
                            new Facet
                            {
                                Name = "Category"
                            },
                            new Facet
                            {
                                Name = "Age",
                                Ranges = Enumerable.Range(0,2048).Select(x=> "[Dx"+ x + " TO Dx" + (x+1)+"]").ToList()
                            }
                        });

                    Assert.Equal(facetResultsA.Results["Category"].Values.Count, facetResultsB.Results["Category"].Values.Count);

                    var facetResultsC = s.Query<Item, Index>()
                        .Where(x => x.Active)
                        .ToFacetsLazy(new Facet[]
                        {
                            new Facet
                            {
                                Name = "Category"
                            },
                            new Facet
                            {
                                Name = "Age",
                                Ranges = Enumerable.Range(0,2048).Select(x=> "[Dx"+ x + " TO Dx" + (x+1)+"]").ToList()
                            }
                        }).Value;
                    Assert.Equal(facetResultsA.Results["Category"].Values.Count, facetResultsC.Results["Category"].Values.Count);
                }
            }
        }
    }
}
