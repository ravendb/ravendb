// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2670.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Suggestions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_2670 : RavenTestBase
    {
        public RavenDB_2670(ITestOutputHelper output) : base(output)
        {
        }

        private class Product
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public string Supplier { get; set; }

            public string Category { get; set; }

            public string QuantityPerUnit { get; set; }

            public decimal PricePerUnit { get; set; }

            public int UnitsInStock { get; set; }

            public int UnitsOnOrder { get; set; }

            public bool Discontinued { get; set; }

            public int ReorderLevel { get; set; }
        }

        private class Products_ByName : AbstractIndexCreationTask<Product>
        {
            public Products_ByName()
            {
                Map = products => from product in products
                                  select new
                                  {
                                      product.Name
                                  };

                Indexes.Add(x => x.Name, FieldIndexing.Search);
                Suggestion(x => x.Name);
            }
        }

        [Fact]
        public void MaxSuggestionsShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                new Products_ByName().Execute(store);

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var result = session
                        .Query<Product, Products_ByName>()
                        .SuggestUsing(f => f.ByField("Name", new[] { "chaig", "tof" }).WithOptions(new SuggestionOptions
                        {
                            PageSize = 5,
                            Distance = StringDistanceTypes.JaroWinkler,
                            SortMode = SuggestionSortMode.Popularity,
                            Accuracy = 0.4f
                        }))
                        .Execute();

                    Assert.True(result["Name"].Suggestions.Count <= 5);
                }
            }
        }
    }
}
