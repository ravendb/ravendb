// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2670.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Suggestion;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_2670 : RavenTestBase
    {
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
                store.Admin.Send(new CreateSampleDataOperation());

                new Products_ByName().Execute(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var result = session
                        .Query<Product, Products_ByName>()
                        .Suggest(new SuggestionQuery
                        {
                            Field = "Name",
                            Term = "<<chaig tof>>",
                            Accuracy = 0.4f,
                            MaxSuggestions = 5,
                            Distance = StringDistanceTypes.JaroWinkler,
                            Popularity = true
                        });

                    Assert.True(result.Suggestions.Length <= 5);
                }
            }
        }
    }
}
