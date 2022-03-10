// -----------------------------------------------------------------------
//  <copyright file="CustomizingIndexQuery.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using FastTests;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs
{
    public class CustomizingIndexQuery : RavenTestBase
    {
        public CustomizingIndexQuery(ITestOutputHelper output) : base(output)
        {
        }
        
        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Skip = "Distinct")]

        public void CanSkipTransformResults(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new PurchaseHistoryIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Shipment
                    {
                        UserId = "users/ayende",
                        Items = new List<ShipmentItem>
                        {
                            new ShipmentItem {ProductId = "products/123"},
                            new ShipmentItem {ProductId = "products/312"},
                            new ShipmentItem {ProductId = "products/243"}
                        },
                        Destination = new Address
                        {
                            Town = "Hadera"
                        }
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var q = session.Query<Shipment, PurchaseHistoryIndex>()
                           .Customize(c => c.WaitForNonStaleResults())
                           .Single();

                    Assert.Equal("Hadera", q.Destination.Town);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Skip = "Distinct")]
        public void CanSkipTransformResults_Lucene(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new PurchaseHistoryIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Shipment
                    {
                        UserId = "users/ayende",
                        Items = new List<ShipmentItem>
                        {
                            new ShipmentItem {ProductId = "products/123"},
                            new ShipmentItem {ProductId = "products/312"},
                            new ShipmentItem {ProductId = "products/243"}
                        },
                        Destination = new Address
                        {
                            Town = "Hadera"
                        }
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var q = session.Advanced.DocumentQuery<Shipment, PurchaseHistoryIndex>()
                            .WaitForNonStaleResults()
                           .Single();

                    Assert.Equal("Hadera", q.Destination.Town);
                }

            }
        }

        private class PurchaseHistoryIndex : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinitionBuilder<Shipment, Shipment>(IndexName)
                {
                    Map = docs => from doc in docs
                                  from product in doc.Items
                                  select new
                                  {
                                      UserId = doc.UserId,
                                      ProductId = product.ProductId
                                  },
                }.ToIndexDefinition(Conventions);
            }
        }

        private class Shipment
        {
            public string Id { get; set; }
            public string UserId { get; set; }
            public Address Destination { get; set; }
            public List<ShipmentItem> Items { get; set; }

            public Shipment()
            {
                Items = new List<ShipmentItem>();
            }           
        }

        private class Address
        {
            public string Name { get; set; }
            public string AddressLineOne { get; set; }
            public string AddressLineTwo { get; set; }
            public string AddressLineThree { get; set; }
            public string Town { get; set; }
            public string Region { get; set; }
            public string AreaCode { get; set; }
            public string Country { get; set; }
        }

        private class ShipmentItem
        {
            public string Price { get; set; }
            public string Name { get; set; }
            public string ProductId { get; set; }
        }

    }
}
