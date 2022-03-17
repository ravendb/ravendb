using System.Collections.Generic;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14952 : RavenTestBase
    {
        public RavenDB_14952(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanDoFacetQueryWithAliasOnFullRange_WhenAliasIsTheSameAsOneOfTheIndexFields()
        {
            using (var store = GetDocumentStore())
            {
                new Orders_Totals().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Company = "C1",
                        Employee = "E1",
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                Quantity = 1,
                                PricePerUnit = 10,
                                Discount = 0
                            },
                            new OrderLine
                            {
                                Quantity = 3,
                                PricePerUnit = 30,
                                Discount = 0
                            }
                        }
                    });

                    session.Store(new Order
                    {
                        Company = "C2",
                        Employee = "E2",
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                Quantity = 2,
                                PricePerUnit = 20,
                                Discount = 0
                            },
                            new OrderLine
                            {
                                Quantity = 4,
                                PricePerUnit = 40,
                                Discount = 0
                            }
                        }
                    });

                    session.Store(new Order
                    {
                        Company = "C3",
                        Employee = "E3",
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                Quantity = 3,
                                PricePerUnit = 30,
                                Discount = 0
                            },
                            new OrderLine
                            {
                                Quantity = 5,
                                PricePerUnit = 50,
                                Discount = 0
                            }
                        }
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var commands = store.Commands())
                {
                    var result = commands.Query(new Raven.Client.Documents.Queries.IndexQuery
                    {
                        Query = @"
from index 'Orders/Totals'
where Total < 1000
select facet(sum(Total)) as Total"
                    });

                    var facetResult = (FacetResult)DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable(typeof(FacetResult), (BlittableJsonReaderObject)result.Results[0], "facet/result");

                    Assert.Equal("Total", facetResult.Name);
                    Assert.Equal(1, facetResult.Values.Count);
                    Assert.Equal(Constants.Documents.Querying.Facet.AllResults, facetResult.Values[0].Range);
                    Assert.Equal(3, facetResult.Values[0].Count);
                }
            }
        }

        private class Orders_Totals : AbstractIndexCreationTask<Order>
        {
            public class Result
            {
                public string Employee { get; set; }

                public string Company { get; set; }

                public decimal Total { get; set; }
            }

            public Orders_Totals()
            {
                Map = orders => from order in orders
                                select new
                                {
                                    order.Employee,
                                    order.Company,
                                    Total = order.Lines.Sum(l => (l.Quantity * l.PricePerUnit) * (1 - l.Discount))
                                };
            }
        }
    }
}
