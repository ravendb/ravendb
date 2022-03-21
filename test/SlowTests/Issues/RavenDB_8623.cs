using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8623 : RavenTestBase
    {
        public RavenDB_8623(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void OrderByOnGroupingShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                new Orders_ByCompanyMostFrequentShippingCountry().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Orders_ByCompanyMostFrequentShippingCountry.Order
                    {
                        Company = "Company 1",
                        ShipTo = new Orders_ByCompanyMostFrequentShippingCountry.Address
                        {
                            Country = "USA"
                        }
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                using (var session = store.OpenSession())
                {
                    var results = session
                        .Query<Orders_ByCompanyMostFrequentShippingCountry.OrderView, Orders_ByCompanyMostFrequentShippingCountry>()
                        .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.NotNull(results[0].Id);
                    Assert.Equal("Company 1", results[0].Company);
                    Assert.Equal("USA", results[0].ShippingCountry);
                }
            }
        }

        private class Orders_ByCompanyMostFrequentShippingCountry : AbstractIndexCreationTask<Orders_ByCompanyMostFrequentShippingCountry.Order, Orders_ByCompanyMostFrequentShippingCountry.OrderView>
        {
            public Orders_ByCompanyMostFrequentShippingCountry()
            {
                Map = orders => from order in orders
                                select new
                                {
                                    order.Id,
                                    order.Company,
                                    ShippingCountry = order.ShipTo.Country,
                                };
                Reduce = results => from result in results
                                    group result by result.Company
                                    into g
                                    let firstValue = g.FirstOrDefault()
                                    let mostFrequentCountry = g.Where(x => !string.IsNullOrEmpty(x.ShippingCountry)).GroupBy(x => x.ShippingCountry).OrderByDescending(x => x.Count()).First().Key
                                    select new
                                    {
                                        firstValue.Id,
                                        firstValue.Company,
                                        ShippingCountry = mostFrequentCountry,
                                    };
            }

            public class OrderView
            {
                public string Id { get; set; }
                public string Company { get; set; }
                public string ShippingCountry { get; set; }
            }

            public class Address
            {
                public string Line1 { get; set; }
                public string Line2 { get; set; }
                public string City { get; set; }
                public string Region { get; set; }
                public string PostalCode { get; set; }
                public string Country { get; set; }
            }

            public class Order
            {
                public string Id { get; set; }
                public string Company { get; set; }
                public string Employee { get; set; }
                public DateTime OrderedAt { get; set; }
                public DateTime RequireAt { get; set; }
                public DateTime? ShippedAt { get; set; }
                public Address ShipTo { get; set; }
                public string ShipVia { get; set; }
                public decimal Freight { get; set; }
                public List<OrderLine> Lines { get; set; }
            }

            public class OrderLine
            {
                public string Product { get; set; }
                public string ProductName { get; set; }
                public decimal PricePerUnit { get; set; }
                public int Quantity { get; set; }
                public decimal Discount { get; set; }
            }
        }
    }
}
