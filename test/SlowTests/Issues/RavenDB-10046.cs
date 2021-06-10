using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10046 : RavenTestBase
    {
        public RavenDB_10046(ITestOutputHelper output) : base(output)
        {
        }

        private class Home
        {
            public Hero Hero { get; set; }
            public string Id { get; set; }
        }

        private class Hero
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [Fact]
        public void CanLoadWithWrappedParameter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Home(), "homes/1");
                    session.Store(new Hero
                    {
                        Name = "Jerry"
                    }, "heros/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var currentPage = new
                    {
                        Id = "homes/1"
                    };
                    var currentContent = new Home
                    {
                        Hero = new Hero
                        {
                            Id = "heros/1"
                        }
                    };

                    var query = from page in session.Query<Home>()
                                where page.Id == currentPage.Id
                                let hero = RavenQuery.Load<Hero>(currentContent.Hero.Id)
                                select new
                                {
                                    page = page,
                                    Hero = hero
                                };

                    Assert.Equal("from 'Homes' as page where id() = $p0 " +
                                 "load $p1 as hero select { page : page, Hero : hero }",
                                query.ToString());

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal("Jerry", result[0].Hero.Name);
                }
            }
        }

        [Fact]
        public void CanProjectWithWrappedParameter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                ProductName = "Coffee",
                                PricePerUnit = 15,
                                Quantity = 2
                            },
                            new OrderLine
                            {
                                ProductName = "Milk",
                                PricePerUnit = 7,
                                Quantity = 10
                            }
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var line = new OrderLine
                    {
                        PricePerUnit = 15,
                        ProductName = "Coffee",
                    };

                    var query = from o in session.Query<Order>()
                                select new
                                {
                                    Any = o.Lines.Any(x => x.ProductName == line.ProductName),
                                    NestedQuery = o.Lines.Where(x => x.PricePerUnit < line.PricePerUnit).Select(y => y.ProductName).ToList()
                                };

                    Assert.Equal("from 'Orders' as o select { " +
                                 "Any : o.Lines.some(function(x){return x.ProductName===$p0;}), " +
                                 "NestedQuery : o.Lines.filter(function(x){return x.PricePerUnit<$p1;}).map(function(y){return y.ProductName;}) }",
                                 query.ToString());

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);

                    Assert.True(result[0].Any);
                    Assert.Equal(1, result[0].NestedQuery.Count);
                    Assert.Equal("Milk", result[0].NestedQuery[0]);
                }
            }
        }

        [Fact]
        public void CanProjectWithWrappedParameterAndLet()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                ProductName = "Coffee",
                                PricePerUnit = 15,
                                Quantity = 2
                            },
                            new OrderLine
                            {
                                ProductName = "Milk",
                                PricePerUnit = 7,
                                Quantity = 10
                            }
                        }
                    });

                    session.Store(new Company
                    {
                        Name = "HR"
                    }, "companies/1");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var line = new OrderLine
                    {
                        PricePerUnit = 15,
                        ProductName = "Coffee",
                        Discount = (decimal)0.25
                    };
                    var myOrder = new Order
                    {
                        Company = "companies/1"
                    };

                    var query = from o in session.Query<Order>()
                                let totalSpentOnOrder = (Func<Order, decimal>)(order => order.Lines.Sum(x => x.PricePerUnit * x.Quantity * (1 - line.Discount)))
                                select new
                                {
                                    Sum = totalSpentOnOrder(o),
                                    Any = o.Lines.Any(x => x.ProductName == line.ProductName),
                                    NestedQuery = o.Lines.Where(x => x.PricePerUnit < line.PricePerUnit).Select(y => y.ProductName).ToList(),
                                    Company = RavenQuery.Load<Company>(myOrder.Company).Name,
                                };

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(o, $p0, $p1, $p2, $p3) {
	var totalSpentOnOrder = function(order){return order.Lines.map(function(x){return x.PricePerUnit*x.Quantity*(1-$p0);}).reduce(function(a, b) { return a + b; }, 0);};
	return { Sum : totalSpentOnOrder(o), Any : o.Lines.some(function(x){return x.ProductName===$p1;}), NestedQuery : o.Lines.filter(function(x){return x.PricePerUnit<$p2;}).map(function(y){return y.ProductName;}), Company : load($p3).Name };
}
from 'Orders' as o select output(o, $p0, $p1, $p2, $p3)", query.ToString());

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);

                    Assert.Equal(75, result[0].Sum);
                    Assert.True(result[0].Any);
                    Assert.Equal(1, result[0].NestedQuery.Count);
                    Assert.Equal("Milk", result[0].NestedQuery[0]);
                    Assert.Equal("HR", result[0].Company);
                }
            }
        }
    }
}
