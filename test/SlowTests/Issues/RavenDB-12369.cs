using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Linq;
using Xunit;
using Order = Tests.Infrastructure.Entities.Order;
using OrderLine = Tests.Infrastructure.Entities.OrderLine;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12369 : RavenTestBase
    {
        public RavenDB_12369(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanUseOrderByNumberInProjection()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new Order
                    {
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                Quantity = 30
                            },
                            new OrderLine
                            {
                                Quantity = 10
                            },
                            new OrderLine
                            {
                                Quantity = 20
                            }
                        }
                    });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var q = Queryable.Select(s.Query<Order>(), x => new
                    {
                        SortedLines = x.Lines.OrderBy(l => l.Quantity).ToList()
                    });

                    var result = q.First();

                    Assert.Equal("from 'Orders' as x select " +
                                 "{ SortedLines : x.Lines.sort(" +
                                 "function (a, b){ return a.Quantity - b.Quantity;}) }"
                        , q.ToString());

                    Assert.Equal(10, result.SortedLines[0].Quantity);
                    Assert.Equal(20, result.SortedLines[1].Quantity);
                    Assert.Equal(30, result.SortedLines[2].Quantity);
                }
            }
        }

        [Fact]
        public void CanUseOrderByStringInProjection()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new Order
                    {
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                ProductName = "cheese"
                            },
                            new OrderLine
                            {
                                ProductName = "avocado"
                            },
                            new OrderLine
                            {
                                ProductName = "beer"
                            }
                        }
                    });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var q = Queryable.Select(s.Query<Order>(), x => new
                    {
                        SortedLines = x.Lines.OrderBy(l => l.ProductName).ToList()
                    });

                    Assert.Equal("from 'Orders' as x select { " +
                                 "SortedLines : x.Lines.sort(function (a, b){ " +
                                 "return ((a.ProductName < b.ProductName) " +
                                 "? -1 : (a.ProductName > b.ProductName)? 1 : 0);}) }"
                        , q.ToString());

                    var result = q.First();

                    Assert.Equal("avocado", result.SortedLines[0].ProductName);
                    Assert.Equal("beer", result.SortedLines[1].ProductName);
                    Assert.Equal("cheese", result.SortedLines[2].ProductName);


                }
            }
        }

        [Fact]
        public void CanUseOrderByDateInProjection()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new MultiOrder
                    {
                        Orders = new List<Order>()
                        {
                            new Order
                            {
                                Company = "Companies/1",
                                OrderedAt = DateTime.Now.AddDays(10)
                            },
                            new Order
                            {
                                Company = "Companies/2",
                                OrderedAt = DateTime.Now.AddDays(-10)
                            },
                            new Order
                            {
                                Company = "Companies/3",
                                OrderedAt = DateTime.Now
                            }
                        }

                    });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var q = Queryable.Select(s.Query<MultiOrder>(), x => new
                    {
                        SortedOrdersByDate = x.Orders.OrderBy(o => o.OrderedAt).ToList()
                    });

                    Assert.Equal("from 'MultiOrders' as x select { " +
                                 "SortedOrdersByDate : x.Orders.sort(function (a, b){ " +
                                 "return ((a.OrderedAt < b.OrderedAt) " +
                                 "? -1 : (a.OrderedAt > b.OrderedAt)? 1 : 0);}) }"
                        , q.ToString());

                    var result = q.First();

                    Assert.Equal("Companies/2", result.SortedOrdersByDate[0].Company);
                    Assert.Equal("Companies/3", result.SortedOrdersByDate[1].Company);
                    Assert.Equal("Companies/1", result.SortedOrdersByDate[2].Company);


                }
            }
        }

        [Fact]
        public void CanUseOrderByNestedNumberInProjection()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new MultiOrder
                    {
                        Info = new List<ShipmentInfo>
                        {
                            new ShipmentInfo
                            {
                                Address = new Address2
                                {
                                    ZipCode = 3
                                }
                            },
                            new ShipmentInfo
                            {
                                Address = new Address2
                                {
                                    ZipCode = 1
                                }
                            },
                            new ShipmentInfo
                            {
                                Address = new Address2
                                {
                                    ZipCode = 2
                                }
                            }
                        }

                    });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var q = Queryable.Select(s.Query<MultiOrder>(), x => new
                    {
                        OrderedBy = x.Info.OrderBy(i => i.Address.ZipCode).ToList()
                    });
                    Assert.Equal("from 'MultiOrders' as x select { " +
                                 "OrderedBy : x.Info.sort(" +
                                 "function (a, b){ return a.Address.ZipCode - b.Address.ZipCode;}) }"
                        , q.ToString());

                    var result = q.First();

                    Assert.Equal(1, result.OrderedBy[0].Address.ZipCode);
                    Assert.Equal(2, result.OrderedBy[1].Address.ZipCode);
                    Assert.Equal(3, result.OrderedBy[2].Address.ZipCode);
                }
            }
        }

        [Fact]
        public void CanUseOrderByNestedStringInProjection()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new MultiOrder
                    {
                        Info = new List<ShipmentInfo>
                        {
                            new ShipmentInfo
                            {
                                Address = new Address2
                                {
                                    City = "Chicago"
                                }
                            },
                            new ShipmentInfo
                            {
                                Address = new Address2
                                {
                                    City = "Atlanta"
                                }
                            },
                            new ShipmentInfo
                            {
                                Address = new Address2
                                {
                                    City = "Berlin"
                                }
                            }
                        }

                    });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var q = Queryable.Select(s.Query<MultiOrder>(), x => new
                    {
                        OrderedBy = x.Info.OrderBy(i => i.Address.City).ToList()
                    });
                    Assert.Equal("from 'MultiOrders' as x select { " +
                                 "OrderedBy : x.Info.sort(function (a, b){ " +
                                 "return ((a.Address.City < b.Address.City) " +
                                 "? -1 : (a.Address.City > b.Address.City)? 1 : 0);}) }"
                        , q.ToString());

                    var result = q.First();

                    Assert.Equal("Atlanta", result.OrderedBy[0].Address.City);
                    Assert.Equal("Berlin", result.OrderedBy[1].Address.City);
                    Assert.Equal("Chicago", result.OrderedBy[2].Address.City);
                }
            }
        }

        [Fact]
        public void CanUseOrderByDescendingNumberInProjection()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new Order
                    {
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                Quantity = 30
                            },
                            new OrderLine
                            {
                                Quantity = 10
                            },
                            new OrderLine
                            {
                                Quantity = 20
                            }
                        }
                    });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var q = Queryable.Select(s.Query<Order>(), x => new
                    {
                        OrderByDescending = x.Lines.OrderByDescending(l => l.Quantity).ToList()
                    });

                    var result = q.First();

                    Assert.Equal("from 'Orders' as x select " +
                                 "{ OrderByDescending : x.Lines.sort(" +
                                 "function (a, b){ return b.Quantity - a.Quantity;}) }"
                        , q.ToString());

                    Assert.Equal(30, result.OrderByDescending[0].Quantity);
                    Assert.Equal(20, result.OrderByDescending[1].Quantity);
                    Assert.Equal(10, result.OrderByDescending[2].Quantity);
                }
            }
        }

        [Fact]
        public void CanUseOrderByDescendingStringInProjection()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new Order
                    {
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                ProductName = "cheese"
                            },
                            new OrderLine
                            {
                                ProductName = "avocado"
                            },
                            new OrderLine
                            {
                                ProductName = "beer"
                            }
                        }
                    });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var q = Queryable.Select(s.Query<Order>(), x => new
                    {
                        OrderByDescending = x.Lines.OrderByDescending(l => l.ProductName).ToList()
                    });

                    Assert.Equal("from 'Orders' as x select { " +
                                 "OrderByDescending : x.Lines.sort(function (a, b){ " +
                                 "return ((a.ProductName < b.ProductName) " +
                                 "? 1 : (a.ProductName > b.ProductName)? -1 : 0);}) }"
                        , q.ToString());

                    var result = q.First();

                    Assert.Equal("cheese", result.OrderByDescending[0].ProductName);
                    Assert.Equal("beer", result.OrderByDescending[1].ProductName);
                    Assert.Equal("avocado", result.OrderByDescending[2].ProductName);

                }
            }
        }

        [Fact]
        public void TestProjectionCanReturnOrderedCollections()
        {
            using (var store = GetDocumentStore())
            {
                var date = DateTime.Now;

                using (var s = store.OpenSession())
                {
                    s.Store(new RavenDocument
                    {
                        Id = "RavenDocuments/1",
                        SubDocs = new List<SubDoc>
                        {
                            new SubDoc
                            {
                                Value = 1,
                                Date = date
                            },
                            new SubDoc
                            {
                                Value = 2,
                                Date = date
                            },
                            new SubDoc
                            {
                                Value = 3,
                                Date = date
                            }
                        }
                    });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var query = s.Query<RavenDocument>().Where(d => d.Id == "RavenDocuments/1");
                    var transformed = MyProjectionWithOrderedCollectionResult.Transform(query).ToList();

                    Assert.Equal(1, transformed.Count);

                    var result = transformed.Single();
                    Assert.Equal(2, result.SubDocResults.Count);

                    Assert.Equal(3, result.SubDocResults.First().Val);
                    Assert.Equal(2, result.SubDocResults.Skip(1).First().Val);
                    foreach (var subDoc in result.SubDocResults)
                    {
                        var delta = (subDoc.Date - date).TotalSeconds;
                        Assert.True(delta < 1);
                    }

                }

            }

        }

        private static class MyProjectionWithOrderedCollectionResult
        {
            public static IRavenQueryable<QueryResult> Transform(IRavenQueryable<RavenDocument> inputDocs)
            {
                const int filterVal = 1;
                var result = from d in inputDocs
                    select new QueryResult
                    {
                        SubDocResults = d.SubDocs
                            .OrderByDescending(s => s.Value)
                            .Where(s => s.Value > filterVal)
                            .Select(s =>
                                new SubDocResult
                                {
                                    Val = s.Value,
                                    Date = s.Date
                                })
                            .ToList()
                    };

                return result;
            }
        }

        private class MultiOrder
        {
            public string Id { get; set; }
            public List<Order> Orders { get; set; }
            public List<ShipmentInfo> Info { get; set; }
        }

        private class ShipmentInfo
        {
            public DateTime DeliveryDate { get; set; }
            public Address2 Address { get; set; }
        }

        private class Address2 : Address
        {
            public int ZipCode { get; set; }
        }


        private class QueryResult
        {
            public string Id { get; set; }
            public int Int1 { get; set; }
            public int Int2 { get; set; }
            public ICollection<SubDocResult> SubDocResults { get; set; }
        }

        private class SubDocResult
        {
            public int Val { get; set; }
            public DateTime Date { get; set; }
        }

        private class SubDoc
        {
            public int Value { get; set; }
            public DateTime Date { get; set; }
        }

        private class RavenDocument
        {
            public List<SubDoc> SubDocs { get; set; }
            public string Id { get; set; }
        }

    }

}
