using System;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11879 : RavenTestBase
    {
        public RavenDB_11879(ITestOutputHelper output) : base(output)
        {
        }

        private class MultiMap : AbstractMultiMapIndexCreationTask
        {
            public class Result
            {
                public DateTime Date { get; set; }

                public string Name { get; set; }

                public string Id { get; set; }
            }

            public MultiMap()
            {
                AddMap<Class1>(orders => from o in orders
                                         select new
                                         {
                                             Id = o.Id,
                                             Date = o.Date,
                                             Name = (string)null
                                         });

                AddMap<Class2>(companies => from c in companies
                                            select new
                                            {
                                                Id = c.Id,
                                                Date = c.Date,
                                                Name = c.Name
                                            });
            }
        }

        private class Class1 : ClassBase
        {
        }

        private class Class2 : ClassBase
        {
        }

        private abstract class ClassBase
        {
            public DateTime Date { get; set; }

            public string Name { get; set; }

            public string Id { get; set; }
        }

        [Fact]
        public void SortOnlyQueriesShouldWorkForMultiMapIndexes()
        {
            using (var store = GetDocumentStore())
            {
                new MultiMap().Execute(store);

                for (var i = 0; i < 50; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new Class1
                        {
                            Date = DateTime.Now,
                            Name = Guid.NewGuid().ToString("N")
                        }, $"orders/{new Random().Next(1, 3)}");

                        session.Store(new Class2
                        {
                            Date = DateTime.Now,
                            Name = Guid.NewGuid().ToString("N")
                        }, $"companies/{new Random().Next(1, 3)}");

                        session.SaveChanges();
                    }

                    Indexes.WaitForIndexing(store);

                    using (var session = store.OpenSession())
                    {
                        var orders = session.Query<MultiMap.Result, MultiMap>()
                            .Customize(x => x.NoCaching())
                            .OrderBy(x => x.Date)
                            .ToList();

                        var orderIds = orders.Select(x => x.Id).ToList();
                        var uniqueOrderIds = orderIds.ToHashSet();

                        Assert.True(orderIds.Count > 0);
                        Assert.Equal(orderIds.Count, uniqueOrderIds.Count);

                        var actualOrder = orders.Select(x => x.Date).ToList();
                        var expectedOrder = actualOrder.OrderBy(x => x);

                        Assert.Equal(expectedOrder, actualOrder);
                    }

                    using (var session = store.OpenSession())
                    {
                        var orders = session.Query<MultiMap.Result, MultiMap>()
                            .Customize(x => x.NoCaching())
                            .OrderByDescending(x => x.Date)
                            .ToList();

                        var orderIds = orders.Select(x => x.Id).ToList();
                        var uniqueOrderIds = orderIds.ToHashSet();

                        Assert.True(orderIds.Count > 0);
                        Assert.Equal(orderIds.Count, uniqueOrderIds.Count);

                        var actualOrder = orders.Select(x => x.Date).ToList();
                        var expectedOrder = actualOrder.OrderByDescending(x => x);

                        Assert.Equal(expectedOrder, actualOrder);
                    }
                }
            }
        }

        [Fact]
        public void SortOnlyQueriesShouldWorkForAutoIndexes()
        {
            using (var store = GetDocumentStore())
            {
                for (var i = 0; i < 50; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new Order
                        {
                            OrderedAt = DateTime.Now,
                            Company = Guid.NewGuid().ToString("N")
                        }, $"orders/{new Random().Next(1, 3)}");

                        session.SaveChanges();
                    }

                    Indexes.WaitForIndexing(store);

                    using (var session = store.OpenSession())
                    {
                        var orders = session.Query<Order>()
                            .Customize(x => x.NoCaching())
                            .OrderBy(x => x.OrderedAt)
                            .ToList();

                        var orderIds = orders.Select(x => x.Id).ToList();
                        var uniqueOrderIds = orderIds.ToHashSet();

                        Assert.True(orderIds.Count > 0);
                        Assert.Equal(orderIds.Count, uniqueOrderIds.Count);

                        var actualOrder = orders.Select(x => x.OrderedAt).ToList();
                        var expectedOrder = actualOrder.OrderBy(x => x);

                        Assert.Equal(expectedOrder, actualOrder);
                    }

                    using (var session = store.OpenSession())
                    {
                        var orders = session.Query<Order>()
                            .Customize(x => x.NoCaching())
                            .OrderByDescending(x => x.OrderedAt)
                            .ToList();

                        var orderIds = orders.Select(x => x.Id).ToList();
                        var uniqueOrderIds = orderIds.ToHashSet();

                        Assert.True(orderIds.Count > 0);
                        Assert.Equal(orderIds.Count, uniqueOrderIds.Count);

                        var actualOrder = orders.Select(x => x.OrderedAt).ToList();
                        var expectedOrder = actualOrder.OrderByDescending(x => x);

                        Assert.Equal(expectedOrder, actualOrder);
                    }
                }
            }
        }
    }
}
