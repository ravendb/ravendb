using System.Collections.Generic;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11480 : RavenTestBase
    {
        public RavenDB_11480(ITestOutputHelper output) : base(output)
        {
        }

        private class Index1 : AbstractIndexCreationTask<Company>
        {
            public class Result
            {
                public string OrderId { get; set; }
            }

            public Index1()
            {
                Map = companies => from c in companies
                                   select new
                                   {
                                       OrderId = "orders/1-A"
                                   };

                Store("OrderId", FieldStorage.Yes);
            }
        }

        private class Product
        {
            public List<Attribute> Attributes { get; set; }
            public string OrderId { get; set; }
        }

        private class Attribute
        {
            public string Name { get; set; }
            public string Value { get; set; }
        }

        private class Index2 : AbstractIndexCreationTask<Product>
        {
            public Index2()
            {
                Map = products => from p in products
                                  select new
                                  {
                                      _ = p.Attributes
                                          .Select(attribute =>
                                              CreateField(attribute.Name, attribute.Value, false, true))
                                  };
            }
        }

        private class Index3 : AbstractIndexCreationTask<Product>
        {
            public Index3()
            {
                Map = products => from p in products
                                  select new
                                  {
                                      _ = p.Attributes
                                          .Select(attribute =>
                                              CreateField(attribute.Name, attribute.Value, false, true)),
                                      OrderId = "orders/1-A"                            
                                  };
            }
        }

        [Fact]
        public void CanLoadOnStoredField()
        {
            using (var store = GetDocumentStore())
            {
                new Index1().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "HR"
                    });

                    session.Store(new Order
                    {
                        Company = "HR-Order"
                    }, "orders/1-A");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = from c in session.Query<Index1.Result, Index1>()
                                let o = RavenQuery.Load<Order>(c.OrderId)
                                select o;

                    var order = query.First();

                    Assert.Equal("HR-Order", order.Company);
                }
            }
        }

        [Fact]
        public void IndexWithDynamicFieldsShouldNotTryToExtractBySourceAliasIfFieldIsMissing()
        {
            using (var store = GetDocumentStore())
            {
                new Index2().Execute(store);

                using (var session = store.OpenSession())
                {
                    // test that query doesn't throw 

                    var query = (from c in session.Query<Index1.Result, Index2>()
                                 let o = RavenQuery.Load<Order>(c.OrderId)
                                 select o).ToList();
                }
            }
        }

        [Fact]
        public void IndexWithDynamicFieldsShouldNotTryToExtractBySourceAliasIfFieldIsNotStored()
        {
            using (var store = GetDocumentStore())
            {
                new Index3().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Product()
                    {
                        Attributes = new List<Attribute>()
                        {
                            new Attribute
                            {
                                Name = "Color",
                                Value = "Red"
                            }
                        },
                        OrderId = "orders/2-A"
                    });

                    session.Store(new Order
                    {
                        Company = "HR-Order"
                    }, "orders/1-A");
                    session.Store(new Order
                    {
                        Company = "HR-Order2"
                    }, "orders/2-A");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = (from c in session.Query<Index1.Result, Index3>()
                                 let o = RavenQuery.Load<Order>(c.OrderId)
                                 select o).ToList();

                    Assert.Equal("HR-Order2", query[0].Company);

                }
            }
        }
    }
}
