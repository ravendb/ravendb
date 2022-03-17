using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13269 : RavenTestBase
    {
        public RavenDB_13269(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanIndexCountOverloads()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Company = "RavenDB",
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                Discount = 3,
                                PricePerUnit = 1,
                                Product = "1",
                                ProductName = "Orange",
                                Quantity = 10
                            },
                            new OrderLine
                            {
                                Discount = 0,
                                PricePerUnit = 10,
                                Product = "2",
                                ProductName = "Apple",
                                Quantity = 5
                            }
                        }
                    });

                    session.Store(new Order
                    {
                        Company = "HR",
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                Discount = 3,
                                PricePerUnit = 1,
                                Product = "3",
                                ProductName = "Table",
                                Quantity = 1
                            },
                            new OrderLine
                            {
                                Discount = 0,
                                PricePerUnit = 10,
                                Product = "1",
                                ProductName = "Orange",
                                Quantity = 2
                            }
                        }
                    });

                    session.SaveChanges();
                }

                store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition
                {
                    Maps = { "from doc in docs.Orders select new { Count = doc.Lines.Length }" },
                    Name = "test1"
                }}));

                store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition
                {
                    Maps = { "from doc in docs.Orders select new { Count = doc.Lines.Count }" },
                    Name = "test2"
                }}));

                store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition
                {
                    Maps = { "from doc in docs.Orders select new { Count = doc.Lines.Count() }" },
                    Name = "test3"
                }}));

                store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition
                {
                    Maps = { "from doc in docs.Orders select new { Count = doc.Lines.Count(x => x.Quantity == 5) }" },
                    Name = "test4"
                }}));

                Indexes.WaitForIndexing(store);
                var stats = Indexes.WaitForIndexingErrors(store, new[] { "test1", "test2", "test3", "test4" }, errorsShouldExists: false);
                Assert.Null(stats);

                using (var session = store.OpenSession())
                {
                    Assert.Equal(2, OrdersCount("test1"));
                    Assert.Equal(2, OrdersCount("test2"));
                    Assert.Equal(2, OrdersCount("test3"));
                    Assert.Equal(2, OrdersCount("test4"));

                    var terms = await GetTerms("test1");
                    Assert.Equal(1, terms.Length);
                    Assert.Equal("2", terms[0]);

                    terms = await GetTerms("test2");
                    Assert.Equal(1, terms.Length);
                    Assert.Equal("2", terms[0]);

                    terms = await GetTerms("test3");
                    Assert.Equal(1, terms.Length);
                    Assert.Equal("2", terms[0]);

                    terms = await GetTerms("test4");
                    Assert.Equal(2, terms.Length);
                    Assert.Equal("0", terms[0]);
                    Assert.Equal("1", terms[1]);

                    int OrdersCount(string indexName)
                    {
                        return session.Query<Order>(indexName).Count();
                    }

                    async Task<string[]> GetTerms(string indexName)
                    {
                        return await store
                            .Maintenance
                            .SendAsync(new GetTermsOperation(indexName, "Count", null, 128));
                    }
                }
            }
        }
    }
}
