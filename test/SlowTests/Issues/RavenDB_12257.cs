using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Subscriptions;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12257 : RavenTestBase
    {
        public RavenDB_12257(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanUseSubscriptionIncludesViaStronglyTypedApi()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var product = new Product();
                    var category = new Category();
                    var supplier = new Supplier();

                    session.Store(category);
                    session.Store(product);

                    product.Category = category.Id;
                    product.Supplier = supplier.Id;

                    session.Store(product);

                    session.SaveChanges();
                }

                var name = store.Subscriptions.Create(new SubscriptionCreationOptions<Product>
                {
                    Includes = builder => builder
                        .IncludeDocuments(x => x.Category)
                        .IncludeDocuments(x => x.Supplier)
                });

                using (var sub = store.Subscriptions.GetSubscriptionWorker<Product>(name))
                {
                    var mre = new AsyncManualResetEvent();
                    var r = sub.Run(batch =>
                    {
                        Assert.NotEmpty(batch.Items);
                        using (var s = batch.OpenSession())
                        {
                            foreach (var item in batch.Items)
                            {
                                s.Load<Category>(item.Result.Category);
                                s.Load<Supplier>(item.Result.Supplier);
                                var product = s.Load<Product>(item.Id);
                                Assert.Same(product, item.Result);
                            }
                            Assert.Equal(0, s.Advanced.NumberOfRequests);
                        }
                        mre.Set();
                    });
                    Assert.True(await mre.WaitAsync(TimeSpan.FromSeconds(30)));
                    await sub.DisposeAsync();
                    await r;// no error
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Sharding)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanUseSubscriptionIncludesOnArraysViaStronglyTypedApi(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var product1 = new Product
                    {
                        Name = "P1"
                    };

                    var product2 = new Product
                    {
                        Name = "P2"
                    };

                    session.Store(product1);
                    session.Store(product2);

                    var order1 = new Order
                    {
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                Product = product1.Id
                            },
                            new OrderLine
                            {
                                Product = product2.Id
                            },
                        }
                    };

                    var order2 = new Order
                    {
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                Product = product2.Id
                            }
                        }
                    };

                    session.Store(order1);
                    session.Store(order2);

                    session.SaveChanges();
                }

                var name = store.Subscriptions.Create(new SubscriptionCreationOptions<Order>
                {
                    Includes = builder => builder
                        .IncludeDocuments(x => x.Lines.Select(y => y.Product))
                });

                using (var sub = store.Subscriptions.GetSubscriptionWorker<Order>(name))
                {
                    var mre = new AsyncManualResetEvent();
                    var r = sub.Run(batch =>
                    {
                        Assert.NotEmpty(batch.Items);
                        using (var s = batch.OpenSession())
                        {
                            foreach (var item in batch.Items)
                            {
                                var res = s.Load<Product>(item.Result.Lines.Select(x => x.Product));
                                Assert.Equal(res.Count, item.Result.Lines.Count);
                            }
                            Assert.Equal(0, s.Advanced.NumberOfRequests);
                        }
                        mre.Set();
                    });
                    Assert.True(await mre.WaitAsync(TimeSpan.FromSeconds(30)));
                    await sub.DisposeAsync();
                    await r;// no error
                }
            }
        }
    }
}
