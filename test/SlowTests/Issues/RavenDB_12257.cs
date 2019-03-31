using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Subscriptions;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12257 : RavenTestBase
    {
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

        [Fact]
        public async Task CanUseSubscriptionIncludesOnArraysViaStronglyTypedApi()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

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
