using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21339 : RavenTestBase
{
    public RavenDB_21339(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public async Task Using_Includes_In_Non_Tracking_Session_Should_Throw()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var supplier = new Supplier { Id = "suppliers/1" };

                session.Store(supplier);
                session.Store(new Product
                {
                    Id = "products/1",
                    Supplier = supplier.Id
                });

                session.SaveChanges();
            }

            using (var session = store.OpenSession(new SessionOptions
            {
                NoTracking = true
            }))
            {
                var e = Assert.Throws<InvalidOperationException>(() => session.Load<Product>("products/1", includes => includes.IncludeDocuments(x => x.Supplier)));
                Assert.Contains("registering includes is forbidden", e.Message);
            }

            using (var session = store.OpenAsyncSession(new SessionOptions
            {
                NoTracking = true
            }))
            {
                var e = await Assert.ThrowsAsync<InvalidOperationException>(() => session.LoadAsync<Product>("products/1", includes => includes.IncludeDocuments(x => x.Supplier)));
                Assert.Contains("registering includes is forbidden", e.Message);
            }

            using (var session = store.OpenSession(new SessionOptions
            {
                NoTracking = true
            }))
            {
                var e = Assert.Throws<InvalidOperationException>(() => session.Query<Product>().Include(x => x.Supplier).ToList());
                Assert.Contains("registering includes is forbidden", e.Message);
            }

            using (var session = store.OpenAsyncSession(new SessionOptions
            {
                NoTracking = true
            }))
            {
                var e = await Assert.ThrowsAsync<InvalidOperationException>(() => session.Query<Product>().Include(x => x.Supplier).ToListAsync());
                Assert.Contains("registering includes is forbidden", e.Message);
            }

            using (var session = store.OpenSession(new SessionOptions
            {
                NoTracking = true
            }))
            {
                var e = Assert.Throws<InvalidOperationException>(() => session.Advanced.DocumentQuery<Product>().Include(x => x.Supplier).ToList());
                Assert.Contains("registering includes is forbidden", e.Message);
            }

            using (var session = store.OpenAsyncSession(new SessionOptions
            {
                NoTracking = true
            }))
            {
                var e = await Assert.ThrowsAsync<InvalidOperationException>(() => session.Advanced.AsyncDocumentQuery<Product>().Include(x => x.Supplier).ToListAsync());
                Assert.Contains("registering includes is forbidden", e.Message);
            }
        }
    }
}
