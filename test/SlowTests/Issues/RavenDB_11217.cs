using System;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11217 : RavenTestBase
    {
        public RavenDB_11217(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void SessionWideNoTrackingShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession(new SessionOptions()))
                {
                    var supplier = new Supplier
                    {
                        Name = "Supplier1"
                    };

                    session.Store(supplier);

                    var product = new Product
                    {
                        Name = "Product1",
                        Supplier = supplier.Id
                    };

                    session.Store(product);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions
                {
                    NoTracking = true
                }))
                {
                    var supplier = new Supplier
                    {
                        Name = "Supplier2"
                    };

                    Assert.Throws<InvalidOperationException>(() => session.Store(supplier));
                }

                using (var session = store.OpenSession(new SessionOptions
                {
                    NoTracking = true
                }))
                {
                    Assert.Equal(0, session.Advanced.NumberOfRequests);

                    var product1 = session
                        .Load<Product>("products/1-A", builder => builder.IncludeDocuments(x => x.Supplier));

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    Assert.NotNull(product1);
                    Assert.Equal("Product1", product1.Name);
                    Assert.False(session.Advanced.IsLoaded(product1.Id));
                    Assert.False(session.Advanced.IsLoaded(product1.Supplier));

                    var supplier = session.Load<Supplier>(product1.Supplier);

                    Assert.NotNull(supplier);
                    Assert.Equal("Supplier1", supplier.Name);
                    Assert.Equal(2, session.Advanced.NumberOfRequests);
                    Assert.False(session.Advanced.IsLoaded(supplier.Id));

                    var product2 = session
                        .Load<Product>("products/1-A", builder => builder.IncludeDocuments(x => x.Supplier));

                    Assert.NotEqual(product1, product2);
                }

                using (var session = store.OpenSession(new SessionOptions
                {
                    NoTracking = true
                }))
                {
                    Assert.Equal(0, session.Advanced.NumberOfRequests);

                    var product1 = session
                        .Advanced
                        .LoadStartingWith<Product>("products/")
                        .FirstOrDefault();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    Assert.NotNull(product1);
                    Assert.Equal("Product1", product1.Name);
                    Assert.False(session.Advanced.IsLoaded(product1.Id));
                    Assert.False(session.Advanced.IsLoaded(product1.Supplier));

                    var supplier = session
                        .Advanced
                        .LoadStartingWith<Supplier>(product1.Supplier)
                        .FirstOrDefault();

                    Assert.NotNull(supplier);
                    Assert.Equal("Supplier1", supplier.Name);
                    Assert.Equal(2, session.Advanced.NumberOfRequests);
                    Assert.False(session.Advanced.IsLoaded(supplier.Id));

                    var product2 = session
                        .Advanced
                        .LoadStartingWith<Product>("products/")
                        .FirstOrDefault();

                    Assert.NotEqual(product1, product2);
                }

                using (var session = store.OpenSession(new SessionOptions
                {
                    NoTracking = true
                }))
                {
                    Assert.Equal(0, session.Advanced.NumberOfRequests);

                    var products = session
                        .Query<Product>()
                        .Include(x => x.Supplier)
                        .ToList();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.Equal(1, products.Count);

                    var product1 = products[0];

                    Assert.NotNull(product1);
                    Assert.Equal("Product1", product1.Name);
                    Assert.False(session.Advanced.IsLoaded(product1.Id));
                    Assert.False(session.Advanced.IsLoaded(product1.Supplier));

                    var supplier = session.Load<Supplier>(product1.Supplier);

                    Assert.NotNull(supplier);
                    Assert.Equal("Supplier1", supplier.Name);
                    Assert.Equal(2, session.Advanced.NumberOfRequests);
                    Assert.False(session.Advanced.IsLoaded(supplier.Id));

                    products = session
                        .Query<Product>()
                        .Include(x => x.Supplier)
                        .ToList();

                    Assert.Equal(1, products.Count);

                    var product2 = products[0];

                    Assert.NotEqual(product1, product2);
                }

                using (var session = store.OpenSession())
                {
                    session.CountersFor("products/1-A").Increment("c1");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions
                {
                    NoTracking = true
                }))
                {
                    var product1 = session.Load<Product>("products/1-A");
                    var counters = session.CountersFor(product1.Id);

                    counters.Get("c1");

                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    counters.Get("c1");

                    Assert.Equal(3, session.Advanced.NumberOfRequests);

                    var val1 = counters.GetAll();

                    Assert.Equal(4, session.Advanced.NumberOfRequests);

                    var val2 = counters.GetAll();

                    Assert.Equal(5, session.Advanced.NumberOfRequests);
                    Assert.False(ReferenceEquals(val1, val2));
                }
            }
        }

        [Fact]
        public void SessionWideNoCachingShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Query<Product>()
                        .Statistics(out var stats)
                        .Where(x => x.Name == "HR")
                        .ToList();

                    Assert.True(stats.DurationInMs >= 0, $"Was {stats.DurationInMs}");

                    session.Query<Product>()
                        .Statistics(out stats)
                        .Where(x => x.Name == "HR")
                        .ToList();

                    Assert.Equal(-1, stats.DurationInMs); // from cache
                }

                using (var session = store.OpenSession(new SessionOptions
                {
                    NoCaching = true
                }))
                {
                    session.Query<Order>()
                        .Statistics(out var stats)
                        .Where(x => x.Company == "HR")
                        .ToList();

                    Assert.True(stats.DurationInMs >= 0, $"Was {stats.DurationInMs}");

                    session.Query<Order>()
                        .Statistics(out stats)
                        .Where(x => x.Company == "HR")
                        .ToList();

                    Assert.True(stats.DurationInMs >= 0, $"Was {stats.DurationInMs}");
                }
            }
        }
    }
}
