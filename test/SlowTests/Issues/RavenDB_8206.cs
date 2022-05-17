using Tests.Infrastructure;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8206 : RavenTestBase
    {
        public RavenDB_8206(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void AggregatingOnNonExistentFieldWillReturnZeros()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                using (var session = store.OpenSession())
                {
                    var objects = session.Advanced.RawQuery<dynamic>("from Products group by Category select sum(Price), Category").ToList();

                    Assert.Equal(8, objects.Count);

                    foreach (var o in objects)
                    {
                        Assert.Equal(0, (long)o.Price);
                    }
                }
            }
        }

        [Fact]
        public void AggregationOnNullValuesWorksWithDoubles()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Product
                    {
                        Category = "categories/1",
                        Price = null
                    });

                    session.Store(new Product
                    {
                        Category = "categories/1",
                        Price = 10.2
                    });

                    session.Store(new Product
                    {
                        Category = "categories/2",
                        Price = 10.2
                    });

                    session.Store(new Product
                    {
                        Category = "categories/2",
                        Price = null
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<Product>("from Products group by Category select sum(Price), Category").ToList();

                    Assert.Equal(10.2, results[0].Price);
                    Assert.Equal(10.2, results[1].Price);
                }
            }
        }

        private class Product
        {
            public string Id { get; set; }
            public string Category { get; set; }
            public double? Price { get; set; }
        }
    }
}
