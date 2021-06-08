using System.Linq;
using FastTests;
using Xunit;
using FastTests.Server.Basic.Entities;
using Tests.Infrastructure.Entities;
using Xunit.Abstractions;

namespace SlowTests.Graph
{
    public class SortGraphQueries: RavenTestBase
    {
        public SortGraphQueries(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void SortOnDecimalFieldShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<Order>(@"
                        match (Orders as o) order by o.Freight as double desc 
                    ").First();
                    Assert.Equal((decimal)1007.64M, result.Freight);
                }
            }
        }

        [Fact]
        public void SortOnEdgesShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<Product>(@"
                        match (Orders as o)-[Lines as l select Product]->(Products as p) 
                        order by l.Discount as double  
                        select p
                    ").First();
                    Assert.Equal("Queso Cabrales", result.Name);
                }
            }
        }

        [Fact]
        public void MultipleSortShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<Order>(@"
                        match (Orders as o)-[Lines as l select Product]->(Products as p) 
                        order by o.OrderedAt desc , l.Discount as double  
                        select o
                        ").Skip(100).First();
                    Assert.Equal("orders/799-A", result.Id);
                }
            }
        }

        [Fact]
        public void SortOnArrayShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Foo { Numbers = new[] { 3, 7, 5 } }, "foo/1");
                    session.Store(new Foo { Numbers = new[] { 3, 7, 6 } }, "foo/2");
                    session.SaveChanges();
                    var result = session.Advanced.RawQuery<Foo>(@"
                        match (Foos as f)
                        order by f.Numbers desc
                        ").ToList().First();
                    Assert.Equal("foo/2", result.Id);
                }
            }

        }

        private class Foo
        {
            public string Id { get; set; }
            public int[] Numbers { get; set; }
        }

        [Fact]
        public void SortOnStringShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<Product>(@"
                        match (Products as p)
                        order by p.Name
                        ").First();
                    Assert.Equal("products/17-A", result.Id);
                }
            }
        }
    }
}
