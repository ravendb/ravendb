using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FastTests;
using Xunit;
using FastTests.Server.Basic.Entities;

namespace SlowTests.Graph
{
    public class SortGraphQueries: RavenTestBase
    {
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
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<Order>(@"
                        match (Orders as o)
                        order by o.Lines
                        ").First();
                    Assert.Equal("orders/1-A", result.Id);
                }
            }
        }

        [Fact]
        public void SortOnObjectShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<Order>(@"
                        match (Orders as o)
                        order by o.ShipTo
                        ").First();
                    Assert.Equal("orders/1-A", result.Id);
                }
            }
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
