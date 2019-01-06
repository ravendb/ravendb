using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FastTests;
using Newtonsoft.Json.Linq;
using Orders;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11671 : RavenTestBase
    {
        [Fact]
        public void Can_properly_parse_query()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var orders = session.Query<Order>().ToList();
                    var results = session.Advanced.RawQuery<Order>("match (Employees as e)<-[Employee]-(Orders as o)-[Company]->(Companies as c) select o").ToArray();
                    Assert.Equal(orders,results);
                }
            }
        }
    }
}
