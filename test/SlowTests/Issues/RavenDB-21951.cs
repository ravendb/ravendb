using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21951 : RavenTestBase
{
    public RavenDB_21951(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.JavaScript)]
    public void JavaScriptIndexWithBoostOnIndexEntryShouldWork()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var o1 = new Order() { Name = "CoolName", Freight = 21.37 };
                var o2 = new Order() { Name = "SomeOtherName", Freight = 1 };
                var o3 = new Order() { Name = "CoolName", Freight = 10 };
                
                session.Store(o1);
                session.Store(o2);
                session.Store(o3);
                
                session.SaveChanges();
                
                var index = new DummyIndex();
                
                index.Execute(store);
                
                Indexes.WaitForIndexing(store);

                var result = session.Query<Order, DummyIndex>().Where(x => x.Name == "CoolName").OrderByScore().ToList();
                
                Assert.Equal(2, result.Count);
                
                Assert.Equal(21.37, result[0].Freight);
                Assert.Equal(10, result[1].Freight);
            }
        }
    }

    private class DummyIndex : AbstractJavaScriptIndexCreationTask
    {
        public DummyIndex()
        {
            Maps = new HashSet<string>()
            {
               @"map('orders', function(order) {
                    return boost({
                        Name: order.Name
                    }, order.Freight)
                })"
            };
        }
    }

    private class Order
    {
        public string Name { get; set; }
        public double Freight { get; set; }
    }
}
