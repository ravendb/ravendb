using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19415 : RavenTestBase
{
    public RavenDB_19415(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void CanAddAllCountersViaInclude()
    {
        using var store = GetDocumentStore();
        {
            using var session = store.OpenSession();
            session.Store(new Order() {Id = "orders/1-A"});
            var countersFor = session.CountersFor("orders/1-A");
            countersFor.Increment("JustExample", 15);
            session.SaveChanges();
        }
        {
            using var session = store.OpenSession();
            var query = session.Query<Order>().Where(x => x.Id == "orders/1-A")
                .Include(i => i.IncludeAllCounters());
            var countersFor = session.CountersFor("orders/1-A");
            Assert.Equal(1, countersFor.GetAll().Count);            
            Assert.Equal(1, query.Count());
            Assert.Equal("from 'Orders' where id() = $p0 include counters()", query.ToString());
        }
    }
}
