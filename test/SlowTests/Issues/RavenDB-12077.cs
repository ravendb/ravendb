using System.Linq;
using FastTests;
using FastTests.Server.Basic.Entities;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12077 : RavenTestBase
    {
        public RavenDB_12077(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanProjectSingleCollectionProperty()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());
                using (var session = store.OpenSession())
                {
                    var query = from o in session.Query<Order>()
                                where o.Id == "orders/825-A"
                                select o.Lines;

                    var queryResult = query.ToArray();
                    Assert.Equal(1, queryResult.Length);
                    Assert.Equal(4, queryResult[0].Count);
                }
            }
        }
    }
}
