using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_15000 : RavenTestBase
    {
        public RavenDB_15000(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanIncludeTimeSeriesWithoutProvidingFromAndToDates_ViaLoad()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Company = "companies/1-A"
                    }, "orders/1-A");

                    session.Store(new Company
                    {
                        Name = "HR"
                    }, "companies/1-A");

                    session.TimeSeriesFor("orders/1-A", "Heartrate").Append(DateTime.Now, 1);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Order order = session.Load<Order>("orders/1-A",
                        i => i.IncludeDocuments("Company")
                            .IncludeTimeSeries("Heartrate", null, null));

                    // should not go to server
                    var company = session.Load<Company>(order.Company);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.Equal("HR", company.Name);

                    // should not go to server
                    var vals = session.TimeSeriesFor(order, "Heartrate")
                        .Get()
                        .ToList();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.Equal(1, vals.Count);
                }
            }
        }

        [Fact]
        public void CanIncludeTimeSeriesWithoutProvidingFromAndToDates_ViaQuery()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Company = "companies/1-A"
                    }, "orders/1-A");

                    session.Store(new Company
                    {
                        Name = "HR"
                    }, "companies/1-A");

                    session.TimeSeriesFor("orders/1-A", "Heartrate").Append(DateTime.Now, 1);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Query<Order>()
                        .Include(i => i
                            .IncludeDocuments(o => o.Company)
                            .IncludeTimeSeries("Heartrate"))
                        .First();

                    // should not go to server
                    var company = session.Load<Company>(order.Company);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.Equal("HR", company.Name);

                    // should not go to server
                    var vals = session.TimeSeriesFor(order, "Heartrate")
                        .Get()
                        .ToList();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.Equal(1, vals.Count);
                }
            }
        }

    }
}
