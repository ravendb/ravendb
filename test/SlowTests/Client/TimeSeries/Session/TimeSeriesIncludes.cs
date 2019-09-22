using System;
using System.Linq;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Client.TimeSeries.Session
{
    public class TimeSeriesIncludes : RavenTestBase
    {
        [Fact]
        public void SessionIncludeTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR" }, "companies/1-A");
                    session.Store(new Order { Company = "companies/1-A" }, "orders/1-A");
                    session.TimeSeriesFor("orders/1-A").Append("Heartrate", baseline, "watches/apple", new []{ 67d });
                    session.TimeSeriesFor("orders/1-A").Append("Heartrate", baseline.AddMinutes(5), "watches/apple", new[] { 64d });
                    session.TimeSeriesFor("orders/1-A").Append("Heartrate", baseline.AddMinutes(10), "watches/fitbit", new[] { 65d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>(
                        "orders/1-A",
                        i => i.IncludeDocuments("Company")
                            .IncludeTimeSeries("Heartrate", DateTime.MinValue, DateTime.MaxValue));

                    var company = session.Load<Company>(order.Company);
                    Assert.Equal("HR", company.Name);

                    // should not go to server
                    var values = session.TimeSeriesFor(order)
                        .Get("Heartrate", DateTime.MinValue, DateTime.MaxValue)
                        .ToList(); 
                    
                    Assert.Equal(3, values.Count);

                    Assert.Equal(1, values[0].Values.Length);
                    Assert.Equal(67d, values[0].Values[0]);
                    Assert.Equal("watches/apple", values[0].Tag);
                    Assert.Equal(baseline, values[0].Timestamp);

                    Assert.Equal(1, values[1].Values.Length);
                    Assert.Equal(64d, values[1].Values[0]);
                    Assert.Equal("watches/apple", values[1].Tag);
                    Assert.Equal(baseline.AddMinutes(5), values[1].Timestamp);

                    Assert.Equal(1, values[2].Values.Length);
                    Assert.Equal(65d, values[2].Values[0]);
                    Assert.Equal("watches/fitbit", values[2].Tag);
                    Assert.Equal(baseline.AddMinutes(10), values[2].Timestamp);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
            }
        }

    }
}
