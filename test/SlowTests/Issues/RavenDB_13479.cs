using System;
using System.Threading;
using FastTests;
using Orders;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Subscriptions;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13479 : RavenTestBase
    {
        public RavenDB_13479(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Basic_Subscription_TimeSeries_Includes()
        {
            var now = DateTime.UtcNow.EnsureMilliseconds();

            using (var store = GetDocumentStore())
            {
                var name = store.Subscriptions
                    .Create(new SubscriptionCreationOptions<Company>()
                    {
                        Includes = builder => builder
                            .IncludeTimeSeries("StockPrice", TimeSeriesRangeType.Last, TimeValue.FromMonths(1))
                    });

                var mre = new ManualResetEventSlim();
                var worker = store.Subscriptions.GetSubscriptionWorker<Company>(name);
                worker.Run(batch =>
                {
                    using (var session = batch.OpenSession())
                    {
                        Assert.Equal(0, session.Advanced.NumberOfRequests);

                        var company = session.Load<Company>("companies/1");
                        Assert.Equal(0, session.Advanced.NumberOfRequests);

                        var timeSeries = session.TimeSeriesFor(company, "StockPrice");
                        var timeSeriesEntries = timeSeries.Get(from: now.AddDays(-7));

                        Assert.Equal(1, timeSeriesEntries.Length);
                        Assert.Equal(now, timeSeriesEntries[0].Timestamp);
                        Assert.Equal(10, timeSeriesEntries[0].Value);

                        Assert.Equal(0, session.Advanced.NumberOfRequests);
                    }

                    mre.Set();
                });

                using (var session = store.OpenSession())
                {
                    var company = new Company { Id = "companies/1", Name = "HR" };
                    session.Store(company);

                    session.TimeSeriesFor(company, "StockPrice").Append(now, 10);

                    session.SaveChanges();
                }

                Assert.True(mre.Wait(TimeSpan.FromSeconds(30)));
            }
        }
    }
}
