using System;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions.Stats;
using Sparrow.Server;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16336 : RavenTestBase
    {
        public RavenDB_16336(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task SubscriptionBatchStats_ShouldIncludeSizeOfIncludedTimeSeriesAndCounters()
        {
            using (var store = GetDocumentStore())
            {
                const string id = "products/1";
                const string counter1 = "Likes";
                const string counter2 = "Dislikes";
                const string timeSeries = "HeartRate";

                int expectedTimeSeriesSize = 0;
                using (var session = store.OpenSession())
                using (store.GetRequestExecutor().ContextPool.AllocateOperationContext(out var ctx))
                {
                    var product = new Product();
                    session.Store(product, id);

                    session.CountersFor(product).Increment(counter1, 3);
                    session.CountersFor(product).Increment(counter2, 5);

                    var baseline = new DateTime(2021, 1, 1);
                    var tsf = session.TimeSeriesFor(product, timeSeries);

                    for (int i = 0; i < TimeSpan.FromHours(2).TotalMinutes; i++)
                    {
                        var dateTime = baseline.AddMinutes(i);
                        tsf.Append(dateTime, i);
                        if (i < 20)
                            // we're only taking the last 100 entries
                            continue;
                        
                        expectedTimeSeriesSize += Sparrow.Extensions.RavenDateTimeExtensions.GetDefaultRavenFormat(dateTime, ctx, out _, isUtc: true); // entry.Timestamp
                        expectedTimeSeriesSize += sizeof(double); // entry.Values
                        expectedTimeSeriesSize += 1; // entry.IsRollup
                    }

                    session.SaveChanges();

                    expectedTimeSeriesSize += id.Length; // doc id
                    expectedTimeSeriesSize += timeSeries.Length; // series name
                    var from = baseline.AddMinutes(20);
                    expectedTimeSeriesSize += Sparrow.Extensions.RavenDateTimeExtensions.GetDefaultRavenFormat(from, ctx, out _, isUtc: true); // range.From
                }

                var expectedCountersSize = counter1.Length
                                           + id.Length
                                           + sizeof(long) // Etag
                                           + sizeof(long) // Value

                                           + counter2.Length
                                           + id.Length
                                           + sizeof(long) // Etag
                                           + sizeof(long) // Value

                                           + id.Length + counter1.Length + counter2.Length; // CountersToGetByDocId property 

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                SubscriptionBatchPerformanceStats stats = null;
                var mre = new AsyncManualResetEvent();
                db.SubscriptionStorage.OnEndBatch += (s, aggregator) =>
                {
                    stats = aggregator.ToBatchPerformanceStats();
                    mre.Set();
                };
                var name = store.Subscriptions.Create(new SubscriptionCreationOptions<Product>
                {
                    Includes = builder => builder
                        .IncludeCounter(counter1)
                        .IncludeCounter(counter2)
                        .IncludeTimeSeries(timeSeries, TimeSeriesRangeType.Last, 100)
                });

                await AssertSubscription(store, name, mre);

                Assert.NotNull(stats);
                Assert.Equal(expectedCountersSize, stats.SizeOfIncludedCountersInBytes);
                Assert.Equal(expectedTimeSeriesSize, stats.SizeOfIncludedTimeSeriesInBytes);
            }
        }

        private static async Task AssertSubscription(DocumentStore store, string name, AsyncManualResetEvent mre)
        {
            var baseline = new DateTime(2021, 1, 1);
            using (var sub = store.Subscriptions.GetSubscriptionWorker<Product>(name))
            {
                var r = sub.Run(batch =>
                {
                    Assert.NotEmpty(batch.Items);
                    using (var s = batch.OpenSession())
                    {
                        foreach (var item in batch.Items)
                        {
                            var product = s.Load<Product>(item.Id);
                            Assert.Same(product, item.Result);

                            var likesValue = s.CountersFor(product).Get("Likes");
                            Assert.Equal(3, likesValue);

                            var dislikesValue = s.CountersFor(product).Get("Dislikes");
                            Assert.Equal(5, dislikesValue);

                            var ts = s.TimeSeriesFor(product, "HeartRate").Get(from: baseline.AddMinutes(20));
                            Assert.Equal(100, ts.Length);
                        }

                        Assert.Equal(0, s.Advanced.NumberOfRequests);
                    }
                });
                Assert.True(await mre.WaitAsync(TimeSpan.FromSeconds(30)));
                await sub.DisposeAsync();
                await r; // no error
            }
        }
    }
}
