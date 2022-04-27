using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Documents.Operations;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_14533 : RavenTestBase
    {
        public RavenDB_14533(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.TimeSeries)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task TimeSeriesSegmentsSummary(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var baseline = new DateTime(2000, 1, 1);

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    
                    for (int i = 0; i < 15000; i++)
                    {
                        session.TimeSeriesFor("users/1", "Heartrate")
                            .Append(baseline.AddMinutes(i), new[] { 58d + i }, "fitbit");
                    }

                    session.SaveChanges();
                }

                var sum = 0;
                var summary = await store.Operations.SendAsync(new GetSegmentsSummaryOperation("users/1", "Heartrate", baseline, baseline.AddMinutes(16000)));
                sum += summary.Results.Sum(seg => seg.NumberOfEntries);
                Assert.Equal(15000, sum);

                sum = 0;
                summary = await store.Operations.SendAsync(new GetSegmentsSummaryOperation("users/1", "Heartrate", baseline, baseline.AddMinutes(100)));
                sum += summary.Results.Sum(seg => seg.NumberOfEntries);
                Assert.NotEqual(15000, sum);

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/1", "Heartrate").Delete(baseline, baseline.AddMinutes(99));

                    session.SaveChanges();
                }

                sum = 0;
                summary = await store.Operations.SendAsync(new GetSegmentsSummaryOperation("users/1", "Heartrate", baseline, baseline.AddMinutes(16000)));
                sum += summary.Results.Sum(seg => seg.NumberOfLiveEntries);
                Assert.Equal(14900, sum);
            }
        }
    }
}
