using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_25101 : RavenTestBase
    {
        public RavenDB_25101(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.TimeSeries)]
        public async Task CanExecuteRollupOnSegmentBoundary()
        {
            var rand = new Random(1337);
            using (var store = GetDocumentStore())
            {
                var p1 = new TimeSeriesPolicy("ByMinute", TimeValue.FromMinutes(1));
                var p2 = new TimeSeriesPolicy("By5Minute", TimeValue.FromMinutes(5));

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p1, p2
                            }
                        },
                    }
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                var baseline = RavenTestHelper.UtcToday.AddDays(-100);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "users/karmel");

                    for (int i = 0; i < 16; i++)
                    {
                        session.TimeSeriesFor("users/karmel", "Heartrate")
                            .Append(baseline.AddMinutes(i), [rand.Next(), rand.Next(), rand.Next(), rand.Next(), rand.Next()], "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceForAsync(store.Database);
                await database.TimeSeriesPolicyRunner.RunRollups();

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/karmel", "Heartrate").Get().ToList();
                    var tsSeconds = (int)(ts.Last().Timestamp - ts.First().Timestamp).TotalSeconds;

                    var ts1 = session.TimeSeriesFor("users/karmel", p1.GetTimeSeriesName("Heartrate")).Get().ToList();
                    var ts1Seconds = (int)(ts1.Last().Timestamp - ts1.First().Timestamp).TotalSeconds;

                    var ts2 = session.TimeSeriesFor("users/karmel", p2.GetTimeSeriesName("Heartrate")).Get().ToList();
                    var ts2Seconds = (int)(ts2.Last().Timestamp - ts2.First().Timestamp).TotalSeconds;

                    Assert.Equal(ts1Seconds, tsSeconds);
                    Assert.Equal(ts2Seconds, tsSeconds);
                }
            }
        }
    }
}
