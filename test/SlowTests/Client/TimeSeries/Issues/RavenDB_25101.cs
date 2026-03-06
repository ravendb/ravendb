using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Tests.Infrastructure;
using Xunit;

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

        [RavenFact(RavenTestCategory.TimeSeries | RavenTestCategory.Querying)]
        public async Task CanQueryTimeSeriesAggregationOnSegmentBoundary()
        {
            var today = DateTime.Today.AddDays(-1);
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "U1",
                        Age = 30,
                    }, "users/1");

                    await session.StoreAsync(new User
                    {
                        Name = "U2",
                        Age = 30,
                    }, "users/2");

                    var tsf1 = session.TimeSeriesFor("users/1", "HeartRate");
                    var tsf2 = session.TimeSeriesFor("users/2", "HeartRate");

                    for (int i = 0; i < 25; i++)
                    {
                        tsf1.Append(today.AddHours(i), [60 + i]);
                        tsf2.Append(today.AddHours(i), [60 + i]);
                    }

                    tsf2.Append(today.AddHours(25), [60 + 25]);

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var offset = TimeZoneInfo.Local.GetUtcOffset(today);
                    var query = session.Query<User>()
                        .Select(p => RavenQuery.TimeSeries(p, "HeartRate")
                            .GroupBy(g => g.Days(1)).Offset(offset)
                            .Select(ts => new
                            {
                                Sum = ts.Sum(),
                                Count = ts.Count()
                            })
                            .ToList());

                    var agg = await query.ToListAsync();
                    Assert.Equal(agg.Count, 2);
                    var r1 = agg[0];
                    Assert.Equal(r1.Results.Length, 2);
                    Assert.Equal(r1.Results[0].Count[0], 24);
                    Assert.Equal(r1.Results[0].From, today.Add(offset).ToUniversalTime());
                    Assert.Equal(r1.Results[0].To, today.AddDays(1).Add(offset).ToUniversalTime());
                    Assert.Equal(r1.Results[1].Count[0], 1);
                    Assert.Equal(r1.Results[1].From, today.AddDays(1).Add(offset).ToUniversalTime());
                    Assert.Equal(r1.Results[1].To, today.AddDays(2).Add(offset).ToUniversalTime());

                    var r2 = agg[1];
                    Assert.Equal(r2.Results.Length, 2);
                    Assert.Equal(r2.Results[0].Count[0], 24);
                    Assert.Equal(r2.Results[0].From, today.Add(offset).ToUniversalTime());
                    Assert.Equal(r2.Results[0].To, today.AddDays(1).Add(offset).ToUniversalTime());
                    Assert.Equal(r2.Results[1].Count[0], 2);
                    Assert.Equal(r2.Results[1].From, today.AddDays(1).Add(offset).ToUniversalTime());
                    Assert.Equal(r2.Results[1].To, today.AddDays(2).Add(offset).ToUniversalTime());
                }
            }
        }
    }
}
