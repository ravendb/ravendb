using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session;
using SlowTests.Client.TimeSeries.Query;
using SlowTests.Core.Utils.Entities;
using Sparrow;
using Xunit;
using Xunit.Abstractions;
using static SlowTests.Client.TimeSeries.Session.TimeSeriesTypedSessionTests;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_16060 : RavenTestBase
    {
        public RavenDB_16060(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanIncludeTypedTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");

                    var ts = session.TimeSeriesFor<HeartRateMeasure>("users/ayende");
                    ts.Append(baseline, new HeartRateMeasure { HeartRate = 59d }, "watches/fitbit");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var items = session.Query<User>()
                        .Include(x => x.IncludeTimeSeries("HeartRateMeasures"))
                        .ToList();

                    foreach (var item in items)
                    {
                        var timeseries = session.TimeSeriesFor<HeartRateMeasure>(item.Id, "HeartRateMeasures")
                            .Get();

                        Assert.Equal(1, timeseries.Length);
                        Assert.Equal(59, timeseries[0].Value.HeartRate);
                    }

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task CanIncludeTypedTimeSeriesAsync()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Oren" }, "users/ayende");

                    var ts = session.TimeSeriesFor<HeartRateMeasure>("users/ayende");
                    ts.Append(baseline, new HeartRateMeasure { HeartRate = 59d }, "watches/fitbit");

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var items = await session.Query<User>()
                        .Include(x => x.IncludeTimeSeries("HeartRateMeasures"))
                        .ToListAsync();

                    foreach (var item in items)
                    {
                        var timeseries = await session.TimeSeriesFor<HeartRateMeasure>(item.Id, "HeartRateMeasures")
                            .GetAsync();

                        Assert.Equal(1, timeseries.Length);
                        Assert.Equal(59, timeseries[0].Value.HeartRate);
                    }

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void CanServeTimeSeriesFromCache_Typed()
        {
            //RavenDB-16136

            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                const string id = "users/gabor";

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Gabor" }, id);

                    var ts = session.TimeSeriesFor<HeartRateMeasure>(id);
                    ts.Append(baseline, new HeartRateMeasure { HeartRate = 59d }, "watches/fitbit");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var timeseries = session.TimeSeriesFor<HeartRateMeasure>(id)
                        .Get();

                    Assert.Equal(1, timeseries.Length);
                    Assert.Equal(59, timeseries[0].Value.HeartRate);

                    var timeseries2 = session.TimeSeriesFor<HeartRateMeasure>(id)
                        .Get(); // should not go to server

                    Assert.Equal(1, timeseries2.Length);
                    Assert.Equal(59, timeseries2[0].Value.HeartRate);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void IncludeTimeSeriesAndMergeWithExistingRangesInCache_Typed()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                var documentId = "users/ayende";

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, documentId);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor<HeartRateMeasure>(documentId);
                    for (int i = 0; i < 360; i++)
                    {
                        var typedMeasure = new HeartRateMeasure
                        {
                            HeartRate = 6
                        };
                        tsf.Append(baseline.AddSeconds(i * 10), typedMeasure, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor<HeartRateMeasure>(documentId)
                        .Get(baseline.AddMinutes(2), baseline.AddMinutes(10))
                        .ToList();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    Assert.Equal(49, vals.Count);
                    Assert.Equal(baseline.AddMinutes(2), vals[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(10), vals[48].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    var user = session.Load<User>(
                        documentId,
                        i => i.IncludeTimeSeries("HeartRateMeasures", baseline.AddMinutes(40), baseline.AddMinutes(50)));

                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    // should not go to server

                    vals = session.TimeSeriesFor<HeartRateMeasure>(documentId)
                        .Get(baseline.AddMinutes(40), baseline.AddMinutes(50))
                        .ToList();

                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    Assert.Equal(61, vals.Count);
                    Assert.Equal(baseline.AddMinutes(40), vals[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(50), vals[60].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    var sessionOperations = (InMemoryDocumentSessionOperations)session;

                    Assert.True(sessionOperations.TimeSeriesByDocId.TryGetValue(documentId, out var cache));
                    Assert.True(cache.TryGetValue("HeartRateMeasures", out var ranges));
                    Assert.Equal(2, ranges.Count);

                    Assert.Equal(baseline.AddMinutes(2), ranges[0].From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(10), ranges[0].To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(40), ranges[1].From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(50), ranges[1].To, RavenTestHelper.DateTimeComparer.Instance);

                    // we intentionally evict just the document (without it's TS data),
                    // so that Load request will go to server

                    sessionOperations.DocumentsByEntity.Evict(user);
                    sessionOperations.DocumentsById.Remove(documentId);

                    // should go to server to get [0, 2] and merge it into existing [2, 10] 
                    user = session.Load<User>(
                        documentId,
                        i => i.IncludeTimeSeries("HeartRateMeasures", baseline, baseline.AddMinutes(2)));

                    Assert.Equal(3, session.Advanced.NumberOfRequests);

                    // should not go to server

                    vals = session.TimeSeriesFor<HeartRateMeasure>(documentId)
                        .Get(baseline, baseline.AddMinutes(2))
                        .ToList();

                    Assert.Equal(3, session.Advanced.NumberOfRequests);

                    Assert.Equal(13, vals.Count);
                    Assert.Equal(baseline, vals[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(2), vals[12].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(2, ranges.Count);
                    Assert.Equal(baseline.AddMinutes(0), ranges[0].From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(10), ranges[0].To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(40), ranges[1].From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(50), ranges[1].To, RavenTestHelper.DateTimeComparer.Instance);

                    // evict just the document
                    sessionOperations.DocumentsByEntity.Evict(user);
                    sessionOperations.DocumentsById.Remove(documentId);

                    // should go to server to get [10, 16] and merge it into existing [0, 10] 
                    user = session.Load<User>(
                        documentId,
                        i => i.IncludeTimeSeries("HeartRateMeasures", baseline.AddMinutes(10), baseline.AddMinutes(16)));

                    Assert.Equal(4, session.Advanced.NumberOfRequests);

                    // should not go to server

                    vals = session.TimeSeriesFor<HeartRateMeasure>(documentId)
                        .Get(baseline.AddMinutes(10), baseline.AddMinutes(16))
                        .ToList();

                    Assert.Equal(4, session.Advanced.NumberOfRequests);

                    Assert.Equal(37, vals.Count);
                    Assert.Equal(baseline.AddMinutes(10), vals[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(16), vals[36].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(2, ranges.Count);
                    Assert.Equal(baseline.AddMinutes(0), ranges[0].From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(16), ranges[0].To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(40), ranges[1].From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(50), ranges[1].To, RavenTestHelper.DateTimeComparer.Instance);

                    // evict just the document
                    sessionOperations.DocumentsByEntity.Evict(user);
                    sessionOperations.DocumentsById.Remove(documentId);

                    // should go to server to get range [17, 19]
                    // and add it to cache in between [10, 16] and [40, 50]

                    user = session.Load<User>(
                        documentId,
                        i => i.IncludeTimeSeries("HeartRateMeasures", baseline.AddMinutes(17), baseline.AddMinutes(19)));

                    Assert.Equal(5, session.Advanced.NumberOfRequests);

                    // should not go to server

                    vals = session.TimeSeriesFor<HeartRateMeasure>(documentId)
                        .Get(baseline.AddMinutes(17), baseline.AddMinutes(19))
                        .ToList();

                    Assert.Equal(5, session.Advanced.NumberOfRequests);

                    Assert.Equal(13, vals.Count);
                    Assert.Equal(baseline.AddMinutes(17), vals[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(19), vals[12].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(3, ranges.Count);
                    Assert.Equal(baseline.AddMinutes(0), ranges[0].From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(16), ranges[0].To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(17), ranges[1].From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(19), ranges[1].To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(40), ranges[2].From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(50), ranges[2].To, RavenTestHelper.DateTimeComparer.Instance);

                    // evict just the document
                    sessionOperations.DocumentsByEntity.Evict(user);
                    sessionOperations.DocumentsById.Remove(documentId);

                    // should go to server to get range [19, 40]
                    // and merge the result with existing ranges [17, 19] and [40, 50] 
                    // into single range [17, 50]

                    user = session.Load<User>(
                        documentId,
                        i => i.IncludeTimeSeries("HeartRateMeasures", baseline.AddMinutes(18), baseline.AddMinutes(48)));

                    Assert.Equal(6, session.Advanced.NumberOfRequests);

                    // should not go to server

                    vals = session.TimeSeriesFor<HeartRateMeasure>(documentId)
                        .Get(baseline.AddMinutes(18), baseline.AddMinutes(48))
                        .ToList();

                    Assert.Equal(6, session.Advanced.NumberOfRequests);

                    Assert.Equal(181, vals.Count);
                    Assert.Equal(baseline.AddMinutes(18), vals[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(48), vals[180].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(2, ranges.Count);
                    Assert.Equal(baseline.AddMinutes(0), ranges[0].From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(16), ranges[0].To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(17), ranges[1].From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(50), ranges[1].To, RavenTestHelper.DateTimeComparer.Instance);

                    // evict just the document
                    sessionOperations.DocumentsByEntity.Evict(user);
                    sessionOperations.DocumentsById.Remove(documentId);

                    // should go to server to get range [12, 22]
                    // and merge the result with existing ranges [0, 16] and [17, 50] 
                    // into single range [0, 50]

                    user = session.Load<User>(
                        documentId,
                        i => i.IncludeTimeSeries("HeartRateMeasures", baseline.AddMinutes(12), baseline.AddMinutes(22)));

                    Assert.Equal(7, session.Advanced.NumberOfRequests);

                    // should not go to server

                    vals = session.TimeSeriesFor<HeartRateMeasure>(documentId)
                        .Get(baseline.AddMinutes(12), baseline.AddMinutes(22))
                        .ToList();

                    Assert.Equal(7, session.Advanced.NumberOfRequests);

                    Assert.Equal(61, vals.Count);
                    Assert.Equal(baseline.AddMinutes(12), vals[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(22), vals[60].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(1, ranges.Count);
                    Assert.Equal(baseline.AddMinutes(0), ranges[0].From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(50), ranges[0].To, RavenTestHelper.DateTimeComparer.Instance);

                    // evict just the document
                    sessionOperations.DocumentsByEntity.Evict(user);
                    sessionOperations.DocumentsById.Remove(documentId);

                    // should go to server to get range [50, ∞]
                    // and merge the result with existing range [0, 50] into single range [0, ∞]

                    user = session.Load<User>(
                        documentId,
                        i => i.IncludeTimeSeries("HeartRateMeasures", TimeSeriesRangeType.Last, TimeValue.FromMinutes(10)));

                    Assert.Equal(8, session.Advanced.NumberOfRequests);

                    // should not go to server

                    vals = session.TimeSeriesFor<HeartRateMeasure>(documentId)
                        .Get(baseline.AddMinutes(50))
                        .ToList();

                    Assert.Equal(8, session.Advanced.NumberOfRequests);

                    Assert.Equal(60, vals.Count);
                    Assert.Equal(baseline.AddMinutes(50), vals[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(59).AddSeconds(50), vals[59].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(1, ranges.Count);
                    Assert.Equal(baseline.AddMinutes(0), ranges[0].From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTime.MaxValue, ranges[0].To, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void IncludeTimeSeriesAndUpdateExistingRangeInCache_Typed()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor<HeartRateMeasure>("users/ayende");
                    for (int i = 0; i < 360; i++)
                    {
                        var typedMeasure = new HeartRateMeasure
                        {
                            HeartRate = 6
                        };
                        tsf.Append(baseline.AddSeconds(i * 10), typedMeasure, "watches/fitbit");
                    }

                    session.SaveChanges();

                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor<HeartRateMeasure>("users/ayende")
                        .Get(baseline.AddMinutes(2), baseline.AddMinutes(10))
                        .ToList();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    Assert.Equal(49, vals.Count);
                    Assert.Equal(baseline.AddMinutes(2), vals[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(10), vals[48].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    session.TimeSeriesFor<HeartRateMeasure>("users/ayende")
                        .Append(baseline.AddMinutes(3).AddSeconds(3), new HeartRateMeasure
                        {
                            HeartRate = 6
                        }, "watches/fitbit");
                    session.SaveChanges();

                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    var user = session.Load<User>(
                        "users/ayende",
                        i => i.IncludeTimeSeries("HeartRateMeasures", baseline.AddMinutes(3), baseline.AddMinutes(5)));

                    Assert.Equal(3, session.Advanced.NumberOfRequests);

                    // should not go to server

                    vals = session.TimeSeriesFor<HeartRateMeasure>("users/ayende")
                        .Get(baseline.AddMinutes(3), baseline.AddMinutes(5))
                        .ToList();

                    Assert.Equal(3, session.Advanced.NumberOfRequests);

                    Assert.Equal(14, vals.Count);
                    Assert.Equal(baseline.AddMinutes(3), vals[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(3).AddSeconds(3), vals[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(5), vals[13].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                }
            }
        }

        [Fact]
        public async Task CanServeTimeSeriesFromCache_Rollup()
        {
            using (var store = GetDocumentStore())
            {
                var raw = new RawTimeSeriesPolicy(TimeSpan.FromHours(24));

                var p1 = new TimeSeriesPolicy("By6Hours", TimeSpan.FromHours(6), raw.RetentionTime * 4);
                var p2 = new TimeSeriesPolicy("By1Day", TimeSpan.FromDays(1), raw.RetentionTime * 5);
                var p3 = new TimeSeriesPolicy("By30Minutes", TimeSpan.FromMinutes(30), raw.RetentionTime * 2);
                var p4 = new TimeSeriesPolicy("By1Hour", TimeSpan.FromMinutes(60), raw.RetentionTime * 3);

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = raw,
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p1,p2,p3,p4
                            }
                        },
                    },
                    PolicyCheckFrequency = TimeSpan.FromSeconds(1)
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));
                await store.TimeSeries.RegisterAsync<User, StockPrice>();

                var database = await GetDocumentDatabaseInstanceFor(store);

                var now = DateTime.UtcNow;
                var nowMinutes = now.Minute;
                now = now.AddMinutes(-nowMinutes);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(-nowMinutes);

                var baseline = now.AddDays(-12);
                var total = TimeSpan.FromDays(12).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "users/karmel");
                    var ts = session.TimeSeriesFor<StockPrice>("users/karmel");
                    var entry = new StockPrice();
                    for (int i = 0; i <= total; i++)
                    {
                        entry.Open = i;
                        entry.Close = i + 100_000;
                        entry.High = i + 200_000;
                        entry.Low = i + 300_000;
                        entry.Volume = i + 400_000;
                        ts.Append(baseline.AddMinutes(i), entry, "watches/fitbit");
                    }
                    session.SaveChanges();
                }

                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                await QueryFromMultipleTimeSeries.VerifyFullPolicyExecution(store, config.Collections["Users"], rawName: "StockPrices");

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesRollupFor<StockPrice>("users/karmel", p1.Name);
                    var res = ts.Get().ToList();
                    Assert.Equal(16, res.Count);

                    // should not go to server
                    res = ts.Get(baseline, baseline.AddYears(1)).ToList();
                    Assert.Equal(16, res.Count);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task CanIncludeTypedTimeSeries_Rollup()
        {
            using (var store = GetDocumentStore())
            {
                var raw = new RawTimeSeriesPolicy(TimeSpan.FromHours(24));

                var p1 = new TimeSeriesPolicy("By6Hours", TimeSpan.FromHours(6), raw.RetentionTime * 4);
                var p2 = new TimeSeriesPolicy("By1Day", TimeSpan.FromDays(1), raw.RetentionTime * 5);
                var p3 = new TimeSeriesPolicy("By30Minutes", TimeSpan.FromMinutes(30), raw.RetentionTime * 2);
                var p4 = new TimeSeriesPolicy("By1Hour", TimeSpan.FromMinutes(60), raw.RetentionTime * 3);

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = raw,
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p1,p2,p3,p4
                            }
                        },
                    },
                    PolicyCheckFrequency = TimeSpan.FromSeconds(1)
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));
                await store.TimeSeries.RegisterAsync<User, StockPrice>();

                var database = await GetDocumentDatabaseInstanceFor(store);

                var now = DateTime.UtcNow;
                var nowMinutes = now.Minute;
                now = now.AddMinutes(-nowMinutes);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(-nowMinutes);

                var baseline = now.AddDays(-12);
                var total = TimeSpan.FromDays(12).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "users/karmel");
                    var ts = session.TimeSeriesFor<StockPrice>("users/karmel");
                    var entry = new StockPrice();
                    for (int i = 0; i <= total; i++)
                    {
                        entry.Open = i;
                        entry.Close = i + 100_000;
                        entry.High = i + 200_000;
                        entry.Low = i + 300_000;
                        entry.Volume = i + 400_000;
                        ts.Append(baseline.AddMinutes(i), entry, "watches/fitbit");
                    }
                    session.SaveChanges();
                }

                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                await QueryFromMultipleTimeSeries.VerifyFullPolicyExecution(store, config.Collections["Users"], rawName: "StockPrices");

                using (var session = store.OpenSession())
                {
                    var user = session.Query<User>()
                        .Include(x => x.IncludeTimeSeries($"StockPrices@{p1.Name}"))
                        .First();

                    // should not go to server
                    var ts = session.TimeSeriesRollupFor<StockPrice>(user.Id, p1.Name);
                    var res = ts.Get().ToList();
                    Assert.Equal(16, res.Count);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

    }
}
