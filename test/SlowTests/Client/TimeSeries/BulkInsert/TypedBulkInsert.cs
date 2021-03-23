using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Client.Attachments;
using Sparrow;
using Xunit;
using Xunit.Abstractions;
using static SlowTests.Client.TimeSeries.Session.TimeSeriesTypedSessionTests;

namespace SlowTests.Client.TimeSeries.BulkInsert
{
    public class TypedBulkInsert : RavenTestBase
    {
        public TypedBulkInsert(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanCreateSimpleTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureMilliseconds();
             
                const string documentId = "users/ayende";

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new { Name = "Oren" }, documentId);

                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor<HeartRateMeasure>(documentId, "Heartrate"))
                    {
                        var measure = new TimeSeriesEntry<HeartRateMeasure>
                        {
                            Timestamp = baseline,
                            Value = new HeartRateMeasure
                            {
                                HeartRate = 59
                            },
                            Tag = "watches/fitbit"
                        };

                        timeSeriesBulkInsert.Append(measure);
                    }
                }

                using (var session = store.OpenSession())
                {
                    var val = session.TimeSeriesFor<HeartRateMeasure>(documentId, "Heartrate")
                        .Get().Single();

                    Assert.Equal(59d, val.Value.HeartRate);
                    Assert.Equal("watches/fitbit", val.Tag);
                    Assert.Equal(baseline, val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void CanCreateSimpleTimeSeries2()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;
                const string documentId = "users/ayende";

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new { Name = "Oren" }, documentId);

                    var measure = new TimeSeriesEntry<HeartRateMeasure>
                    {
                        Timestamp = baseline.AddMinutes(1),
                        Value = new HeartRateMeasure
                        {
                            HeartRate = 59d
                        },
                        Tag = "watches/fitbit"
                    };

                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor<HeartRateMeasure>(documentId, "Heartrate"))
                    {
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(1), new HeartRateMeasure { HeartRate = 59 }, "watches/fitbit");
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(2), new HeartRateMeasure { HeartRate = 60 }, "watches/fitbit");
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(2), new HeartRateMeasure { HeartRate = 61 }, "watches/fitbit");
                    }
                }

                using (var session = store.OpenSession())
                {
                    var val = session.TimeSeriesFor<HeartRateMeasure>(documentId, "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(2, val.Count);
                }
            }
        }

        [Fact]
        public void CanCreateTimeSeriesWithoutPassingName()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureMilliseconds();

                const string documentId = "users/ayende";

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new User { Name = "Oren" }, documentId);

                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor<StockPrice>(documentId))
                    {
                        var measure = new TimeSeriesEntry<StockPrice>
                        {
                            Timestamp = baseline,
                            Value = new StockPrice
                            {
                                Close = 1,
                                Open = 2,
                                High = 3,
                                Low = 4,
                                Volume = 55
                            },
                            Tag = "tag"
                        };

                        timeSeriesBulkInsert.Append(measure);
                    }
                }

                using (var session = store.OpenSession())
                {
                    var val = session.TimeSeriesFor<StockPrice>(documentId)
                        .Get().Single();

                    Assert.Equal(1, val.Value.Close);
                    Assert.Equal(2, val.Value.Open);
                    Assert.Equal(3, val.Value.High);
                    Assert.Equal(4, val.Value.Low);
                    Assert.Equal(55, val.Value.Volume);

                    Assert.Equal("tag", val.Tag);
                    Assert.Equal(baseline, val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                }

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<User>(documentId);
                    var names = session.Advanced.GetTimeSeriesFor(doc);
                    
                    Assert.Equal(1, names.Count);
                    Assert.Equal("StockPrices", names[0]);
                }
            }
        }

        [Fact]
        public void CanDeleteTimestamp()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;
                const string documentId = "users/ayende";

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new { Name = "Oren" }, documentId);

                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor<HeartRateMeasure>(documentId, "Heartrate"))
                    {
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(1), new HeartRateMeasure { HeartRate = 59 }, "watches/fitbit");
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(2), new HeartRateMeasure { HeartRate = 69 }, "watches/fitbit");
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(3), new HeartRateMeasure { HeartRate = 79 }, "watches/fitbit");
                    }
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, documentId);
                    session.TimeSeriesFor(documentId, "Heartrate")
                        .Delete(baseline.AddMinutes(2));

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor(documentId, "Heartrate")
                         .Get(DateTime.MinValue, DateTime.MaxValue)
                         .ToList();

                    Assert.Equal(2, vals.Count);
                    Assert.Equal(new[] { 59d }, vals[0].Values);
                    Assert.Equal("watches/fitbit", vals[0].Tag);
                    Assert.Equal(baseline.AddMinutes(1), vals[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 79d }, vals[1].Values);
                    Assert.Equal("watches/fitbit", vals[1].Tag);
                    Assert.Equal(baseline.AddMinutes(3), vals[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void UsingDifferentTags()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;
                const string documentId = "users/ayende";

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new { Name = "Oren" }, documentId);

                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor<HeartRateMeasure>(documentId, "Heartrate"))
                    {
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(1), new HeartRateMeasure { HeartRate = 59 }, "watches/fitbit");
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(2), new HeartRateMeasure { HeartRate = 70 }, "watches/apple");
                    }
                }

                using (var session = store.OpenSession())
                {

                    var vals = session.TimeSeriesFor(documentId, "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();

                    Assert.Equal(2, vals.Count);
                    Assert.Equal(new[] { 59d }, vals[0].Values);
                    Assert.Equal("watches/fitbit", vals[0].Tag);
                    Assert.Equal(baseline.AddMinutes(1), vals[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 70d }, vals[1].Values);
                    Assert.Equal("watches/apple", vals[1].Tag);
                    Assert.Equal(baseline.AddMinutes(2), vals[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void CanStoreAndReadMultipleTimestamps()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;
                const string documentId = "users/ayende";

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new { Name = "Oren" }, documentId);
                    using (var ts = bulkInsert.TimeSeriesFor<HeartRateMeasure>(documentId))
                    {
                        ts.Append(baseline.AddMinutes(1), new HeartRateMeasure { HeartRate = 59 }, "watches/fitbit");
                    }
                }

                using (var bulkInsert = store.BulkInsert())
                {
                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor<HeartRateMeasure>(documentId))
                    {
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(2), new HeartRateMeasure { HeartRate = 61 }, "watches/fitbit");
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(3), new HeartRateMeasure { HeartRate = 62 }, "watches/apple-watch");
                    }
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor<HeartRateMeasure>(documentId)
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();

                    Assert.Equal(3, vals.Count);

                    Assert.Equal(59, vals[0].Value.HeartRate);
                    Assert.Equal("watches/fitbit", vals[0].Tag);
                    Assert.Equal(baseline.AddMinutes(1), vals[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(61, vals[1].Value.HeartRate);
                    Assert.Equal("watches/fitbit", vals[1].Tag);
                    Assert.Equal(baseline.AddMinutes(2), vals[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(62, vals[2].Value.HeartRate);
                    Assert.Equal("watches/apple-watch", vals[2].Tag);
                    Assert.Equal(baseline.AddMinutes(3), vals[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void CanStoreLargeNumberOfValues()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                const string documentId = "users/ayende";

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, documentId);
                    session.SaveChanges();
                }

                var offset = 0;

                for (int i = 0; i < 10; i++)
                {
                    using (var bulkInsert = store.BulkInsert())
                    {
                        using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor<HeartRateMeasure>(documentId, "Heartrate"))
                        {
                            for (int j = 0; j < 1000; j++)
                            {
                                timeSeriesBulkInsert.Append(baseline.AddMinutes(offset++), new HeartRateMeasure { HeartRate = offset }, "watches/fitbit");
                            }
                        }
                    }
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor<HeartRateMeasure>(documentId, "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();

                    Assert.Equal(10_000, vals.Count);

                    for (int i = 0; i < 10_000; i++)
                    {
                        Assert.Equal(baseline.AddMinutes(i), vals[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(1 + i, vals[i].Value.HeartRate);
                    }
                }
            }
        }

        [Fact]
        public void CanStoreValuesOutOfOrder()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                const string documentId = "users/ayende";

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, documentId);
                    session.SaveChanges();
                }

                const int retries = 1000;

                var offset = 0;

                using (var bulkInsert = store.BulkInsert())
                {
                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor<HeartRateMeasure>(documentId, "Heartrate"))
                    {
                        for (int j = 0; j < retries; j++)
                        {
                            timeSeriesBulkInsert.Append(baseline.AddMinutes(offset), new HeartRateMeasure { HeartRate = offset }, "watches/fitbit");
                            offset += 5;
                        }
                    }
                }

                offset = 1;

                using (var bulkInsert = store.BulkInsert())
                {
                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor<HeartRateMeasure>(documentId, "Heartrate"))
                    {
                        for (int j = 0; j < retries; j++)
                        {
                            timeSeriesBulkInsert.Append(baseline.AddMinutes(offset), new HeartRateMeasure { HeartRate = offset }, "watches/fitbit");
                            offset += 5;
                        }
                    }
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor<HeartRateMeasure>(documentId, "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();

                    Assert.Equal(2 * retries, vals.Count);

                    offset = 0;
                    for (int i = 0; i < retries; i++)
                    {
                        Assert.Equal(baseline.AddMinutes(offset), vals[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(offset, vals[i].Value.HeartRate);

                        offset++;
                        i++;

                        Assert.Equal(baseline.AddMinutes(offset), vals[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(offset, vals[i].Value.HeartRate);


                        offset += 4;
                    }
                }
            }
        }

        private struct StockPrice
        {
            [TimeSeriesValue(0)] public double Open;
            [TimeSeriesValue(1)] public double Close;
            [TimeSeriesValue(2)] public double High;
            [TimeSeriesValue(3)] public double Low;
            [TimeSeriesValue(4)] public double Volume;
        }

        [Fact]
        public void CanGetTimeSeriesNames()
        {
            using (var store = GetDocumentStore())
            {
                const string documentId1 = "users/karmel";
                const string documentId2 = "users/ayende";

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new User(), documentId1);
                    using (var ts = bulkInsert.TimeSeriesFor<StockPrice>(documentId1, "Nasdaq2"))
                        ts.Append(DateTime.Now, new StockPrice { Open = 7547.31, Close = 7123.5 }, "web");
                }

                using (var bulkInsert = store.BulkInsert())
                {
                    using (var ts = bulkInsert.TimeSeriesFor<HeartRateMeasure>(documentId1, "Heartrate2"))
                        ts.Append(DateTime.Now, new HeartRateMeasure{ HeartRate = 76 }, "watches/apple");
                }

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new User(), documentId2);
                    using (var ts = bulkInsert.TimeSeriesFor<StockPrice>(documentId2, "Nasdaq"))
                        ts.Append(DateTime.Now, new StockPrice { Open = 7547.31, Close = 7123.5 }, "web");
                }

                using (var bulkInsert = store.BulkInsert())
                {
                    using (var ts = bulkInsert.TimeSeriesFor<HeartRateMeasure>(documentId2, "Heartrate"))
                        ts.Append(DateTime.Now, new HeartRateMeasure { HeartRate = 58 }, "fitbit");
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>(documentId2);
                    var tsNames = session.Advanced.GetTimeSeriesFor(user);
                    Assert.Equal(2, tsNames.Count);

                    // should be sorted
                    Assert.Equal("Heartrate", tsNames[0]);
                    Assert.Equal("Nasdaq", tsNames[1]);
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>(documentId1);
                    var tsNames = session.Advanced.GetTimeSeriesFor(user);
                    Assert.Equal(2, tsNames.Count);

                    // should be sorted
                    Assert.Equal("Heartrate2", tsNames[0]);
                    Assert.Equal("Nasdaq2", tsNames[1]);
                }

                using (var bulkInsert = store.BulkInsert())
                {
                    using (var ts = bulkInsert.TimeSeriesFor<HeartRateMeasure>(documentId2, "heartrate"))
                        ts.Append(DateTime.Today.AddMinutes(1), new HeartRateMeasure { HeartRate = 58 }, "fitbit");
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/ayende");
                    var tsNames = session.Advanced.GetTimeSeriesFor(user);
                    Assert.Equal(2, tsNames.Count);

                    // should preserve original casing
                    Assert.Equal("Heartrate", tsNames[0]);
                    Assert.Equal("Nasdaq", tsNames[1]);
                }
            }
        }

        [Fact]
        public void CanGetTimeSeriesNames2()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.ToUniversalTime();
                const string documentId = "users/ayende";

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, documentId);
                    session.SaveChanges();
                }

                var offset = 0;

                for (int i = 0; i < 100; i++)
                {
                    using (var bulkInsert = store.BulkInsert())
                    {
                        using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor<HeartRateMeasure>(documentId, "Heartrate"))
                        {
                            for (int j = 0; j < 1000; j++)
                            {
                                timeSeriesBulkInsert.Append(baseline.AddMinutes(offset++), new HeartRateMeasure { HeartRate = offset }, "watches/fitbit");
                            }
                        }
                    }
                }

                offset = 0;

                for (int i = 0; i < 100; i++)
                {
                    using (var bulkInsert = store.BulkInsert())
                    {
                        using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor<HeartRateMeasure>(documentId, "Pulse"))
                        {
                            for (int j = 0; j < 1000; j++)
                            {
                                timeSeriesBulkInsert.Append(baseline.AddMinutes(offset++), new HeartRateMeasure { HeartRate = offset }, "watches/fitbit");
                            }
                        }
                    }
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor<HeartRateMeasure>(documentId, "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(100_000, vals.Count);

                    for (int i = 0; i < 100_000; i++)
                    {
                        Assert.Equal(baseline.AddMinutes(i), vals[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(1 + i, vals[i].Value.HeartRate);
                    }
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor<HeartRateMeasure>(documentId, "Pulse")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(100_000, vals.Count);

                    for (int i = 0; i < 100_000; i++)
                    {
                        Assert.Equal(baseline.AddMinutes(i), vals[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(1 + i, vals[i].Value.HeartRate);
                    }
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/ayende");
                    var tsNames = session.Advanced.GetTimeSeriesFor(user);
                    Assert.Equal(2, tsNames.Count);

                    // should be sorted
                    Assert.Equal("Heartrate", tsNames[0]);
                    Assert.Equal("Pulse", tsNames[1]);
                }
            }
        }

        [Fact]
        public void DocumentsChangeVectorShouldBeUpdatedAfterAddingNewTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 1; i <= 5; i++)
                    {
                        var id = $"users/{i}";
                        bulkInsert.Store(new User
                        {
                            Name = "Oren"
                        }, id);

                        using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor<HeartRateMeasure>(id, "Heartrate"))
                        {
                            timeSeriesBulkInsert.Append(baseline.AddMinutes(1), new HeartRateMeasure { HeartRate = 59 }, "watches/fitbit");
                        }
                    }
                }

                var cvs = new List<string>();

                using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 1; i <= 5; i++)
                    {
                        var id = $"users/{i}";
                        bulkInsert.Store(new User
                        {
                            Name = "Oren"
                        }, id);

                        using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor<HeartRateMeasure>(id, "Heartrate"))
                        {
                            timeSeriesBulkInsert.Append(baseline.AddMinutes(1), new HeartRateMeasure { HeartRate = 59 }, "watches/fitbit");
                        }
                    }
                }

                using (var bulkInsert = store.BulkInsert())
                using (var session = store.OpenSession())
                {
                    for (int i = 2; i < 5; i++)
                    {
                        var id = $"users/{i}";
                        var u = session.Load<User>(id);
                        var cv = session.Advanced.GetChangeVectorFor(u);
                        cvs.Add(cv);

                        using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor<StockPrice>(id, "Nasdaq"))
                        {
                            timeSeriesBulkInsert.Append(baseline.AddMinutes(1), new StockPrice
                            {
                                High = 4012.5d
                            }, "web");
                        }
                    }
                }

                using (var session = store.OpenSession())
                {
                    for (int i = 2; i < 5; i++)
                    {
                        var u = session.Load<User>($"users/{i}");
                        var cv = session.Advanced.GetChangeVectorFor(u);
                        var oldCv = cvs[i - 2];
                        var conflictStatus = ChangeVectorUtils.GetConflictStatus(cv, oldCv);

                        Assert.Equal(ConflictStatus.Update, conflictStatus);
                    }
                }
            }
        }

        [Fact]
        public void CanStoreAndReadMultipleTimeseriesForDifferentDocuments()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;
                const string documentId1 = "users/ayende";
                const string documentId2 = "users/grisha";

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new { Name = "Oren" }, documentId1);
                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor<HeartRateMeasure>(documentId1, "Heartrate"))
                    {
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(1), new HeartRateMeasure { HeartRate = 59 }, "watches/fitbit");
                    }

                    bulkInsert.Store(new { Name = "Grisha" }, documentId2);
                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor<HeartRateMeasure>(documentId2, "Heartrate"))
                    {
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(1), new HeartRateMeasure { HeartRate = 59 }, "watches/fitbit");
                    }
                }

                using (var bulkInsert = store.BulkInsert())
                {
                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor<HeartRateMeasure>(documentId1, "Heartrate"))
                    {
                        var messure = new TimeSeriesEntry<HeartRateMeasure>
                        {
                            Timestamp = baseline.AddMinutes(2),
                            Tag = "watches/fitbit",
                            Value = new HeartRateMeasure
                            {
                                HeartRate = 61
                            }
                        };

                        timeSeriesBulkInsert.Append(messure);
                    }

                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor<HeartRateMeasure>(documentId2, "Heartrate"))
                    {
                        var measure = new TimeSeriesEntry<HeartRateMeasure>
                        {
                            Timestamp = baseline.AddMinutes(2),
                            Tag = "watches/fitbit",
                            Value = new HeartRateMeasure
                            {
                                HeartRate = 61
                            }
                        };

                        timeSeriesBulkInsert.Append(measure);
                    }
                    
                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor<HeartRateMeasure>(documentId1, "Heartrate"))
                    {
                        var measure = new TimeSeriesEntry<HeartRateMeasure>
                        {
                            Timestamp = baseline.AddMinutes(3),
                            Tag = "watches/apple-watch",
                            Value = new HeartRateMeasure
                            {
                                HeartRate = 62
                            }
                        };

                        timeSeriesBulkInsert.Append(measure);
                    }

                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor<HeartRateMeasure>(documentId2, "Heartrate"))
                    {
                        var measure = new TimeSeriesEntry<HeartRateMeasure>
                        {
                            Timestamp = baseline.AddMinutes(3),
                            Tag = "watches/apple-watch",
                            Value = new HeartRateMeasure
                            {
                                HeartRate = 62
                            }
                        };

                        timeSeriesBulkInsert.Append(measure);
                    }
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor<HeartRateMeasure>(documentId1, "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    ValidateValues();

                    vals = session.TimeSeriesFor<HeartRateMeasure>(documentId2, "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    ValidateValues();

                    void ValidateValues()
                    {
                        Assert.Equal(3, vals.Count);

                        Assert.Equal(59, vals[0].Value.HeartRate);
                        Assert.Equal("watches/fitbit", vals[0].Tag);
                        Assert.Equal(baseline.AddMinutes(1), vals[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        Assert.Equal(61, vals[1].Value.HeartRate);
                        Assert.Equal("watches/fitbit", vals[1].Tag);
                        Assert.Equal(baseline.AddMinutes(2), vals[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        Assert.Equal(62, vals[2].Value.HeartRate);
                        Assert.Equal("watches/apple-watch", vals[2].Tag);
                        Assert.Equal(baseline.AddMinutes(3), vals[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    }
                }
            }
        }

        [Theory]
        [InlineData(128)]
        [InlineData(1024)]
        [InlineData(10*1024)]
        [InlineData(100*1024)]
        public void CanAppendALotOfTimeSeries(int numberOfMeasures)
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.ToUniversalTime();
                const string documentId = "users/ayende";

                var offset = 0;

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new { Name = "Oren" }, documentId);

                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor<HeartRateMeasure>(documentId, "Heartrate"))
                    {
                        for (int j = 0; j < numberOfMeasures; j++)
                        {
                            timeSeriesBulkInsert.Append(baseline.AddMinutes(offset++), new HeartRateMeasure { HeartRate = offset }, "watches/fitbit");
                        }
                    }
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor<HeartRateMeasure>(documentId, "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(numberOfMeasures, vals.Count);

                    for (int i = 0; i < numberOfMeasures; i++)
                    {
                        Assert.Equal(baseline.AddMinutes(i), vals[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(1 + i, vals[i].Value.HeartRate);
                    }
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/ayende");
                    var tsNames = session.Advanced.GetTimeSeriesFor(user);
                    Assert.Equal(1, tsNames.Count);

                    Assert.Equal("Heartrate", tsNames[0]);
                }
            }
        }



        [Fact]
        public void ErrorHandling()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;
                const string documentId = "users/ayende";

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new { Name = "Oren" }, documentId);

                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor(documentId, "Heartrate"))
                    {
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(1), 59d, "watches/fitbit");

                        var error = Assert.Throws<InvalidOperationException>(() => bulkInsert.Store(new { Name = "Oren" }, documentId));
                        AssertError();

                        error = Assert.Throws<InvalidOperationException>(() => bulkInsert.CountersFor("test").Increment("1", 1));
                        AssertError();

                        error = Assert.Throws<InvalidOperationException>(() => bulkInsert.TimeSeriesFor(documentId, "Pulse"));
                        AssertError();

                        error = Assert.Throws<InvalidOperationException>(() => bulkInsert.TimeSeriesFor(documentId, "Heartrate"));
                        AssertError();

                        void AssertError()
                        {
                            Assert.Equal("There is an already running time series operation, did you forget to Dispose it?", error.Message);
                        }
                    }
                }

                using (var session = store.OpenSession())
                {
                    var val = session.TimeSeriesFor(documentId, "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .Single();

                    Assert.Equal(new[] { 59d }, val.Values);
                    Assert.Equal("watches/fitbit", val.Tag);
                    Assert.Equal(baseline.AddMinutes(1), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }
        
        [Fact]
        public async Task CanHaveBulkInsertWithDocumentsAndAttachmentAndCountersAndTypedTimeSeries()
        {
            const int count = 100;
            const int size = 64 * 1024;
            
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();
                var streams = new Dictionary<string, Dictionary<string, MemoryStream>>();
                var counters = new Dictionary<string, string>();
                var bulks = new Dictionary<string, BulkInsertOperation.AttachmentsBulkInsert>();
                
                using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 0; i < count; i++)
                    {
                        var id = $"name/{i}";
                        streams[id] = new Dictionary<string, MemoryStream>();

                        // insert Documents
                        bulkInsert.Store(new User {Name = $"Name_{i}" }, id);

                        bulks[id] = bulkInsert.AttachmentsFor(id);
                    }

                    foreach (var bulk in bulks)
                    {
                        var rnd = new Random(DateTime.Now.Millisecond);
                        var bArr = new byte[size];
                        rnd.NextBytes(bArr);
                        var name = $"{bulk.Key}_{rnd.Next(100)}";
                        var stream = new MemoryStream(bArr);
                        
                        // insert Attachments
                        await bulk.Value.StoreAsync(name, stream);

                        stream.Position = 0;
                        streams[bulk.Key][name] = stream;
                        
                        // insert Counters
                        await bulkInsert.CountersFor(bulk.Key).IncrementAsync(name);
                        counters[bulk.Key] = name;

                        // insert Time Series
                        using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor<HeartRateMeasure>(bulk.Key, "HeartRate"))
                        {
                            timeSeriesBulkInsert.Append(baseline.AddMinutes(1), new HeartRateMeasure { HeartRate = 59 }, "watches/fitBit");
                        }
                    }
                }

                foreach (var id in streams.Keys)
                {
                    using (var session = store.OpenSession())
                    {
                        
                        var timeSeriesVal = session.TimeSeriesFor<HeartRateMeasure>(id, "HeartRate")
                            .Get(DateTime.MinValue, DateTime.MaxValue)
                            .FirstOrDefault();

                        Assert.NotNull(timeSeriesVal);
                        Assert.Equal(59, timeSeriesVal.Value.HeartRate);
                        Assert.Equal("watches/fitBit", timeSeriesVal.Tag);
                        Assert.Equal(baseline.AddMinutes(1), timeSeriesVal.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        
                        var attachmentsNames = streams.Select(x => new AttachmentRequest(id, x.Key));
                        var attachmentsEnumerator = session.Advanced.Attachments.Get(attachmentsNames);
                
                        while (attachmentsEnumerator.MoveNext())
                        {
                            Assert.NotNull(attachmentsEnumerator.Current);
                            Assert.True(AttachmentsStreamTests.CompareStreams(attachmentsEnumerator.Current.Stream, streams[id][attachmentsEnumerator.Current.Details.Name]));
                        }
                    }
                
                    var val = store.Operations
                        .Send(new GetCountersOperation(id, new[] { counters[id] }))
                        .Counters[0]?.TotalValue;
                    Assert.Equal(1, val);
                }
            }
        }
    }
}
