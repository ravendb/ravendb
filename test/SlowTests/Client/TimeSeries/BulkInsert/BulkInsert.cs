using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Client.Attachments;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Threading;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.BulkInsert
{
    public class BulkInsert : RavenTestBase
    {
        public BulkInsert(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanCreateSimpleTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday.EnsureMilliseconds();

                const string documentId = "users/ayende";

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new { Name = "Oren" }, documentId);

                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor(documentId, "Heartrate"))
                    {
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(1), 59d, "watches/fitbit");
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
        public void CanParseUnixTimeFormat()
        {
            using (var ctx = new JsonOperationContext(4096, 4096, 32, SharedMultipleUseFlag.None))
            {
                var djv = new DynamicJsonValue
                {
                    ["Name"] = "CanParseUnixTimeFormat",
                    ["TimeFormat"] = TimeSeriesOperation.TimeFormat.UnixTimeInMs,
                    ["Appends"] = new DynamicJsonArray
                    {
                        new DynamicJsonArray
                        {
                            1588464000021
                            ,3,1,2,3,
                            "SomeTag"
                        }
                    }
                };
                var input = ctx.ReadObject(djv, "CanParseUnixTimeFormat");
                var op = TimeSeriesOperation.ParseForBulkInsert(input);
                Assert.Equal("CanParseUnixTimeFormat", op.Name);
                Assert.Equal("SomeTag", op.Appends[0].Tag);
                Assert.Equal(new double[] { 1, 2, 3 }, op.Appends[0].Values);
                Assert.Equal(new DateTime(2020, 5, 3, 0, 0, 0, 21), op.Appends[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);


                djv = new DynamicJsonValue
                {
                    ["Name"] = "CanParseUnixTimeFormat",
                    ["TimeFormat"] = TimeSeriesOperation.TimeFormat.UnixTimeInNs,
                    ["Appends"] = new DynamicJsonArray
                    {
                        new DynamicJsonArray
                        {
                            1257894000000000000
                            ,3,3,2,1,
                            "SomeTag2"
                        }
                    }
                };
                input = ctx.ReadObject(djv, "CanParseUnixTimeFormat");
                op = TimeSeriesOperation.ParseForBulkInsert(input);
                Assert.Equal("CanParseUnixTimeFormat", op.Name);
                Assert.Equal("SomeTag2", op.Appends[0].Tag);
                Assert.Equal(new double[] { 3, 2, 1 }, op.Appends[0].Values);
                Assert.Equal(new DateTime(2009, 11, 10, 23, 0, 0), op.Appends[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
            }
        }

        [Fact]
        public void CanCreateSimpleTimeSeriesWithDifferentCulture()
        {
            var currentCulture = CultureInfo.CurrentCulture;

            try
            {
                CultureInfo.CurrentCulture = new CultureInfo("de-DE", true);

                using (var store = GetDocumentStore())
                {
                    var baseline = RavenTestHelper.UtcToday;
                    const string documentId = "users/ayende";

                    using (var bulkInsert = store.BulkInsert())
                    {
                        bulkInsert.Store(new { Name = "Oren" }, documentId);

                        using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor(documentId, "Heartrate"))
                        {
                            timeSeriesBulkInsert.Append(baseline.AddMinutes(1), 59.5, "watches/fitbit");
                        }
                    }

                    using (var session = store.OpenSession())
                    {
                        var val = session.TimeSeriesFor(documentId, "Heartrate")
                            .Get(DateTime.MinValue, DateTime.MaxValue)
                            .Single();

                        Assert.Equal(new[] { 59.5 }, val.Values);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMinutes(1), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    }
                }
            }
            finally
            {
                CultureInfo.CurrentCulture = currentCulture;
            }
        }

        [Fact]
        public void CanCreateSimpleTimeSeries2()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                const string documentId = "users/ayende";

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new { Name = "Oren" }, documentId);

                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor(documentId, "Heartrate"))
                    {
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(1), 59d, "watches/fitbit");
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(2), 60d, "watches/fitbit");
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(2), 61d, "watches/fitbit");
                    }
                }

                using (var session = store.OpenSession())
                {
                    var val = session.TimeSeriesFor(documentId, "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(2, val.Count);
                }
            }
        }

        [Fact]
        public void CanDeleteTimestamp()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                const string documentId = "users/ayende";

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new { Name = "Oren" }, documentId);

                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor(documentId, "Heartrate"))
                    {
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(1), 59d, "watches/fitbit");
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(2), 69d, "watches/fitbit");
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(3), 79d, "watches/fitbit");
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
                var baseline = RavenTestHelper.UtcToday;
                const string documentId = "users/ayende";

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new { Name = "Oren" }, documentId);

                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor(documentId, "Heartrate"))
                    {
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(1), 59d, "watches/fitbit");
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(2), 70d, "watches/apple");
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
        public void UsingDifferentNumberOfValues_SmallToLarge()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                const string documentId = "users/ayende";

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new { Name = "Oren" }, documentId);

                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor(documentId, "Heartrate"))
                    {
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(1), 59d, "watches/fitbit");
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(2), new[] { 70d, 120d, 80d }, "watches/apple");
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(3), 69d, "watches/fitbit");
                    }
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor(documentId, "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();

                    Assert.Equal(3, vals.Count);
                    Assert.Equal(new[] { 59d }, vals[0].Values);
                    Assert.Equal("watches/fitbit", vals[0].Tag);
                    Assert.Equal(baseline.AddMinutes(1), vals[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 70d, 120d, 80d }, vals[1].Values);
                    Assert.Equal("watches/apple", vals[1].Tag);
                    Assert.Equal(baseline.AddMinutes(2), vals[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 69d }, vals[2].Values);
                    Assert.Equal("watches/fitbit", vals[2].Tag);
                    Assert.Equal(baseline.AddMinutes(3), vals[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void UsingDifferentNumberOfValues_LargeToSmall()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                const string documentId = "users/ayende";

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new { Name = "Oren" }, documentId);

                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor(documentId, "Heartrate"))
                    {
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(1), new[] { 70d, 120d, 80d }, "watches/apple");
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(2), new[] { 59d }, "watches/fitbit");
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(3), new[] { 69d }, "watches/fitbit");
                    }
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor(documentId, "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();

                    Assert.Equal(3, vals.Count);

                    Assert.Equal(new[] { 70d, 120d, 80d }, vals[0].Values);
                    Assert.Equal("watches/apple", vals[0].Tag);
                    Assert.Equal(baseline.AddMinutes(1), vals[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 59d }, vals[1].Values);
                    Assert.Equal("watches/fitbit", vals[1].Tag);
                    Assert.Equal(baseline.AddMinutes(2), vals[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 69d }, vals[2].Values);
                    Assert.Equal("watches/fitbit", vals[2].Tag);
                    Assert.Equal(baseline.AddMinutes(3), vals[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void CanStoreAndReadMultipleTimestamps()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                const string documentId = "users/ayende";

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new { Name = "Oren" }, documentId);
                    using (var ts = bulkInsert.TimeSeriesFor(documentId, "Heartrate"))
                    {
                        ts.Append(baseline.AddMinutes(1), 59d, "watches/fitbit");
                    }
                }

                using (var bulkInsert = store.BulkInsert())
                {
                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor(documentId, "Heartrate"))
                    {
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(2), 61d, "watches/fitbit");
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(3), 62d, "watches/apple-watch");
                    }
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor(documentId, "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();

                    Assert.Equal(3, vals.Count);

                    Assert.Equal(new[] { 59d }, vals[0].Values);
                    Assert.Equal("watches/fitbit", vals[0].Tag);
                    Assert.Equal(baseline.AddMinutes(1), vals[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 61d }, vals[1].Values);
                    Assert.Equal("watches/fitbit", vals[1].Tag);
                    Assert.Equal(baseline.AddMinutes(2), vals[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 62d }, vals[2].Values);
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
                        using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor(documentId, "Heartrate"))
                        {
                            for (int j = 0; j < 1000; j++)
                            {
                                timeSeriesBulkInsert.Append(baseline.AddMinutes(offset++), new double[] { offset }, "watches/fitbit");
                            }
                        }
                    }
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor(documentId, "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();

                    Assert.Equal(10_000, vals.Count);

                    for (int i = 0; i < 10_000; i++)
                    {
                        Assert.Equal(baseline.AddMinutes(i), vals[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(1 + i, vals[i].Values[0]);
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
                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor(documentId, "Heartrate"))
                    {
                        for (int j = 0; j < retries; j++)
                        {
                            timeSeriesBulkInsert.Append(baseline.AddMinutes(offset), new double[] { offset }, "watches/fitbit");
                            offset += 5;
                        }
                    }
                }

                offset = 1;

                using (var bulkInsert = store.BulkInsert())
                {
                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor(documentId, "Heartrate"))
                    {
                        for (int j = 0; j < retries; j++)
                        {
                            timeSeriesBulkInsert.Append(baseline.AddMinutes(offset), new double[] { offset }, "watches/fitbit");
                            offset += 5;
                        }
                    }
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor(documentId, "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();

                    Assert.Equal(2 * retries, vals.Count);

                    offset = 0;
                    for (int i = 0; i < retries; i++)
                    {
                        Assert.Equal(baseline.AddMinutes(offset), vals[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(offset, vals[i].Values[0]);

                        offset++;
                        i++;

                        Assert.Equal(baseline.AddMinutes(offset), vals[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(offset, vals[i].Values[0]);


                        offset += 4;
                    }
                }
            }
        }

        [Fact]
        public void CanUseLocalDateTimeWhenRequestingTimeSeriesRange()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                const string documentId = "users/ayende";

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new { Name = "Oren" }, documentId);
                    using (var ts = bulkInsert.TimeSeriesFor(documentId, "Heartrate"))
                    {
                        ts.Append(baseline, new[] { 0d }, "watches/fitbit");
                    }
                }

                using (var bulkInsert = store.BulkInsert())
                {
                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor(documentId, "Heartrate"))
                    {
                        for (double i = 1; i < 10; i++)
                        {
                            timeSeriesBulkInsert.Append(baseline.AddMinutes(i), i, "watches/fitbit");
                        }
                    }
                }

                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        var vals = session.TimeSeriesFor(documentId, "Heartrate")
                            .Get(baseline.AddMinutes(i), DateTime.MaxValue)
                            .ToList();

                        Assert.Equal(10 - i, vals.Count);

                        for (double j = 0; j < vals.Count; j++)
                        {
                            Assert.Equal(new[] { j + i }, vals[(int)j].Values);
                        }
                    }
                }

                var maxTimeStamp = baseline.AddMinutes(9);

                for (int i = 1; i < 10; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        var vals = session.TimeSeriesFor(documentId, "Heartrate")
                            .Get(baseline, maxTimeStamp.AddMinutes(-i))
                            .ToList();

                        Assert.Equal(10 - i, vals.Count);

                        for (double j = 0; j < vals.Count; j++)
                        {
                            Assert.Equal(new[] { j }, vals[(int)j].Values);
                        }
                    }
                }
            }
        }

        [Fact]
        public void CanRequestNonExistingTimeSeriesRange()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                const string documentId = "users/ayende";

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new { Name = "Oren" }, documentId);

                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor(documentId, "Heartrate"))
                    {
                        timeSeriesBulkInsert.Append(baseline, new[] { 58d }, "watches/fitbit");
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(10), new[] { 60d }, "watches/fitbit");
                    }
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor(documentId, "Heartrate")
                        .Get(baseline.AddMinutes(-10), baseline.AddMinutes(-5))?
                        .ToList();

                    Assert.Empty(vals);

                    vals = session.TimeSeriesFor(documentId, "Heartrate")
                        .Get(baseline.AddMinutes(5), baseline.AddMinutes(9))?
                        .ToList();

                    Assert.Empty(vals);
                }
            }
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
                    using (var ts = bulkInsert.TimeSeriesFor(documentId1, "Nasdaq2"))
                        ts.Append(DateTime.Now, new[] { 7547.31 }, "web");
                }

                using (var bulkInsert = store.BulkInsert())
                {
                    using (var ts = bulkInsert.TimeSeriesFor(documentId1, "Heartrate2"))
                        ts.Append(DateTime.Now, new[] { 7547.31 }, "web");
                }

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new User(), documentId2);
                    using (var ts = bulkInsert.TimeSeriesFor(documentId2, "Nasdaq"))
                        ts.Append(DateTime.Now, new[] { 7547.31 }, "web");
                }

                using (var bulkInsert = store.BulkInsert())
                {
                    using (var ts = bulkInsert.TimeSeriesFor(documentId2, "Heartrate"))
                        ts.Append(DateTime.Now, new[] { 58d }, "fitbit");
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
                    using (var ts = bulkInsert.TimeSeriesFor(documentId2, "heartrate"))
                        ts.Append(RavenTestHelper.UtcToday.AddMinutes(1), new[] { 58d }, "fitbit");
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
                var baseline = RavenTestHelper.UtcToday.ToUniversalTime();
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
                        using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor(documentId, "Heartrate"))
                        {
                            for (int j = 0; j < 1000; j++)
                            {
                                timeSeriesBulkInsert.Append(baseline.AddMinutes(offset++), new double[] { offset }, "watches/fitbit");
                            }
                        }
                    }
                }

                offset = 0;

                for (int i = 0; i < 100; i++)
                {
                    using (var bulkInsert = store.BulkInsert())
                    {
                        using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor(documentId, "Pulse"))
                        {
                            for (int j = 0; j < 1000; j++)
                            {
                                timeSeriesBulkInsert.Append(baseline.AddMinutes(offset++), new double[] { offset }, "watches/fitbit");
                            }
                        }
                    }
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor(documentId, "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(100_000, vals.Count);

                    for (int i = 0; i < 100_000; i++)
                    {
                        Assert.Equal(baseline.AddMinutes(i), vals[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(1 + i, vals[i].Values[0]);
                    }
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor(documentId, "Pulse")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(100_000, vals.Count);

                    for (int i = 0; i < 100_000; i++)
                    {
                        Assert.Equal(baseline.AddMinutes(i), vals[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(1 + i, vals[i].Values[0]);
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
                var baseline = RavenTestHelper.UtcToday;

                using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 1; i <= 5; i++)
                    {
                        var id = $"users/{i}";
                        bulkInsert.Store(new User
                        {
                            Name = "Oren"
                        }, id);

                        using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor(id, "Heartrate"))
                        {
                            timeSeriesBulkInsert.Append(baseline.AddMinutes(1), new[] { 59d }, "watches/fitbit");
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

                        using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor(id, "Heartrate"))
                        {
                            timeSeriesBulkInsert.Append(baseline.AddMinutes(1), new[] { 59d }, "watches/fitbit");
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

                        using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor(id, "Nasdaq"))
                        {
                            timeSeriesBulkInsert.Append(baseline.AddMinutes(1), 4012.5d, "web");
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
        public void CanUseIEnumerableValues()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                const string documentId = "users/ayende";
                var values = new List<double>
                {
                    59d
                };

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new { Name = "Oren" }, "users/ayende");
                    using (var ts = bulkInsert.TimeSeriesFor(documentId, "Heartrate"))
                    {
                        ts.Append(baseline.AddMinutes(1), values, "watches/fitbit");
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
        public void ShouldDeleteTimeSeriesUponDocumentDeletion()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                const string documentId = "users/ayende";

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new User { Name = "Oren" }, documentId);

                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor(documentId, "Heartrate"))
                    {
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(1), new double[] { 59d }, "watches/fitbit");
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(2), new double[] { 59d }, "watches/fitbit");
                    }

                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor(documentId, "Heartrate2"))
                    {
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(1), new double[] { 59d }, "watches/apple");
                    }
                }

                using (var session = store.OpenSession())
                {
                    session.Delete(documentId);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor(documentId, "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue);
                    Assert.Null(vals);

                    vals = session.TimeSeriesFor(documentId, "Heartrate2")
                        .Get(DateTime.MinValue, DateTime.MaxValue);
                    Assert.Null(vals);
                }
            }
        }

        [Fact]
        public void CanSkipAndTakeTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                const string documentId = "users/ayende";

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new { Name = "Oren" }, documentId);

                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor(documentId, "Heartrate"))
                    {
                        for (int i = 0; i < 100; i++)
                        {
                            timeSeriesBulkInsert.Append(baseline.AddMinutes(i), new[] { 100d + i }, "watches/fitbit");
                        }
                    }
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor(documentId, "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue, start: 5, pageSize: 20)
                        .ToList();

                    Assert.Equal(20, vals.Count);

                    for (int i = 0; i < vals.Count; i++)
                    {
                        Assert.Equal(baseline.AddMinutes(5 + i), vals[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(105d + i, vals[i].Value);
                    }
                }
            }
        }

        [Fact]
        public void CanStoreAndReadMultipleTimeseriesForDifferentDocuments()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                const string documentId1 = "users/ayende";
                const string documentId2 = "users/grisha";

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new { Name = "Oren" }, documentId1);
                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor(documentId1, "Heartrate"))
                    {
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(1), new[] { 59d }, "watches/fitbit");
                    }

                    bulkInsert.Store(new { Name = "Grisha" }, documentId2);
                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor(documentId2, "Heartrate"))
                    {
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(1), new[] { 59d }, "watches/fitbit");
                    }
                }

                using (var bulkInsert = store.BulkInsert())
                {
                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor(documentId1, "Heartrate"))
                    {
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(2), new[] { 61d }, "watches/fitbit");
                    }

                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor(documentId2, "Heartrate"))
                    {
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(2), new[] { 61d }, "watches/fitbit");
                    }

                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor(documentId1, "Heartrate"))
                    {
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(3), new[] { 62d }, "watches/apple-watch");
                    }

                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor(documentId2, "Heartrate"))
                    {
                        timeSeriesBulkInsert.Append(baseline.AddMinutes(3), new[] { 62d }, "watches/apple-watch");
                    }
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor(documentId1, "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    ValidateValues();

                    vals = session.TimeSeriesFor(documentId2, "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    ValidateValues();

                    void ValidateValues()
                    {
                        Assert.Equal(3, vals.Count);

                        Assert.Equal(new[] { 59d }, vals[0].Values);
                        Assert.Equal("watches/fitbit", vals[0].Tag);
                        Assert.Equal(baseline.AddMinutes(1), vals[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        Assert.Equal(new[] { 61d }, vals[1].Values);
                        Assert.Equal("watches/fitbit", vals[1].Tag);
                        Assert.Equal(baseline.AddMinutes(2), vals[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        Assert.Equal(new[] { 62d }, vals[2].Values);
                        Assert.Equal("watches/apple-watch", vals[2].Tag);
                        Assert.Equal(baseline.AddMinutes(3), vals[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    }
                }
            }
        }

        [Theory]
        [InlineData(128)]
        [InlineData(1024)]
        [InlineData(10 * 1024)]
        [InlineData(100 * 1024)]
        public void CanAppendALotOfTimeSeries(int numberOfTimeSeries)
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday.ToUniversalTime();
                const string documentId = "users/ayende";

                var offset = 0;

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new { Name = "Oren" }, documentId);

                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor(documentId, "Heartrate"))
                    {
                        for (int j = 0; j < numberOfTimeSeries; j++)
                        {
                            timeSeriesBulkInsert.Append(baseline.AddMinutes(offset++), new double[] { offset }, "watches/fitbit");
                        }
                    }
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor(documentId, "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(numberOfTimeSeries, vals.Count);

                    for (int i = 0; i < numberOfTimeSeries; i++)
                    {
                        Assert.Equal(baseline.AddMinutes(i), vals[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(1 + i, vals[i].Values[0]);
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

        [Theory]
        [InlineData(2)]
        [InlineData(4)]
        [InlineData(8)]
        [InlineData(16)]
        [InlineData(32)]
        public void CanAppendALotOfValuesAndTimeSeriesInParallel(int numberOfValues)
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                const string documentId = "users/ayende";
                var numberOfMeasures = 10_000;
                var numberOfTimeSeries = 10;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, documentId);
                    session.SaveChanges();
                }
                var count = new CountdownEvent(numberOfTimeSeries);
                Parallel.For(0, numberOfTimeSeries, async (i) =>
                {
                    var rand = new Random();
                    var offset = 0;
                    await using (var bulkInsert = store.BulkInsert())
                    using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor(documentId, "Heartrate" + "/" + i))
                    {
                        for (int j = 0; j < numberOfMeasures; j++)
                        {
                            var values = new double[numberOfValues];
                            for (int k = 0; k < numberOfValues; k++)
                            {
                                values[k] = (double)rand.Next(-100_000, 100_000) / 1000; // between -100.000 and 100.000
                            }

                            offset += rand.Next(1, 5);
                            await timeSeriesBulkInsert.AppendAsync(baseline.AddSeconds(offset), values);
                        }
                    }

                    count.Signal();
                });

                count.Wait();

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/ayende");
                    var tsNames = session.Advanced.GetTimeSeriesFor(user);
                    Assert.Equal(numberOfTimeSeries, tsNames.Count);
                }

                for (int j = 0; j < numberOfTimeSeries; j++)
                {
                    var tsName = "Heartrate" + "/" + j;
                    using (var session = store.OpenSession())
                    {
                        var vals = session.TimeSeriesFor(documentId, tsName)
                            .Get(DateTime.MinValue, DateTime.MaxValue)
                            .ToList();
                        Assert.Equal(numberOfMeasures, vals.Count);
                    }
                }
            }
        }

        [Fact]
        public void ErrorHandling()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
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
        public async Task CanHaveBulkInsertWithDocumentsAndAttachmentAndCountersAndTimeSeries()
        {
            int count = 100;
            int size = 64 * 1024;

            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday.EnsureUtc();
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
                        bulkInsert.Store(new User { Name = $"Name_{i}" }, id);

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
                        using (var timeSeriesBulkInsert = bulkInsert.TimeSeriesFor(bulk.Key, "HeartRate"))
                        {
                            timeSeriesBulkInsert.Append(baseline.AddMinutes(1), 59d, "watches/fitBit");
                        }
                    }
                }

                foreach (var id in streams.Keys)
                {
                    using (var session = store.OpenSession())
                    {

                        var timeSeriesVal = session.TimeSeriesFor(id, "HeartRate")
                            .Get(DateTime.MinValue, DateTime.MaxValue)
                            .FirstOrDefault();

                        Assert.Equal(new[] { 59d }, timeSeriesVal.Values);
                        Assert.Equal("watches/fitBit", timeSeriesVal.Tag);
                        Assert.Equal(baseline.AddMinutes(1), timeSeriesVal.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        var attachmentsNames = streams.Select(x => new AttachmentRequest(id, x.Key));
                        var attachmentsEnumerator = session.Advanced.Attachments.Get(attachmentsNames);

                        while (attachmentsEnumerator.MoveNext())
                        {
                            Assert.NotNull(attachmentsEnumerator.Current != null);
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
