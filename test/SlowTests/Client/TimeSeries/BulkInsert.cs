using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries
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
                var baseline = DateTime.Today;
                const string documentId = "users/ayende";

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new { Name = "Oren" }, documentId);
                    bulkInsert.AppendTimeSeries(documentId, "Heartrate", baseline.AddMinutes(1), "watches/fitbit", new[] { 59d });
                }

                using (var session = store.OpenSession())
                {
                    var val = session.TimeSeriesFor(documentId)
                        .Get("Heartrate", DateTime.MinValue, DateTime.MaxValue)
                        .Single();

                    Assert.Equal(new[] { 59d }, val.Values);
                    Assert.Equal("watches/fitbit", val.Tag);
                    Assert.Equal(baseline.AddMinutes(1), val.Timestamp);
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
                    bulkInsert.AppendTimeSeries(documentId, "Heartrate", baseline.AddMinutes(1), "watches/fitbit", new[] { 59d });
                    bulkInsert.AppendTimeSeries(documentId, "Heartrate", baseline.AddMinutes(2), "watches/fitbit", new[] { 60d });
                    bulkInsert.AppendTimeSeries(documentId, "Heartrate", baseline.AddMinutes(2), "watches/fitbit", new[] { 61d });
                }

                using (var session = store.OpenSession())
                {
                    var val = session.TimeSeriesFor(documentId)
                        .Get("Heartrate", DateTime.MinValue, DateTime.MaxValue)
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
                var baseline = DateTime.Today;
                const string documentId = "users/ayende";

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new { Name = "Oren" }, documentId);
                    bulkInsert.AppendTimeSeries(documentId, "Heartrate", baseline.AddMinutes(1), "watches/fitbit", new[] { 59d });
                    bulkInsert.AppendTimeSeries(documentId, "Heartrate", baseline.AddMinutes(2), "watches/fitbit", new[] { 69d });
                    bulkInsert.AppendTimeSeries(documentId,"Heartrate", baseline.AddMinutes(3), "watches/fitbit", new[] { 79d });
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, documentId);
                    session.TimeSeriesFor(documentId)
                        .Remove("Heartrate", baseline.AddMinutes(2));

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor(documentId)
                         .Get("Heartrate", DateTime.MinValue, DateTime.MaxValue)
                         .ToList();
                    Assert.Equal(2, vals.Count);
                    Assert.Equal(new[] { 59d }, vals[0].Values);
                    Assert.Equal("watches/fitbit", vals[0].Tag);
                    Assert.Equal(baseline.AddMinutes(1), vals[0].Timestamp);

                    Assert.Equal(new[] { 79d }, vals[1].Values);
                    Assert.Equal("watches/fitbit", vals[1].Tag);
                    Assert.Equal(baseline.AddMinutes(3), vals[1].Timestamp);
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
                    bulkInsert.AppendTimeSeries(documentId, "Heartrate", baseline.AddMinutes(1), "watches/fitbit", new[] { 59d });
                    bulkInsert.AppendTimeSeries(documentId, "Heartrate", baseline.AddMinutes(2), "watches/apple", new[] { 70d });
                }

                using (var session = store.OpenSession())
                {

                    var vals = session.TimeSeriesFor(documentId)
                        .Get("Heartrate", DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(2, vals.Count);
                    Assert.Equal(new[] { 59d }, vals[0].Values);
                    Assert.Equal("watches/fitbit", vals[0].Tag);
                    Assert.Equal(baseline.AddMinutes(1), vals[0].Timestamp);

                    Assert.Equal(new[] { 70d }, vals[1].Values);
                    Assert.Equal("watches/apple", vals[1].Tag);
                    Assert.Equal(baseline.AddMinutes(2), vals[1].Timestamp);
                }
            }
        }

        [Fact]
        public void UsingDifferentNumberOfValues_SmallToLarge()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;
                const string documentId = "users/ayende";

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new { Name = "Oren" }, documentId);

                    bulkInsert.AppendTimeSeries(documentId, "Heartrate", baseline.AddMinutes(1), "watches/fitbit", new[] { 59d });
                    bulkInsert.AppendTimeSeries(documentId, "Heartrate", baseline.AddMinutes(2), "watches/apple", new[] { 70d, 120d, 80d });
                    bulkInsert.AppendTimeSeries(documentId, "Heartrate", baseline.AddMinutes(3), "watches/fitbit", new[] { 69d });
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor(documentId)
                        .Get("Heartrate", DateTime.MinValue, DateTime.MaxValue)
                        .ToList();

                    Assert.Equal(3, vals.Count);
                    Assert.Equal(new[] { 59d }, vals[0].Values);
                    Assert.Equal("watches/fitbit", vals[0].Tag);
                    Assert.Equal(baseline.AddMinutes(1), vals[0].Timestamp);

                    Assert.Equal(new[] { 70d, 120d, 80d }, vals[1].Values);
                    Assert.Equal("watches/apple", vals[1].Tag);
                    Assert.Equal(baseline.AddMinutes(2), vals[1].Timestamp);

                    Assert.Equal(new[] { 69d }, vals[2].Values);
                    Assert.Equal("watches/fitbit", vals[2].Tag);
                    Assert.Equal(baseline.AddMinutes(3), vals[2].Timestamp);
                }
            }
        }

        [Fact]
        public void UsingDifferentNumberOfValues_LargeToSmall()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;
                const string documentId = "users/ayende";

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new { Name = "Oren" }, documentId);

                    bulkInsert.AppendTimeSeries(documentId, "Heartrate", baseline.AddMinutes(1), "watches/apple", new[] { 70d, 120d, 80d });
                    bulkInsert.AppendTimeSeries(documentId, "Heartrate", baseline.AddMinutes(2), "watches/fitbit", new[] { 59d });
                    bulkInsert.AppendTimeSeries(documentId, "Heartrate", baseline.AddMinutes(3), "watches/fitbit", new[] { 69d });
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor(documentId)
                        .Get("Heartrate", DateTime.MinValue, DateTime.MaxValue)
                        .ToList();

                    Assert.Equal(3, vals.Count);

                    Assert.Equal(new[] { 70d, 120d, 80d }, vals[0].Values);
                    Assert.Equal("watches/apple", vals[0].Tag);
                    Assert.Equal(baseline.AddMinutes(1), vals[0].Timestamp);

                    Assert.Equal(new[] { 59d }, vals[1].Values);
                    Assert.Equal("watches/fitbit", vals[1].Tag);
                    Assert.Equal(baseline.AddMinutes(2), vals[1].Timestamp);

                    Assert.Equal(new[] { 69d }, vals[2].Values);
                    Assert.Equal("watches/fitbit", vals[2].Tag);
                    Assert.Equal(baseline.AddMinutes(3), vals[2].Timestamp);
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
                    bulkInsert.AppendTimeSeries(documentId, "Heartrate", baseline.AddMinutes(1), "watches/fitbit", new[] { 59d });
                }

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.AppendTimeSeries(documentId, "Heartrate", baseline.AddMinutes(2), "watches/fitbit", new[] { 61d });
                    bulkInsert.AppendTimeSeries(documentId, "Heartrate", baseline.AddMinutes(3), "watches/apple-watch", new[] { 62d });
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor(documentId)
                        .Get("Heartrate", DateTime.MinValue, DateTime.MaxValue)
                        .ToList();

                    Assert.Equal(3, vals.Count);

                    Assert.Equal(new[] { 59d }, vals[0].Values);
                    Assert.Equal("watches/fitbit", vals[0].Tag);
                    Assert.Equal(baseline.AddMinutes(1), vals[0].Timestamp);

                    Assert.Equal(new[] { 61d }, vals[1].Values);
                    Assert.Equal("watches/fitbit", vals[1].Tag);
                    Assert.Equal(baseline.AddMinutes(2), vals[1].Timestamp);

                    Assert.Equal(new[] { 62d }, vals[2].Values);
                    Assert.Equal("watches/apple-watch", vals[2].Tag);
                    Assert.Equal(baseline.AddMinutes(3), vals[2].Timestamp);
                }
            }
        }

        [Fact]
        public void CanStoreLargeNumberOfValues()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;
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
                        for (int j = 0; j < 1000; j++)
                        {
                            bulkInsert.AppendTimeSeries(documentId, "Heartrate", baseline.AddMinutes(offset++), "watches/fitbit", new double[] { offset });
                        }
                    }
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor(documentId)
                        .Get("Heartrate", DateTime.MinValue, DateTime.MaxValue)
                        .ToList();

                    Assert.Equal(10_000, vals.Count);

                    for (int i = 0; i < 10_000; i++)
                    {
                        Assert.Equal(baseline.AddMinutes(i), vals[i].Timestamp);
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
                var baseline = DateTime.Today;
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
                    for (int j = 0; j < retries; j++)
                    {
                        bulkInsert.AppendTimeSeries(documentId, "Heartrate", baseline.AddMinutes(offset), "watches/fitbit", new double[] { offset });

                        offset += 5;
                    }
                }

                offset = 1;

                using (var bulkInsert = store.BulkInsert())
                {
                    for (int j = 0; j < retries; j++)
                    {
                        bulkInsert.AppendTimeSeries(documentId, "Heartrate", baseline.AddMinutes(offset), "watches/fitbit", new double[] { offset });
                        offset += 5;
                    }
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor(documentId)
                        .Get("Heartrate", DateTime.MinValue, DateTime.MaxValue)
                        .ToList();

                    Assert.Equal(2 * retries, vals.Count);

                    offset = 0;
                    for (int i = 0; i < retries; i++)
                    {
                        Assert.Equal(baseline.AddMinutes(offset), vals[i].Timestamp);
                        Assert.Equal(offset, vals[i].Values[0]);

                        offset++;
                        i++;

                        Assert.Equal(baseline.AddMinutes(offset), vals[i].Timestamp);
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
                var baseline = DateTime.Today;
                const string documentId = "users/ayende";

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new { Name = "Oren" }, documentId);
                    bulkInsert.AppendTimeSeries(documentId, "Heartrate", baseline, "watches/fitbit", new[] { 0d });
                }

                using (var session = store.OpenSession())
                {
                    var timeSeriesFor = session.TimeSeriesFor(documentId);

                    for (double i = 1; i < 10; i++)
                    {
                        timeSeriesFor
                            .Append("Heartrate", baseline.AddMinutes(i), "watches/fitbit", new[] { i });
                    }

                    session.SaveChanges();
                }


                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        var vals = session.TimeSeriesFor(documentId)
                            .Get("Heartrate", baseline.AddMinutes(i), DateTime.MaxValue)
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
                        var vals = session.TimeSeriesFor(documentId)
                            .Get("Heartrate", baseline, maxTimeStamp.AddMinutes(-i))
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
                var baseline = DateTime.Today;
                const string documentId = "users/ayende";

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new { Name = "Oren" }, documentId);
                    bulkInsert.AppendTimeSeries(documentId, "Heartrate", baseline, "watches/fitbit", new[] { 58d });
                    bulkInsert.AppendTimeSeries(documentId, "Heartrate", baseline.AddMinutes(10), "watches/fitbit", new[] { 60d });
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor(documentId)
                        .Get("Heartrate", baseline.AddMinutes(-10), baseline.AddMinutes(-5))
                        .ToList();

                    Assert.Equal(0, vals.Count);

                    vals = session.TimeSeriesFor(documentId)
                        .Get("Heartrate", baseline.AddMinutes(5), baseline.AddMinutes(9))
                        .ToList();

                    Assert.Equal(0, vals.Count);
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
                    bulkInsert.AppendTimeSeries(documentId1, "Nasdaq2", DateTime.Now, "web", new[] { 7547.31 });
                }

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.AppendTimeSeries(documentId1, "Heartrate2", DateTime.Now, "web", new[] { 7547.31 });
                }

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new User(), documentId2);
                    bulkInsert.AppendTimeSeries(documentId2, "Nasdaq", DateTime.Now, "web", new[] { 7547.31 });
                }

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.AppendTimeSeries(documentId2, "Heartrate", DateTime.Today.AddMinutes(1), "fitbit", new[] { 58d });
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

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende")
                        .Append("heartrate", DateTime.Today.AddMinutes(1), "fitbit", new[] { 58d }); // putting ts name as lower cased

                    session.SaveChanges();
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
                var baseline = DateTime.Today;
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
                        for (int j = 0; j < 1000; j++)
                        {
                            bulkInsert.AppendTimeSeries(documentId, "Heartrate", baseline.AddMinutes(offset++), "watches/fitbit", new double[] { offset });
                        }
                    }
                }

                offset = 0;

                for (int i = 0; i < 100; i++)
                {
                    using (var bulkInsert = store.BulkInsert())
                    {
                        for (int j = 0; j < 1000; j++)
                        {
                            bulkInsert.AppendTimeSeries(documentId, "Pulse", baseline.AddMinutes(offset++), "watches/fitbit", new double[] { offset });
                        }
                    }
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor(documentId)
                        .Get("Heartrate", DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(100_000, vals.Count);

                    for (int i = 0; i < 100_000; i++)
                    {
                        Assert.Equal(baseline.AddMinutes(i), vals[i].Timestamp);
                        Assert.Equal(1 + i, vals[i].Values[0]);
                    }
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor(documentId)
                        .Get("Pulse", DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(100_000, vals.Count);

                    for (int i = 0; i < 100_000; i++)
                    {
                        Assert.Equal(baseline.AddMinutes(i), vals[i].Timestamp);
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

                        bulkInsert.AppendTimeSeries(id, "Heartrate", baseline.AddMinutes(1), "watches/fitbit", new[] { 59d });
                    }
                }

                var cvs = new List<string>();

                using (var session = store.OpenSession())
                {
                    for (int i = 2; i < 5; i++)
                    {
                        var id = $"users/{i}";
                        var u = session.Load<User>(id);
                        var cv = session.Advanced.GetChangeVectorFor(u);
                        cvs.Add(cv);

                        session.TimeSeriesFor(id)
                            .Append("Nasdaq", baseline.AddMinutes(1), "web", new[] { 4012.5d });

                    }

                    session.SaveChanges();
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
                var baseline = DateTime.Today;
                const string documentId = "users/ayende";
                IEnumerable<double> values = new List<double>
                {
                    59d
                };

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new { Name = "Oren" }, "users/ayende");
                    bulkInsert.AppendTimeSeries(documentId, "Heartrate", baseline.AddMinutes(1), "watches/fitbit", values);
                }

                using (var session = store.OpenSession())
                {
                    var val = session.TimeSeriesFor(documentId)
                        .Get("Heartrate", DateTime.MinValue, DateTime.MaxValue)
                        .Single();
                    Assert.Equal(new[] { 59d }, val.Values);
                    Assert.Equal("watches/fitbit", val.Tag);
                    Assert.Equal(baseline.AddMinutes(1), val.Timestamp);
                }
            }
        }

        [Fact]
        public void ShouldDeleteTimeSeriesUponDocumentDeletion()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;
                const string documentId = "users/ayende";

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new User { Name = "Oren" }, documentId);

                    bulkInsert.AppendTimeSeries(documentId, "Heartrate", baseline.AddMinutes(1), "watches/fitbit", new []{ 59d });
                    bulkInsert.AppendTimeSeries(documentId, "Heartrate", baseline.AddMinutes(2), "watches/fitbit", new[] { 59d });
                    bulkInsert.AppendTimeSeries(documentId, "Heartrate2", baseline.AddMinutes(1), "watches/apple", new[] { 59d });
                }

                using (var session = store.OpenSession())
                {
                    session.Delete(documentId);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor(documentId).Get("Heartrate", DateTime.MinValue, DateTime.MaxValue);
                    Assert.Equal(0, vals.Count());

                    vals = session.TimeSeriesFor(documentId).Get("Heartrate2", DateTime.MinValue, DateTime.MaxValue);
                    Assert.Equal(0, vals.Count());
                }
            }
        }

        [Fact]
        public void CanSkipAndTakeTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;
                const string documentId = "users/ayende";

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new { Name = "Oren" }, documentId);

                    for (int i = 0; i < 100; i++)
                    {
                        bulkInsert.AppendTimeSeries(documentId, "Heartrate", baseline.AddMinutes(i), "watches/fitbit", new[] { 100d + i });
                    }
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor(documentId)
                        .Get("Heartrate", DateTime.MinValue, DateTime.MaxValue, start: 5, pageSize :20)
                        .ToList();

                    Assert.Equal(20, vals.Count);

                    for (int i = 0; i < vals.Count; i++)
                    {
                        Assert.Equal(baseline.AddMinutes(5 + i), vals[i].Timestamp);
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
                var baseline = DateTime.Today;
                const string documentId1 = "users/ayende";
                const string documentId2 = "users/grisha";

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new { Name = "Oren" }, documentId1);
                    bulkInsert.AppendTimeSeries(documentId1, "Heartrate", baseline.AddMinutes(1), "watches/fitbit", new[] { 59d });
                    bulkInsert.Store(new { Name = "Grisha" }, documentId2);
                    bulkInsert.AppendTimeSeries(documentId2, "Heartrate", baseline.AddMinutes(1), "watches/fitbit", new[] { 59d });
                }

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.AppendTimeSeries(documentId1, "Heartrate", baseline.AddMinutes(2), "watches/fitbit", new[] { 61d });
                    bulkInsert.AppendTimeSeries(documentId2, "Heartrate", baseline.AddMinutes(2), "watches/fitbit", new[] { 61d });
                    bulkInsert.AppendTimeSeries(documentId1, "Heartrate", baseline.AddMinutes(3), "watches/apple-watch", new[] { 62d });
                    bulkInsert.AppendTimeSeries(documentId2, "Heartrate", baseline.AddMinutes(3), "watches/apple-watch", new[] { 62d });
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor(documentId1)
                        .Get("Heartrate", DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    ValidateValues();

                    vals = session.TimeSeriesFor(documentId2)
                        .Get("Heartrate", DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    ValidateValues();

                    void ValidateValues()
                    {
                        Assert.Equal(3, vals.Count);

                        Assert.Equal(new[] { 59d }, vals[0].Values);
                        Assert.Equal("watches/fitbit", vals[0].Tag);
                        Assert.Equal(baseline.AddMinutes(1), vals[0].Timestamp);

                        Assert.Equal(new[] { 61d }, vals[1].Values);
                        Assert.Equal("watches/fitbit", vals[1].Tag);
                        Assert.Equal(baseline.AddMinutes(2), vals[1].Timestamp);

                        Assert.Equal(new[] { 62d }, vals[2].Values);
                        Assert.Equal("watches/apple-watch", vals[2].Tag);
                        Assert.Equal(baseline.AddMinutes(3), vals[2].Timestamp);
                    }
                }
            }
        }
    }
}
