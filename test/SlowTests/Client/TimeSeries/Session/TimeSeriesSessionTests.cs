using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Session
{
    public class TimeSeriesSessionTests : RavenTestBase
    {
        public TimeSeriesSessionTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanCreateSimpleTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(1), "watches/fitbit", new[] { 59d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var val = session.TimeSeriesFor("users/ayende")
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

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(1), "watches/fitbit", new[] { 59d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(2), "watches/fitbit", new[] { 60d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(2), "watches/fitbit", new[] { 61d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var val = session.TimeSeriesFor("users/ayende")
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

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(1), "watches/fitbit", new[] { 59d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(2), "watches/fitbit", new[] { 69d });
                    session.TimeSeriesFor("users/ayende")
                     .Append("Heartrate", baseline.AddMinutes(3), "watches/fitbit", new[] { 79d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende")
                        .Remove("Heartrate", baseline.AddMinutes(2));

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende")
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

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(1), "watches/fitbit", new[] { 59d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(2), "watches/apple", new[] { 70d });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {

                    var vals = session.TimeSeriesFor("users/ayende")
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

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(1), "watches/fitbit", new[] { 59d });

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(2), "watches/apple", new[] { 70d, 120d, 80d });

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(3), "watches/fitbit", new[] { 69d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende")
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

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(1), "watches/apple", new[] { 70d, 120d, 80d });

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(2), "watches/fitbit", new[] { 59d });


                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(3), "watches/fitbit", new[] { 69d });

                    session.SaveChanges();
                }


                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende")
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

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(1), "watches/fitbit", new[] { 59d });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(2), "watches/fitbit", new[] { 61d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(3), "watches/apple-watch", new[] { 62d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende")
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

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                var offset = 0;

                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenSession())
                    {

                        for (int j = 0; j < 1000; j++)
                        {
                            session.TimeSeriesFor("users/ayende")
                                .Append("Heartrate", baseline.AddMinutes(offset++), "watches/fitbit", new double[] { offset });
                        }

                        session.SaveChanges();
                    }
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende")
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

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                const int retries = 1000;

                var offset = 0;

                using (var session = store.OpenSession())
                {

                    for (int j = 0; j < retries; j++)
                    {
                        session.TimeSeriesFor("users/ayende")
                            .Append("Heartrate", baseline.AddMinutes(offset), "watches/fitbit", new double[] { offset });

                        offset += 5;
                    }

                    session.SaveChanges();
                }

                offset = 1;

                using (var session = store.OpenSession())
                {

                    for (int j = 0; j < retries; j++)
                    {
                        session.TimeSeriesFor("users/ayende")
                            .Append("Heartrate", baseline.AddMinutes(offset), "watches/fitbit", new double[] { offset });
                        offset += 5;
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende")
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

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline, "watches/fitbit", new[] { 0d });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var timeSeriesFor = session.TimeSeriesFor("users/ayende");

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
                        var vals = session.TimeSeriesFor("users/ayende")
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
                        var vals = session.TimeSeriesFor("users/ayende")
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

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline, "watches/fitbit", new[] { 58d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(10), "watches/fitbit", new[] { 60d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende")
                        .Get("Heartrate", baseline.AddMinutes(-10), baseline.AddMinutes(-5))
                        .ToList();

                    Assert.Equal(0, vals.Count);

                    vals = session.TimeSeriesFor("users/ayende")
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
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/karmel");
                    session.TimeSeriesFor("users/karmel")
                        .Append("Nasdaq2", DateTime.Now, "web", new[] { 7547.31 });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/karmel")
                        .Append("Heartrate2", DateTime.Now, "web", new[] { 7547.31 });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");
                    session.TimeSeriesFor("users/ayende")
                        .Append("Nasdaq", DateTime.Now, "web", new[] { 7547.31 });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", DateTime.Today.AddMinutes(1), "fitbit", new[] { 58d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/ayende");
                    var tsNames = session.Advanced.GetTimeSeriesFor(user);
                    Assert.Equal(2, tsNames.Count);

                    // should be sorted
                    Assert.Equal("Heartrate", tsNames[0]);
                    Assert.Equal("Nasdaq", tsNames[1]);
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/karmel");
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

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                var offset = 0;

                for (int i = 0; i < 100; i++)
                {
                    using (var session = store.OpenSession())
                    {

                        for (int j = 0; j < 1000; j++)
                        {
                            session.TimeSeriesFor("users/ayende")
                                .Append("Heartrate", baseline.AddMinutes(offset++), "watches/fitbit", new double[] { offset });
                        }

                        session.SaveChanges();
                    }
                }


                offset = 0;

                for (int i = 0; i < 100; i++)
                {
                    using (var session = store.OpenSession())
                    {

                        for (int j = 0; j < 1000; j++)
                        {
                            session.TimeSeriesFor("users/ayende")
                                .Append("Pulse", baseline.AddMinutes(offset++), "watches/fitbit", new double[] { offset });
                        }

                        session.SaveChanges();
                    }
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende")
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
                    var vals = session.TimeSeriesFor("users/ayende")
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

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 5; i++)
                    {
                        var id = $"users/{i}";
                        session.Store(new User
                        {
                            Name = "Oren"
                        }, id);

                        session.TimeSeriesFor(id)
                            .Append("Heartrate", baseline.AddMinutes(1), "watches/fitbit", new[] { 59d });
                    }

                    session.SaveChanges();
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

                IEnumerable<double> values = new List<double>
                {
                    59d
                };

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(1), "watches/fitbit", values);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var val = session.TimeSeriesFor("users/ayende")
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
                var id = "users/ayende";

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, id);

                    var timeSeriesFor = session.TimeSeriesFor(id);

                    timeSeriesFor.Append("Heartrate", baseline.AddMinutes(1), "watches/fitbit", new []{ 59d });
                    timeSeriesFor.Append("Heartrate", baseline.AddMinutes(2), "watches/fitbit", new[] { 59d });
                    timeSeriesFor.Append("Heartrate2", baseline.AddMinutes(1), "watches/apple", new[] { 59d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Delete(id);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor(id).Get("Heartrate", DateTime.MinValue, DateTime.MaxValue);
                    Assert.Equal(0, vals.Count());

                    vals = session.TimeSeriesFor(id).Get("Heartrate2", DateTime.MinValue, DateTime.MaxValue);
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

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");

                    for (int i = 0; i < 100; i++)
                    {
                        session.TimeSeriesFor("users/ayende")
                            .Append("Heartrate", baseline.AddMinutes(i), "watches/fitbit", new[] { 100d + i });
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende")
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

    }
}
