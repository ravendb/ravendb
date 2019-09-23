using System;
using System.Linq;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Client.TimeSeries.Session
{
    public class TimeSeriesIncludes : RavenTestBase
    {
        [Fact]
        public void SessionLoadWithIncludeTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR" }, "companies/1-A");
                    session.Store(new Order { Company = "companies/1-A" }, "orders/1-A");
                    session.TimeSeriesFor("orders/1-A").Append("Heartrate", baseline, "watches/apple", new []{ 67d });
                    session.TimeSeriesFor("orders/1-A").Append("Heartrate", baseline.AddMinutes(5), "watches/apple", new[] { 64d });
                    session.TimeSeriesFor("orders/1-A").Append("Heartrate", baseline.AddMinutes(10), "watches/fitbit", new[] { 65d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>(
                        "orders/1-A",
                        i => i.IncludeDocuments("Company")
                            .IncludeTimeSeries("Heartrate", DateTime.MinValue, DateTime.MaxValue));

                    var company = session.Load<Company>(order.Company);
                    Assert.Equal("HR", company.Name);

                    // should not go to server
                    var values = session.TimeSeriesFor(order)
                        .Get("Heartrate", DateTime.MinValue, DateTime.MaxValue)
                        .ToList(); 
                    
                    Assert.Equal(3, values.Count);

                    Assert.Equal(1, values[0].Values.Length);
                    Assert.Equal(67d, values[0].Values[0]);
                    Assert.Equal("watches/apple", values[0].Tag);
                    Assert.Equal(baseline, values[0].Timestamp);

                    Assert.Equal(1, values[1].Values.Length);
                    Assert.Equal(64d, values[1].Values[0]);
                    Assert.Equal("watches/apple", values[1].Tag);
                    Assert.Equal(baseline.AddMinutes(5), values[1].Timestamp);

                    Assert.Equal(1, values[2].Values.Length);
                    Assert.Equal(65d, values[2].Values[0]);
                    Assert.Equal("watches/fitbit", values[2].Tag);
                    Assert.Equal(baseline.AddMinutes(10), values[2].Timestamp);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
            }
        }

        [Fact]
        public void IncludeTimeSeriesAndMergeWithExistingRangesInCache()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User{ Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor("users/ayende");
                    for (int i = 0; i < 360; i++)
                    {
                        tsf.Append("Heartrate", baseline.AddSeconds(i * 10), "watches/fitbit", new[] { 6d });
                    }

                    session.SaveChanges();

                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende")
                        .Get("Heartrate", baseline.AddMinutes(2), baseline.AddMinutes(10))
                        .ToList();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    Assert.Equal(49, vals.Count);
                    Assert.Equal(baseline.AddMinutes(2), vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(10), vals[48].Timestamp);

                    var user = session.Load<User>(
                        "users/ayende",
                        i => i.IncludeTimeSeries("Heartrate", baseline.AddMinutes(40), baseline.AddMinutes(50)));

                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    // should not go to server

                    vals = session.TimeSeriesFor("users/ayende")
                        .Get("Heartrate", baseline.AddMinutes(40), baseline.AddMinutes(50))
                        .ToList();

                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    Assert.Equal(61, vals.Count);
                    Assert.Equal(baseline.AddMinutes(40), vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(50), vals[60].Timestamp);

                    Assert.True(((InMemoryDocumentSessionOperations)session).TimeSeriesByDocId.TryGetValue("users/ayende", out var cache));
                    Assert.True(cache.TryGetValue("Heartrate", out var ranges));
                    Assert.Equal(2, ranges.Count);

                    Assert.Equal(baseline.AddMinutes(2), ranges[0].From);
                    Assert.Equal(baseline.AddMinutes(10), ranges[0].To);
                    Assert.Equal(baseline.AddMinutes(40), ranges[1].From);
                    Assert.Equal(baseline.AddMinutes(50), ranges[1].To);

                    session.Advanced.Evict(user);

                    // should go to server to get [0, 2] and merge it into existing [2, 10] 
                    user = session.Load<User>(
                        "users/ayende",
                        i => i.IncludeTimeSeries("Heartrate", baseline, baseline.AddMinutes(2)));

                    Assert.Equal(3, session.Advanced.NumberOfRequests);

                    // should not go to server

                    vals = session.TimeSeriesFor("users/ayende")
                        .Get("Heartrate", baseline, baseline.AddMinutes(2))
                        .ToList();

                    Assert.Equal(3, session.Advanced.NumberOfRequests);

                    Assert.Equal(13, vals.Count);
                    Assert.Equal(baseline, vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(2), vals[12].Timestamp);

                    Assert.Equal(2, ranges.Count);
                    Assert.Equal(baseline.AddMinutes(0), ranges[0].From);
                    Assert.Equal(baseline.AddMinutes(10), ranges[0].To);
                    Assert.Equal(baseline.AddMinutes(40), ranges[1].From);
                    Assert.Equal(baseline.AddMinutes(50), ranges[1].To);

                    // should go to server to get [10, 16] and merge it into existing [0, 10] 

                    session.Advanced.Evict(user);
                    user = session.Load<User>(
                        "users/ayende",
                        i => i.IncludeTimeSeries("Heartrate", baseline.AddMinutes(10), baseline.AddMinutes(16)));

                    Assert.Equal(4, session.Advanced.NumberOfRequests);

                    // should not go to server

                    vals = session.TimeSeriesFor("users/ayende")
                        .Get("Heartrate", baseline.AddMinutes(10), baseline.AddMinutes(16))
                        .ToList();

                    Assert.Equal(4, session.Advanced.NumberOfRequests);

                    Assert.Equal(37, vals.Count);
                    Assert.Equal(baseline.AddMinutes(10), vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(16), vals[36].Timestamp);

                    Assert.Equal(2, ranges.Count);
                    Assert.Equal(baseline.AddMinutes(0), ranges[0].From);
                    Assert.Equal(baseline.AddMinutes(16), ranges[0].To);
                    Assert.Equal(baseline.AddMinutes(40), ranges[1].From);
                    Assert.Equal(baseline.AddMinutes(50), ranges[1].To);

                    // should go to server to get range [17, 19]
                    // and add it to cache in between [10, 16] and [40, 50]

                    session.Advanced.Evict(user);
                    user = session.Load<User>(
                        "users/ayende",
                        i => i.IncludeTimeSeries("Heartrate", baseline.AddMinutes(17), baseline.AddMinutes(19)));

                    Assert.Equal(5, session.Advanced.NumberOfRequests);

                    // should not go to server

                    vals = session.TimeSeriesFor("users/ayende")
                        .Get("Heartrate", baseline.AddMinutes(17), baseline.AddMinutes(19))
                        .ToList();

                    Assert.Equal(5, session.Advanced.NumberOfRequests);

                    Assert.Equal(13, vals.Count);
                    Assert.Equal(baseline.AddMinutes(17), vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(19), vals[12].Timestamp);

                    Assert.Equal(3, ranges.Count);
                    Assert.Equal(baseline.AddMinutes(0), ranges[0].From);
                    Assert.Equal(baseline.AddMinutes(16), ranges[0].To);
                    Assert.Equal(baseline.AddMinutes(17), ranges[1].From);
                    Assert.Equal(baseline.AddMinutes(19), ranges[1].To);
                    Assert.Equal(baseline.AddMinutes(40), ranges[2].From);
                    Assert.Equal(baseline.AddMinutes(50), ranges[2].To);

                    // should go to server to get range [19, 40]
                    // and merge the result with existing ranges [17, 19] and [40, 50] 
                    // into single range [17, 50]

                    session.Advanced.Evict(user);
                    user = session.Load<User>(
                        "users/ayende",
                        i => i.IncludeTimeSeries("Heartrate", baseline.AddMinutes(18), baseline.AddMinutes(48)));

                    Assert.Equal(6, session.Advanced.NumberOfRequests);

                    // should not go to server

                    vals = session.TimeSeriesFor("users/ayende")
                        .Get("Heartrate", baseline.AddMinutes(18), baseline.AddMinutes(48))
                        .ToList();

                    Assert.Equal(6, session.Advanced.NumberOfRequests);

                    Assert.Equal(181, vals.Count);
                    Assert.Equal(baseline.AddMinutes(18), vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(48), vals[180].Timestamp);

                    Assert.Equal(2, ranges.Count);
                    Assert.Equal(baseline.AddMinutes(0), ranges[0].From);
                    Assert.Equal(baseline.AddMinutes(16), ranges[0].To);
                    Assert.Equal(baseline.AddMinutes(17), ranges[1].From);
                    Assert.Equal(baseline.AddMinutes(50), ranges[1].To);

                    // should go to server to get range [12, 22]
                    // and merge the result with existing ranges [0, 16] and [17, 50] 
                    // into single range [0, 50]


                    session.Advanced.Evict(user);
                    user = session.Load<User>(
                        "users/ayende",
                        i => i.IncludeTimeSeries("Heartrate", baseline.AddMinutes(12), baseline.AddMinutes(22)));

                    Assert.Equal(7, session.Advanced.NumberOfRequests);

                    // should not go to server

                    vals = session.TimeSeriesFor("users/ayende")
                        .Get("Heartrate", baseline.AddMinutes(12), baseline.AddMinutes(22))
                        .ToList();

                    Assert.Equal(7, session.Advanced.NumberOfRequests);

                    Assert.Equal(61, vals.Count);
                    Assert.Equal(baseline.AddMinutes(12), vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(22), vals[60].Timestamp);

                    Assert.Equal(1, ranges.Count);
                    Assert.Equal(baseline.AddMinutes(0), ranges[0].From);
                    Assert.Equal(baseline.AddMinutes(50), ranges[0].To);

                }
            }
        }


        [Fact]
        public void IncludeTimeSeriesAndUpdateExistingRangeInCache()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor("users/ayende");
                    for (int i = 0; i < 360; i++)
                    {
                        tsf.Append("Heartrate", baseline.AddSeconds(i * 10), "watches/fitbit", new[] { 6d });
                    }

                    session.SaveChanges();

                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende")
                        .Get("Heartrate", baseline.AddMinutes(2), baseline.AddMinutes(10))
                        .ToList();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    Assert.Equal(49, vals.Count);
                    Assert.Equal(baseline.AddMinutes(2), vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(10), vals[48].Timestamp);

                    session.TimeSeriesFor("users/ayende").Append("Heartrate", baseline.AddMinutes(3).AddSeconds(3), "watches/fitbit", new[] { 6d });
                    session.SaveChanges();

                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    var user = session.Load<User>(
                        "users/ayende",
                        i => i.IncludeTimeSeries("Heartrate", baseline.AddMinutes(3), baseline.AddMinutes(5)));

                    Assert.Equal(3, session.Advanced.NumberOfRequests);

                    // should not go to server

                    vals = session.TimeSeriesFor("users/ayende")
                        .Get("Heartrate", baseline.AddMinutes(3), baseline.AddMinutes(5))
                        .ToList();

                    Assert.Equal(3, session.Advanced.NumberOfRequests);

                    Assert.Equal(14, vals.Count);
                    Assert.Equal(baseline.AddMinutes(3), vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(3).AddSeconds(3), vals[1].Timestamp);
                    Assert.Equal(baseline.AddMinutes(5), vals[13].Timestamp);

                }
            }
        }


        [Fact]
        public void IncludeMultipleTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor("users/ayende");

                    for (int i = 0; i < 360; i++)
                    {
                        tsf.Append("Heartrate", baseline.AddSeconds(i * 10), "watches/fitbit", new[] { 6d });
                        tsf.Append("BloodPressure", baseline.AddSeconds(i * 10), "watches/fitbit", new[] { 66d });
                        tsf.Append("Nasdaq", baseline.AddSeconds(i * 10), "nasdaq.com", new[] { 8097.23 });

                    }

                    session.SaveChanges();

                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>(
                        "users/ayende",
                        i => i.IncludeTimeSeries("Heartrate", baseline.AddMinutes(3), baseline.AddMinutes(5))
                            .IncludeTimeSeries("BloodPressure", baseline.AddMinutes(40), baseline.AddMinutes(45))
                            .IncludeTimeSeries("Nasdaq", baseline.AddMinutes(15), baseline.AddMinutes(25)));

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    Assert.Equal("Oren", user.Name);

                    // should not go to server

                    var vals = session.TimeSeriesFor("users/ayende")
                        .Get("Heartrate", baseline.AddMinutes(3), baseline.AddMinutes(5))
                        .ToList();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    Assert.Equal(13, vals.Count);
                    Assert.Equal(baseline.AddMinutes(3), vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(5), vals[12].Timestamp);

                    // should not go to server

                    vals = session.TimeSeriesFor("users/ayende")
                        .Get("BloodPressure", baseline.AddMinutes(42), baseline.AddMinutes(43))
                        .ToList();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    Assert.Equal(7, vals.Count);
                    Assert.Equal(baseline.AddMinutes(42), vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(43), vals[6].Timestamp);


                    // should not go to server

                    vals = session.TimeSeriesFor("users/ayende")
                        .Get("BloodPressure", baseline.AddMinutes(40), baseline.AddMinutes(45))
                        .ToList();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    Assert.Equal(31, vals.Count);
                    Assert.Equal(baseline.AddMinutes(40), vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(45), vals[30].Timestamp);

                    // should not go to server

                    vals = session.TimeSeriesFor("users/ayende")
                        .Get("Nasdaq", baseline.AddMinutes(15), baseline.AddMinutes(25))
                        .ToList();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    Assert.Equal(61, vals.Count);
                    Assert.Equal(baseline.AddMinutes(15), vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(25), vals[60].Timestamp);

                }
            }
        }

        [Fact]
        public void ShouldCacheEmptyTimeSeriesRanges()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor("users/ayende");

                    for (int i = 0; i < 360; i++)
                    {
                        tsf.Append("Heartrate", baseline.AddSeconds(i * 10), "watches/fitbit", new[] { 6d });
                    }

                    session.SaveChanges();

                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>(
                        "users/ayende",
                        i => i.IncludeTimeSeries("Heartrate", baseline.AddMinutes(-30), baseline.AddMinutes(-10)));

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    Assert.Equal("Oren", user.Name);

                    // should not go to server

                    var vals = session.TimeSeriesFor("users/ayende")
                        .Get("Heartrate", baseline.AddMinutes(-30), baseline.AddMinutes(-10))
                        .ToList();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    Assert.Equal(0, vals.Count);
                    
                    Assert.True(((InMemoryDocumentSessionOperations)session).TimeSeriesByDocId.TryGetValue("users/ayende", out var cache));
                    Assert.True(cache.TryGetValue("Heartrate", out var ranges));
                    Assert.Equal(1, ranges.Count);
                    Assert.Empty(ranges[0].Values);
                    Assert.Equal(baseline.AddMinutes(-30), ranges[0].From);
                    Assert.Equal(baseline.AddMinutes(-10), ranges[0].To);

                    // should not go to server

                    vals = session.TimeSeriesFor("users/ayende")
                        .Get("Heartrate", baseline.AddMinutes(-25), baseline.AddMinutes(-15))
                        .ToList();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    Assert.Equal(0, vals.Count);

                    session.Advanced.Evict(user);

                    user = session.Load<User>(
                        "users/ayende",
                        i => i.IncludeTimeSeries("BloodPressure", baseline.AddMinutes(10), baseline.AddMinutes(30)));

                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    // should not go to server

                    vals = session.TimeSeriesFor("users/ayende")
                        .Get("BloodPressure", baseline.AddMinutes(10), baseline.AddMinutes(30))
                        .ToList();

                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    Assert.Equal(0, vals.Count);

                    Assert.True(cache.TryGetValue("BloodPressure", out ranges));
                    Assert.Equal(1, ranges.Count);
                    Assert.Empty(ranges[0].Values);
                    Assert.Equal(baseline.AddMinutes(10), ranges[0].From);
                    Assert.Equal(baseline.AddMinutes(30), ranges[0].To);


                }
            }
        }

    }
}
