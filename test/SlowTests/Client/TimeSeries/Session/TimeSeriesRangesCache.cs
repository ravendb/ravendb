using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Session
{
    public class TimeSeriesRangesCache : RavenTestBase
    {
        public TimeSeriesRangesCache(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldGetTimeSeriesValueFromCache()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddMinutes(1), new[] { 59d }, "watches/fitbit");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var val = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .Single();

                    Assert.Equal(new[] { 59d }, val.Values);
                    Assert.Equal("watches/fitbit", val.Tag);
                    Assert.Equal(baseline.AddMinutes(1), val.Timestamp);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // should load from cache
                    val = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .Single();

                    Assert.Equal(new[] { 59d }, val.Values);
                    Assert.Equal("watches/fitbit", val.Tag);
                    Assert.Equal(baseline.AddMinutes(1), val.Timestamp);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void ShouldGetPartialRangeFromCache()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddMinutes(1), new[] { 59d }, "watches/fitbit");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var val = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .Single();
                     
                    Assert.Equal(new[] { 59d }, val.Values);
                    Assert.Equal("watches/fitbit", val.Tag);
                    Assert.Equal(baseline.AddMinutes(1), val.Timestamp);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // should load from cache
                    val = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(baseline, baseline.AddDays(1))
                        .Single();

                    Assert.Equal(new[] { 59d }, val.Values);
                    Assert.Equal("watches/fitbit", val.Tag);
                    Assert.Equal(baseline.AddMinutes(1), val.Timestamp);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    var inMemoryDocumentSession = (InMemoryDocumentSessionOperations)session;

                    Assert.True(inMemoryDocumentSession.TimeSeriesByDocId.TryGetValue("users/ayende", out var cache));
                    Assert.True(cache.TryGetValue("Heartrate", out var ranges));
                    Assert.Equal(1, ranges.Count);
                }
            }
        }

        [Fact]
        public void ShouldMergeTimeSeriesRangesInCache()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    for (int i = 0; i < 360; i++)
                    {
                        tsf.Append(baseline.AddSeconds(i * 10), new[] {6d}, "watches/fitbit");
                    }

                    session.SaveChanges();

                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(baseline.AddMinutes(2), baseline.AddMinutes(10))
                        .ToList();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    Assert.Equal(49, vals.Count); 
                    Assert.Equal(baseline.AddMinutes(2), vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(10), vals[48].Timestamp);

                    // should load partial range from cache
                    vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(baseline.AddMinutes(5), baseline.AddMinutes(7))
                        .ToList();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    Assert.Equal(13, vals.Count);
                    Assert.Equal(baseline.AddMinutes(5), vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(7), vals[12].Timestamp);

                    // should go to server
                    vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(baseline.AddMinutes(40), baseline.AddMinutes(50))
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

                    // should go to server to get [0, 2] and merge it into existing [2, 10] 
                    vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(baseline, baseline.AddMinutes(5))
                        .ToList();

                    Assert.Equal(3, session.Advanced.NumberOfRequests);

                    Assert.Equal(31, vals.Count);
                    Assert.Equal(baseline, vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(5), vals[30].Timestamp);

                    Assert.Equal(2, ranges.Count);
                    Assert.Equal(baseline.AddMinutes(0), ranges[0].From);
                    Assert.Equal(baseline.AddMinutes(10), ranges[0].To);
                    Assert.Equal(baseline.AddMinutes(40), ranges[1].From);
                    Assert.Equal(baseline.AddMinutes(50), ranges[1].To);

                    // should go to server to get [10, 16] and merge it into existing [0, 10] 
                    vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(baseline.AddMinutes(8), baseline.AddMinutes(16))
                        .ToList();

                    Assert.Equal(4, session.Advanced.NumberOfRequests);

                    Assert.Equal(49, vals.Count);
                    Assert.Equal(baseline.AddMinutes(8), vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(16), vals[48].Timestamp);

                    Assert.Equal(2, ranges.Count);
                    Assert.Equal(baseline.AddMinutes(0), ranges[0].From);
                    Assert.Equal(baseline.AddMinutes(16), ranges[0].To);
                    Assert.Equal(baseline.AddMinutes(40), ranges[1].From);
                    Assert.Equal(baseline.AddMinutes(50), ranges[1].To);

                    // should go to server to get range [17, 19]
                    // and add it to cache in between [10, 16] and [40, 50]

                    vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(baseline.AddMinutes(17), baseline.AddMinutes(19))
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

                    vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(baseline.AddMinutes(18), baseline.AddMinutes(48))
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

                    // should go to server to get range [16, 17]
                    // and merge the result with existing ranges [0, 16] and [17, 50] 
                    // into single range [0, 50]

                    vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(baseline.AddMinutes(12), baseline.AddMinutes(22))
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
        public void ShouldMergeTimeSeriesRangesInCache2()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    for (int i = 0; i < 360; i++)
                    {
                        tsf.Append(baseline.AddSeconds(i * 10), new[] { 60d }, "watches/fitbit");
                    }

                    tsf = session.TimeSeriesFor("users/ayende", "Heartrate2");

                    tsf.Append(baseline.AddHours(1), new[] { 70d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(90), new[] { 75d }, "watches/fitbit");

                    session.SaveChanges();

                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(baseline.AddMinutes(2), baseline.AddMinutes(10))
                        .ToList();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    Assert.Equal(49, vals.Count);
                    Assert.Equal(baseline.AddMinutes(2), vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(10), vals[48].Timestamp);

                    // should go the server
                    vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(baseline.AddMinutes(22), baseline.AddMinutes(32))
                        .ToList();

                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    Assert.Equal(61, vals.Count);
                    Assert.Equal(baseline.AddMinutes(22), vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(32), vals[60].Timestamp);

                    // should go to server 
                    vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(baseline.AddMinutes(1), baseline.AddMinutes(11))
                        .ToList();

                    Assert.Equal(3, session.Advanced.NumberOfRequests);

                    Assert.Equal(61, vals.Count);
                    Assert.Equal(baseline.AddMinutes(1), vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(11), vals[60].Timestamp);

                    Assert.True(((InMemoryDocumentSessionOperations)session).TimeSeriesByDocId.TryGetValue("users/ayende", out var cache));
                    Assert.True(cache.TryGetValue("Heartrate", out var ranges));
                    Assert.Equal(2, ranges.Count);

                    Assert.Equal(baseline.AddMinutes(1), ranges[0].From);
                    Assert.Equal(baseline.AddMinutes(11), ranges[0].To);
                    Assert.Equal(baseline.AddMinutes(22), ranges[1].From);
                    Assert.Equal(baseline.AddMinutes(32), ranges[1].To);

                    // should go to server to get [32, 35] and merge with [22, 32]
                    vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(baseline.AddMinutes(25), baseline.AddMinutes(35))
                        .ToList();

                    Assert.Equal(4, session.Advanced.NumberOfRequests);

                    Assert.Equal(61, vals.Count);
                    Assert.Equal(baseline.AddMinutes(25), vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(35), vals[60].Timestamp);

                    Assert.Equal(2, ranges.Count);

                    Assert.Equal(baseline.AddMinutes(1), ranges[0].From);
                    Assert.Equal(baseline.AddMinutes(11), ranges[0].To);
                    Assert.Equal(baseline.AddMinutes(22), ranges[1].From);
                    Assert.Equal(baseline.AddMinutes(35), ranges[1].To);

                    // should go to server to get [20, 22] and [35, 40]
                    // and merge them with [22, 35] into a single range [20, 40]
                    vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(baseline.AddMinutes(20), baseline.AddMinutes(40))
                        .ToList();

                    Assert.Equal(5, session.Advanced.NumberOfRequests);

                    Assert.Equal(121, vals.Count);
                    Assert.Equal(baseline.AddMinutes(20), vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(40), vals[120].Timestamp);

                    Assert.Equal(2, ranges.Count);

                    Assert.Equal(baseline.AddMinutes(1), ranges[0].From);
                    Assert.Equal(baseline.AddMinutes(11), ranges[0].To);
                    Assert.Equal(baseline.AddMinutes(20), ranges[1].From);
                    Assert.Equal(baseline.AddMinutes(40), ranges[1].To);

                    // should go to server to get [15, 20] and merge with [20, 40]
                    vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(baseline.AddMinutes(15), baseline.AddMinutes(35))
                        .ToList();

                    Assert.Equal(6, session.Advanced.NumberOfRequests);

                    Assert.Equal(121, vals.Count);
                    Assert.Equal(baseline.AddMinutes(15), vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(35), vals[120].Timestamp);

                    Assert.Equal(2, ranges.Count);

                    Assert.Equal(baseline.AddMinutes(1), ranges[0].From);
                    Assert.Equal(baseline.AddMinutes(11), ranges[0].To);
                    Assert.Equal(baseline.AddMinutes(15), ranges[1].From);
                    Assert.Equal(baseline.AddMinutes(40), ranges[1].To);

                    // should go to server and add new cache entry for Heartrate2
                    vals = session.TimeSeriesFor("users/ayende", "Heartrate2")
                        .Get(baseline, baseline.AddHours(2))
                        .ToList();

                    Assert.Equal(7, session.Advanced.NumberOfRequests);

                    Assert.Equal(2, vals.Count);
                    Assert.Equal(baseline.AddHours(1), vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(90), vals[1].Timestamp);

                    Assert.True(cache.TryGetValue("Heartrate2", out var ranges2));
                    Assert.Equal(1, ranges2.Count);
                    Assert.Equal(baseline, ranges2[0].From);
                    Assert.Equal(baseline.AddHours(2), ranges2[0].To);

                    // should not go to server
                    vals = session.TimeSeriesFor("users/ayende", "Heartrate2")
                        .Get(baseline.AddMinutes(30), baseline.AddMinutes(100))
                        .ToList();

                    Assert.Equal(7, session.Advanced.NumberOfRequests);

                    Assert.Equal(2, vals.Count);
                    Assert.Equal(baseline.AddHours(1), vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(90), vals[1].Timestamp);

                    // should go to server 
                    vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(baseline.AddMinutes(42), baseline.AddMinutes(43))
                        .ToList();

                    Assert.Equal(8, session.Advanced.NumberOfRequests);

                    Assert.Equal(7, vals.Count);
                    Assert.Equal(baseline.AddMinutes(42), vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(43), vals[6].Timestamp);

                    Assert.Equal(3, ranges.Count);

                    Assert.Equal(baseline.AddMinutes(1), ranges[0].From);
                    Assert.Equal(baseline.AddMinutes(11), ranges[0].To);
                    Assert.Equal(baseline.AddMinutes(15), ranges[1].From);
                    Assert.Equal(baseline.AddMinutes(40), ranges[1].To);
                    Assert.Equal(baseline.AddMinutes(42), ranges[2].From);
                    Assert.Equal(baseline.AddMinutes(43), ranges[2].To);

                    // should go to server and to get the missing parts and merge all ranges into [0, 45]
                    vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get( baseline, baseline.AddMinutes(45))
                        .ToList();

                    Assert.Equal(9, session.Advanced.NumberOfRequests);

                    Assert.Equal(271, vals.Count);
                    Assert.Equal(baseline, vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(45), vals[270].Timestamp);

                    Assert.True(cache.TryGetValue("Heartrate", out ranges));
                    Assert.Equal(1, ranges.Count);

                    Assert.Equal(baseline.AddMinutes(0), ranges[0].From);
                    Assert.Equal(baseline.AddMinutes(45), ranges[0].To);

                }
            }
        }

        [Fact]
        public void ShouldMergeTimeSeriesRangesInCache3()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    for (int i = 0; i < 360; i++)
                    {
                        tsf.Append(baseline.AddSeconds(i * 10), new[] { 60d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(baseline.AddMinutes(1), baseline.AddMinutes(2))
                        .ToList();

                    Assert.Equal(7, vals.Count);
                    Assert.Equal(baseline.AddMinutes(1), vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(2), vals[6].Timestamp);

                    vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(baseline.AddMinutes(5), baseline.AddMinutes(6))
                        .ToList();

                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    Assert.Equal(7, vals.Count);
                    Assert.Equal(baseline.AddMinutes(5), vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(6), vals[6].Timestamp);

                    Assert.True(((InMemoryDocumentSessionOperations)session).TimeSeriesByDocId.TryGetValue("users/ayende", out var cache));
                    Assert.True(cache.TryGetValue("Heartrate", out var ranges));
                    Assert.Equal(2, ranges.Count);

                    Assert.Equal(baseline.AddMinutes(1), ranges[0].From);
                    Assert.Equal(baseline.AddMinutes(2), ranges[0].To);
                    Assert.Equal(baseline.AddMinutes(5), ranges[1].From);
                    Assert.Equal(baseline.AddMinutes(6), ranges[1].To);

                    // should go to server to get [2, 3] and merge with [1, 2]
                    vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(baseline.AddMinutes(2), baseline.AddMinutes(3))
                        .ToList();

                    Assert.Equal(3, session.Advanced.NumberOfRequests);

                    Assert.Equal(7, vals.Count);
                    Assert.Equal(baseline.AddMinutes(2), vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(3), vals[6].Timestamp);

                    Assert.Equal(2, ranges.Count);

                    Assert.Equal(baseline.AddMinutes(1), ranges[0].From);
                    Assert.Equal(baseline.AddMinutes(3), ranges[0].To);
                    Assert.Equal(baseline.AddMinutes(5), ranges[1].From);
                    Assert.Equal(baseline.AddMinutes(6), ranges[1].To);

                    // should go to server to get [4, 5] and merge with [5, 6]
                    vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(baseline.AddMinutes(4), baseline.AddMinutes(5))
                        .ToList();

                    Assert.Equal(4, session.Advanced.NumberOfRequests);

                    Assert.Equal(7, vals.Count);
                    Assert.Equal(baseline.AddMinutes(4), vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(5), vals[6].Timestamp);

                    Assert.Equal(2, ranges.Count);

                    Assert.Equal(baseline.AddMinutes(1), ranges[0].From);
                    Assert.Equal(baseline.AddMinutes(3), ranges[0].To);
                    Assert.Equal(baseline.AddMinutes(4), ranges[1].From);
                    Assert.Equal(baseline.AddMinutes(6), ranges[1].To);

                    // should go to server to get [3, 4] and merge all ranges into [1, 6]
                    vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(baseline.AddMinutes(3), baseline.AddMinutes(4))
                        .ToList();

                    Assert.Equal(5, session.Advanced.NumberOfRequests);

                    Assert.Equal(7, vals.Count);
                    Assert.Equal(baseline.AddMinutes(3), vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(4), vals[6].Timestamp);

                    Assert.Equal(1, ranges.Count);

                    Assert.Equal(baseline.AddMinutes(1), ranges[0].From);
                    Assert.Equal(baseline.AddMinutes(6), ranges[0].To);

                }
            }
        }

        [Fact]
        public void CanHandleRangesWithNoValues()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    for (int i = 0; i < 360; i++)
                    {
                        tsf.Append(baseline.AddSeconds(i * 10), new[] { 60d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(baseline.AddHours(-2), baseline.AddHours(-1))
                        .ToList();

                    Assert.Equal(0, vals.Count);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // should not go to server
                    vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(baseline.AddHours(-2), baseline.AddHours(-1))
                        .ToList();

                    Assert.Equal(0, vals.Count);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // should not go to server
                    vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(baseline.AddMinutes(-90), baseline.AddMinutes(-70))
                        .ToList();

                    Assert.Equal(0, vals.Count);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // should go to server to get [-60, 1] and merge with [-120, -60]

                    vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(baseline.AddHours(-1), baseline.AddMinutes(1))
                        .ToList();

                    Assert.Equal(7, vals.Count);
                    Assert.Equal(baseline, vals[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(1), vals[6].Timestamp);

                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    Assert.True(((InMemoryDocumentSessionOperations)session).TimeSeriesByDocId.TryGetValue("users/ayende", out var cache));
                    Assert.True(cache.TryGetValue("Heartrate", out var ranges));
                    Assert.Equal(1, ranges.Count);

                    Assert.Equal(baseline.AddHours(-2), ranges[0].From);
                    Assert.Equal(baseline.AddMinutes(1), ranges[0].To);

                }
            }
        }

    }
}
