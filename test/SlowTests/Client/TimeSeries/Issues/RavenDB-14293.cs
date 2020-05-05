using System;
using System.Linq;
using FastTests;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_14293 : RavenTestBase
    {
        public RavenDB_14293(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanRemoveTimeSeriesEntry()
        {
            using (var store = GetDocumentStore())
            {
                var now1 = DateTime.Now; // DateTime.Today works as expected

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(now1, new[] { 59d }, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Remove(now1);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)?
                        .ToList();
                    Assert.Null(vals);
                }
            }
        }

        [Fact]
        public void ShouldNotAppendSameValueTwice()
        {
            using (var store = GetDocumentStore())
            {
                var now1 = DateTime.Now;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(now1, new[] { 59d }, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(now1, new[] { 59d }, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(1, vals.Count);
                }
            }
        }

        [Fact]
        public void CanGetTimeSeriesRange()
        {
            using (var store = GetDocumentStore())
            {
                var now1 = DateTime.Now.EnsureMilliseconds();

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(now1, new[] { 59d }, "watches/fitbit");
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(now1.AddMinutes(5), new[] { 64d }, "watches/apple");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(now1, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(2, vals.Count);
                }
            }
        }
    }
}
