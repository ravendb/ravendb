using System;
using System.Linq;
using FastTests;
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
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", now1, "watches/fitbit", new[] { 59d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende")
                        .Remove("Heartrate", now1);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende")
                        .Get("Heartrate", DateTime.MinValue, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(0, vals.Count);
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
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", now1, "watches/fitbit", new[] { 59d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", now1, "watches/fitbit", new[] { 59d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende")
                        .Get("Heartrate", DateTime.MinValue, DateTime.MaxValue)
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
                var now1 = DateTime.Now;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", now1, "watches/fitbit", new[] { 59d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", now1.AddMinutes(5), "watches/apple", new[] { 64d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende")
                        .Get("Heartrate", now1, DateTime.MaxValue)
                        .ToList();
                    Assert.Equal(2, vals.Count);
                }
            }
        }
    }
}
