using System;
using System.Linq;
using FastTests;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_14312 : RavenTestBase
    {
        public RavenDB_14312(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanHaveNullTag()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline, null, new[] { 59d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende")
                        .Get("Heartrate", DateTime.MinValue, DateTime.MaxValue)
                        .ToList();

                    Assert.Equal(1, vals.Count);
                    Assert.Null(vals[0].Tag);
                    Assert.Equal(baseline, vals[0].Timestamp);
                    Assert.Equal(new[] { 59d }, vals[0].Values);
                }
            }
        }

        [Fact]
        public void CanHaveNullTag2()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;
                var id = "users/1";

                using (var session = store.OpenSession())
                {

                    session.Store(new User
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.TimeSeriesFor(id);

                    tsf.Append("HeartRate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                    tsf.Append("HeartRate", baseline.AddMinutes(62), "watches/apple", new[] { 79d });
                    tsf.Append("HeartRate", baseline.AddMinutes(63), null, new[] { 69d });

                    tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(61), "watches/apple", new[] { 159d });
                    tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(62), null, new[] { 179d });
                    tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(63), "watches/fitbit", new[] { 169d });

                    tsf.Append("HeartRate", baseline.AddMonths(3).AddMinutes(61), null, new[] { 159d });
                    tsf.Append("HeartRate", baseline.AddMonths(3).AddMinutes(62), null, new[] { 179d });
                    tsf.Append("HeartRate", baseline.AddMonths(3).AddMinutes(63), "watches/fitbit", new[] { 169d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var all = session.TimeSeriesFor(id).Get("HeartRate", DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(9, all.Count);
                }
            }
        }
    }
}
