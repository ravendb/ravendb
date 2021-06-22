using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Queries;
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
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline, new[] { 59d });
                    
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .ToList();

                    Assert.Equal(1, vals.Count);
                    Assert.Null(vals[0].Tag);
                    Assert.Equal(baseline, vals[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(new[] { 59d }, vals[0].Values);
                }
            }
        }

        [Fact]
        public void CanHaveNullTag2()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                var id = "users/1";

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d });

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d });
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");

                    tsf.Append(baseline.AddMonths(3).AddMinutes(61), new[] { 159d });
                    tsf.Append(baseline.AddMonths(3).AddMinutes(62), new[] { 179d });
                    tsf.Append(baseline.AddMonths(3).AddMinutes(63), new[] { 169d }, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var all = session.TimeSeriesFor(id, "HeartRate").Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(9, all.Count);
                }
            }
        }
    }
}
