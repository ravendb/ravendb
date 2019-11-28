using System;
using System.Linq;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries
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
    }
}
