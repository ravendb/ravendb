using System;
using FastTests;
using Xunit;

namespace SlowTests.Client.TimeSeries.Query
{
    public class TimeSeriesQueryTests : RavenTestBase
    {
        private class TimeSeriesRangeAggregation
        {
            public long Count;
            public double? Max, Min, Last, First;
            public DateTime To, From;
        }

        private class TimeSeriesAggregation
        {
            public long Count { get; set; }
            public TimeSeriesRangeAggregation[] Results { get; set; }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_Simple()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(62), "watches/fitbit", new[] { 79d });

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });
                    session.SaveChanges();
                }


                using (var session = store.OpenSession())
                {
                    var agg = session.Advanced.RawQuery<TimeSeriesAggregation>(@"
    declare timeseries out(u) 
    {
        from u.Heartrate between $start and $end
        group by 1h
        select min(), max(), first(), last()
    }
    from @all_docs as u
    where id() == 'users/ayende'
    select out(u)
")
        .AddParameter("start", baseline)
        .AddParameter("end", baseline.AddDays(1))
        .First();
                    Assert.Equal(3, agg.Count);

                    Assert.Equal(1, agg.Results.Length);

                    var val = agg.Results[0];


                    Assert.Equal(59, val.First);
                    Assert.Equal(59, val.Min);

                    Assert.Equal(69, val.Last);
                    Assert.Equal(79, val.Max);

                    Assert.Equal(baseline.AddMinutes(60), val.From);
                    Assert.Equal(baseline.AddMinutes(120), val.To);
                }
            }
        }

    }
}
