using System;
using System.Linq;
using System.Text;
using FastTests;
using Raven.Client.Documents.Session;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Query
{
    public class TimeSeriesQueryTests : RavenTestBase
    {
        public TimeSeriesQueryTests(ITestOutputHelper output) : base(output)
        {
        }

        private class TimeSeriesRangeAggregation
        {
#pragma warning disable 649
            public long Count;
            public double? Max, Min, Last, First;
            public DateTime To, From;
#pragma warning restore 649
        }

        private class TimeSeriesAggregation
        {
            public long Count { get; set; }
            public TimeSeriesRangeAggregation[] Results { get; set; }
        }

        [Fact]
        public unsafe void CanQueryTimeSeriesAggregation_Simple()
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
                    var query = session.Advanced.RawQuery<TimeSeriesAggregation>(@"
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
                        .AddParameter("end", baseline.AddDays(1));

                    var agg = query.First();

                    if (agg.Count != 3)
                    {
                        var db = GetDocumentDatabaseInstanceFor(store).Result;
                        var tss = db.DocumentsStorage.TimeSeriesStorage;
                        using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                        using (ctx.OpenReadTransaction())
                        {
                            var reader = tss.GetReader(ctx, "users/ayende", "Heartrate", baseline, baseline.AddDays(1));

                            Assert.True(reader.Init());

                            Assert.NotNull(reader._tvr);

                            var key = reader._tvr.Read(0, out var size);

                            TimeSeriesValuesSegment.ParseTimeSeriesKey(key, size, out var docId, out var name, out DateTime baseline2);

                            Assert.Equal("users/ayende", docId);
                            Assert.Equal("Heartrate", name);
                            Assert.Equal(baseline.AddMinutes(61), baseline2);

                            Assert.Equal(1, reader.SegmentsOrValues().Count());

                            Assert.False(query.First().Count == 3, "Query assertion failed once and passed on second try. sanity check passed");

                            //Assert.True(false, "Query assertion failed twice. sanity check passed");
                        }
                    }

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
