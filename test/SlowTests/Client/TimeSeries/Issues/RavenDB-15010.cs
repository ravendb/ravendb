using System;
using FastTests;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_15010 : RavenTestBase
    {
        public RavenDB_15010(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanPassTimeSeriesNameAsQueryParameter()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");

                    for (int i = 0; i < 100; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var q = session.Advanced.RawQuery<TimeSeriesAggregationResult>($@"
declare timeseries out() 
{{
    from $name between $start and $end
    group by 1h
    select min(), max(), first(), last()
}}
from @all_docs as u
where id() == 'users/ayende'
select out()
")
                        .AddParameter("start", DateTime.MinValue)
                        .AddParameter("end", DateTime.MaxValue)
                        .AddParameter("name", "Heartrate");

                    var res = q.First();

                    Assert.Equal(100, res.Count);
                    Assert.Equal(2, res.Results.Length);
                    Assert.Equal(baseline, res.Results[0].From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddHours(1), res.Results[0].To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddHours(1), res.Results[1].From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddHours(2), res.Results[1].To, RavenTestHelper.DateTimeComparer.Instance);

                }
            }
        }

        [Fact]
        public void CanPassTimeSeriesNameAsQueryParameter_2()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");

                    for (int i = 0; i < 100; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var q = session.Advanced.RawQuery<TimeSeriesAggregationResult>($@"
declare timeseries out(name) 
{{
    from name between $start and $end
    group by 1h
    select min(), max(), first(), last()
}}
from @all_docs as u
where id() == 'users/ayende'
select out($tsName)
")
                        .AddParameter("start", DateTime.MinValue)
                        .AddParameter("end", DateTime.MaxValue)
                        .AddParameter("$tsName", "Heartrate");

                    var res = q.First();

                    Assert.Equal(100, res.Count);
                    Assert.Equal(2, res.Results.Length);
                    Assert.Equal(baseline, res.Results[0].From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddHours(1), res.Results[0].To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddHours(1), res.Results[1].From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddHours(2), res.Results[1].To, RavenTestHelper.DateTimeComparer.Instance);

                }
            }
        }
    }
}
