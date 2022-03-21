using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Sparrow.Extensions;
using Xunit;
using Xunit.Abstractions;
using static SlowTests.Client.TimeSeries.Issues.RavenDB_15020;

namespace SlowTests.Client.TimeSeries.Query
{
    public class IncrementalTimeSeriesQueries : ReplicationTestBase
    {
        public IncrementalTimeSeriesQueries(ITestOutputHelper output) : base(output)
        {
        }

        private const string IncrementalTsName = Constants.Headers.IncrementalTimeSeriesPrefix + "HeartRate";

        private class PeopleIndex : AbstractIndexCreationTask<Person>
        {
            public PeopleIndex()
            {
                Map = people => from person in people
                                select new
                                {
                                    person.Age
                                };
            }

            public override string IndexName => "People";
        }


        private class Person
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public string LastName { get; set; }

            public int Age { get; set; }

            public string WorksAt { get; set; }
        }


        private class QueryResult
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public TimeSeriesAggregationResult HeartRate { get; set; }

            public TimeSeriesAggregationResult BloodPressure { get; set; }
        }

        private class RawQueryResult
        {
            public TimeSeriesAggregationResult HeartRate { get; set; }

            public TimeSeriesAggregationResult BloodPressure { get; set; }

            public TimeSeriesAggregationResult Stocks { get; set; }

            public string Name { get; set; }
        }

        private class CustomRawQueryResult
        {
            public double Value { get; set; }

            public string Tag { get; set; }

            public long Count { get; set; }

            public long Mid { get; set; }
        }

        private class CustomRawQueryResult2
        {
            public TimeSeriesRawResult Series { get; set; }

            public TimeSeriesRawResult Series2 { get; set; }

            public double[] Series3 { get; set; }
        }

        private class CustomRawQueryResult3
        {
            public TimeSeriesRawResult HeartRate { get; set; }

            public TimeSeriesRawResult Stocks { get; set; }
        }

        private class TsResult
        {
            public double[] Percentile { get; set; }

            public long[] Count { get; set; }

            public double[] StdDev { get; set; }

        }

        private struct StockPrice
        {
#pragma warning disable CS0649
            [TimeSeriesValue(0)] public double Open;
            [TimeSeriesValue(1)] public double Close;
            [TimeSeriesValue(2)] public double High;
            [TimeSeriesValue(3)] public double Low;
            [TimeSeriesValue(4)] public double Volume;
#pragma warning restore CS0649
        }

        [Fact]
        public void CanQueryIncrementalTimeSeriesAggregation_DeclareSyntax()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");

                    var tsf = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                    tsf.Increment(baseline.AddMinutes(61), new[] { 59d });
                    tsf.Increment(baseline.AddMinutes(62), new[] { 79d });
                    tsf.Increment(baseline.AddMinutes(63), new[] { 69d });

                    session.SaveChanges();
                }

                WaitForUserToContinueTheTest(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
    declare timeseries out(u)
    {
        from u.'INC:HeartRate' between $start and $end
        group by 1h
        select min(), max(), first(), last()
    }
    from @all_docs as u
    where id() == 'users/ayende'
    select out(u)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddDays(1).EnsureUtc());

                    var agg = query.First();

                    Assert.Equal(3, agg.Count);

                    Assert.Equal(1, agg.Results.Length);

                    var val = agg.Results[0];

                    Assert.Equal(59, val.First[0]);
                    Assert.Equal(59, val.Min[0]);

                    Assert.Equal(69, val.Last[0]);
                    Assert.Equal(79, val.Max[0]);

                    Assert.Equal(baseline.AddMinutes(60), val.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(120), val.To, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void CanQueryIncrementalTimeSeriesAggregation_DeclareSyntax_MultipleSeries()
        {
            const string incTsName2 = Constants.Headers.IncrementalTimeSeriesPrefix + "BloodPressure";
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                var baseline2 = RavenTestHelper.UtcToday.AddDays(-1);

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.IncrementalTimeSeriesFor(id, IncrementalTsName);
                        tsf.Increment(baseline.AddMinutes(61), new[] { 59d });
                        tsf.Increment(baseline.AddMinutes(62), new[] { 79d });
                        tsf.Increment(baseline.AddMinutes(63), new[] { 69d });

                        tsf = session.IncrementalTimeSeriesFor(id, incTsName2);
                        tsf.Increment(baseline2.AddMinutes(61), new[] { 159d });
                        tsf.Increment(baseline2.AddMinutes(62), new[] { 179d });
                        tsf.Increment(baseline2.AddMinutes(63), new[] { 168d });
                    }

                    session.SaveChanges();
                }

                new PeopleIndex().Execute(store);

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<RawQueryResult>(@"
declare timeseries heart_rate(doc)
{
    from doc.'INC:HeartRate' between $start and $end
    group by 1h
    select min(), max()
}
declare timeseries blood_pressure(doc)
{
    from doc.'INC:BloodPressure' between $start2 and $end2
    group by 1h
    select min(), max(), avg()
}
from index 'People' as p
where p.Age > 49
select heart_rate(p) as HeartRate, blood_pressure(p) as BloodPressure
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddDays(1).EnsureUtc())
                        .AddParameter("start2", baseline2.EnsureUtc())
                        .AddParameter("end2", baseline2.AddDays(1).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        var heartrate = agg.HeartRate;

                        Assert.Equal(3, heartrate.Count);

                        Assert.Equal(1, heartrate.Results.Length);

                        var val = heartrate.Results[0];

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);

                        Assert.Equal(baseline.AddMinutes(60), val.From, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(baseline.AddMinutes(120), val.To, RavenTestHelper.DateTimeComparer.Instance);

                        var bloodPressure = agg.BloodPressure;

                        Assert.Equal(3, bloodPressure.Count);

                        Assert.Equal(1, bloodPressure.Results.Length);

                        val = bloodPressure.Results[0];

                        Assert.Equal(159, val.Min[0]);
                        Assert.Equal(179, val.Max[0]);

                        double expectedAvg = (159 + 168 + 179) / 3.0;

                        Assert.Equal(expectedAvg, val.Average[0]);

                        Assert.Equal(baseline2.AddMinutes(60), val.From, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(baseline2.AddMinutes(120), val.To, RavenTestHelper.DateTimeComparer.Instance);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryIncrementalTimeSeriesAggregation_DeclareSyntax_FromLoadedDocument()
        {
            const string stocksIncTs = Constants.Headers.IncrementalTimeSeriesPrefix + "Stocks";
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";
                        var company = $"companies/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30,
                            WorksAt = company
                        }, id);

                        session.Store(new Company(), company);

                        var tsf = session.IncrementalTimeSeriesFor(company, stocksIncTs);

                        tsf.Increment(baseline.AddMinutes(61), new[] { 1259.51d });
                        tsf.Increment(baseline.AddMinutes(62), new[] { 1279.62d });
                        tsf.Increment(baseline.AddMinutes(63), new[] { 1269.73d });
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(c)
{
    from c.'INC:Stocks' between $start and $end
    group by 1h
    select min(), max(), avg()
}
from People as p
where p.Age > 49
load p.WorksAt as Company
select out(Company)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddDays(1).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(3, agg.Count);

                        Assert.Equal(1, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(1259.51d, val.Min[0]);
                        Assert.Equal(1279.62d, val.Max[0]);

                        double expectedAvg = (1259.51d + 1279.62d + 1269.73d) / 3.0;
                        Assert.Equal(expectedAvg, val.Average[0]);

                        Assert.Equal(baseline.AddMinutes(60), val.From, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(baseline.AddMinutes(120), val.To, RavenTestHelper.DateTimeComparer.Instance);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryIncrementalTimeSeriesAggregation_SelectSyntax()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.IncrementalTimeSeriesFor(id, IncrementalTsName);
                        tsf.Increment(baseline.AddMinutes(61), new[] { 59d });
                        tsf.Increment(baseline.AddMinutes(62), new[] { 79d });
                        tsf.Increment(baseline.AddMinutes(63), new[] { 69d });
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
from People as p
select timeseries(
    from 'INC:HeartRate' between $start and $end
    group by 1h
    select min(), max())
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddDays(1).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(3, result.Count);

                    for (int i = 0; i < 3; i++)
                    {
                        var agg = result[i];
                        Assert.Equal(3, agg.Count);

                        Assert.Equal(1, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);

                        Assert.Equal(baseline.AddMinutes(60), val.From, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(baseline.AddMinutes(120), val.To, RavenTestHelper.DateTimeComparer.Instance);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryIncrementalTimeSeriesAggregation_SelectSyntax_WhereOnValue()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.IncrementalTimeSeriesFor(id, IncrementalTsName);

                        tsf.Increment(baseline.AddMinutes(61), 59);
                        tsf.Increment(baseline.AddMinutes(62), 79);
                        tsf.Increment(baseline.AddMinutes(63), 69);

                        tsf.Increment(baseline.AddMonths(1).AddMinutes(61), 159);
                        tsf.Increment(baseline.AddMonths(1).AddMinutes(62), 67);
                        tsf.Increment(baseline.AddMonths(1).AddMinutes(63), 57);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
from People as doc
where doc.Age > 49
select timeseries(from doc.'INC:HeartRate' between $start and $end
        where Values[0] < 70
    group by '1 month'
    select min(), max())
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());
                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(4, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(69, val.Max[0]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(57, val.Min[0]);
                        Assert.Equal(67, val.Max[0]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Theory]
        [InlineData("months")]
        [InlineData("month")]
        [InlineData("mon")]
        [InlineData("mo")]
        public void CanQueryIncrementalTimeSeriesAggregation_GroupByMonth(string syntax)
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.IncrementalTimeSeriesFor(id, IncrementalTsName);

                        tsf.Increment(baseline.AddMinutes(61), 59);
                        tsf.Increment(baseline.AddMinutes(62), 79);
                        tsf.Increment(baseline.AddMinutes(63), 69);

                        tsf.Increment(baseline.AddMonths(1).AddMinutes(61), 159);
                        tsf.Increment(baseline.AddMonths(1).AddMinutes(62), 179);
                        tsf.Increment(baseline.AddMonths(1).AddMinutes(63), 169);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>($@"
declare timeseries out(x)
{{
    from x.'{IncrementalTsName}' between $start and $end
    group by '1 {syntax}'
    select min(), max(), avg()
}}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(6, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(159, val.Min[0]);
                        Assert.Equal(179, val.Max[0]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryIncrementalTimeSeriesAggregation_NoSelectOrGroupBy()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.IncrementalTimeSeriesFor(id, IncrementalTsName);

                        tsf.Increment(baseline.AddMinutes(61), 59);
                        tsf.Increment(baseline.AddMinutes(62), 79);
                        tsf.Increment(baseline.AddMinutes(63), 69);

                        tsf.Increment(baseline.AddMonths(1).AddMinutes(61), 159);
                        tsf.Increment(baseline.AddMonths(1).AddMinutes(62), 179);
                        tsf.Increment(baseline.AddMonths(1).AddMinutes(63), 169);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesRawResult>(@"
declare timeseries out(x)
{
    from x.'INC:HeartRate' between $start and $end
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(6, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(59, val.Values[0]);
                        Assert.Equal(baseline.AddMinutes(61), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[1];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(79, val.Values[0]);
                        Assert.Equal(baseline.AddMinutes(62), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[2];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(69, val.Values[0]);
                        Assert.Equal(baseline.AddMinutes(63), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[3];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(159, val.Values[0]);
                        Assert.Equal(baseline.AddMonths(1).AddMinutes(61), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[4];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(179, val.Values[0]);
                        Assert.Equal(baseline.AddMonths(1).AddMinutes(62), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[5];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(169, val.Values[0]);
                        Assert.Equal(baseline.AddMonths(1).AddMinutes(63), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryIncrementalTimeSeriesAggregation_WithMultipleValues()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.IncrementalTimeSeriesFor(id, IncrementalTsName);

                        tsf.Increment(baseline.AddMinutes(61), new[] { 59d, 159 });
                        tsf.Increment(baseline.AddMinutes(62), new[] { 79d, 179 });
                        tsf.Increment(baseline.AddMinutes(63), new[] { 69d, 169 });

                        tsf.Increment(baseline.AddMonths(1).AddMinutes(61), new[] { 159d, 259 });
                        tsf.Increment(baseline.AddMonths(1).AddMinutes(62), new[] { 179d, 279 });
                        tsf.Increment(baseline.AddMonths(1).AddMinutes(63), new[] { 169d, 269 });
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries heart_rate(doc)
{
    from doc.'INC:HeartRate' between $start and $end
    group by '1 month'
    select min(), max(), avg()
}
from People as p
where p.Age > 49
select heart_rate(p)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(3).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(6, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(2, val.Min.Length);
                        Assert.Equal(2, val.Max.Length);
                        Assert.Equal(2, val.Average.Length);

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);
                        Assert.Equal(69, val.Average[0]);

                        Assert.Equal(159, val.Min[1]);
                        Assert.Equal(179, val.Max[1]);
                        Assert.Equal(169, val.Average[1]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(2, val.Min.Length);
                        Assert.Equal(2, val.Max.Length);
                        Assert.Equal(2, val.Average.Length);

                        Assert.Equal(159, val.Min[0]);
                        Assert.Equal(179, val.Max[0]);
                        Assert.Equal(169, val.Average[0]);

                        Assert.Equal(259, val.Min[1]);
                        Assert.Equal(279, val.Max[1]);
                        Assert.Equal(269, val.Average[1]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryIncrementalTimeSeriesAggregation_DifferentNumberOfValues()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.IncrementalTimeSeriesFor(id, IncrementalTsName);

                        tsf.Increment(baseline.AddMinutes(61), new[] { 59d });
                        tsf.Increment(baseline.AddMinutes(62), new[] { 79d, 179 });
                        tsf.Increment(baseline.AddMinutes(63), new[] { 69d, 169, 269 });

                        tsf.Increment(baseline.AddDays(1).AddMinutes(61), new[] { 159d, 259 });
                        tsf.Increment(baseline.AddDays(1).AddMinutes(62), new[] { 179d, 279, 379 });
                        tsf.Increment(baseline.AddDays(1).AddMinutes(63), new[] { 169d });

                        tsf.Increment(baseline.AddDays(2).AddMinutes(61), new[] { 259d, 359, 459 });
                        tsf.Increment(baseline.AddDays(2).AddMinutes(62), new[] { 279d });
                        tsf.Increment(baseline.AddDays(2).AddMinutes(63), new[] { 269d, 369, 469, 569 });
                    }

                    session.SaveChanges();
                }

                var offset = TimeZoneInfo.Local.BaseUtcOffset.ToString(@"hh\:mm");
                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>($@"
declare timeseries heart_rate(doc)
{{
    from doc.'{IncrementalTsName}' between $start and $end
    group by '1 day'
    select min(), max(), avg(), first(), last()
    offset '{offset}'
}}
from People as p
where p.Age > 49
select heart_rate(p)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(3).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(9, agg.Count);

                        Assert.Equal(3, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(3, val.Min.Length);
                        Assert.Equal(3, val.Max.Length);
                        Assert.Equal(3, val.Average.Length);
                        Assert.Equal(3, val.First.Length);
                        Assert.Equal(3, val.Last.Length);

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);
                        Assert.Equal(69, val.Average[0]);
                        Assert.Equal(59, val.First[0]);
                        Assert.Equal(69, val.Last[0]);
                        Assert.Equal(3, val.Count[0]);

                        Assert.Equal(169, val.Min[1]);
                        Assert.Equal(179, val.Max[1]);
                        Assert.Equal(2, val.Count[1]);
                        Assert.Equal((169 + 179) / 2, val.Average[1]);
                        Assert.Equal(179, val.First[1]);
                        Assert.Equal(169, val.Last[1]);

                        Assert.Equal(269, val.Min[2]);
                        Assert.Equal(269, val.Max[2]);
                        Assert.Equal(1, val.Count[2]);
                        Assert.Equal(269, val.Average[2]);
                        Assert.Equal(269, val.First[2]);
                        Assert.Equal(269, val.Last[2]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, baseline.Day, 0, 0, 0);
                        var expectedTo = expectedFrom.AddDays(1);

                        Assert.Equal(expectedFrom, val.From, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(expectedTo, val.To, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[1];

                        Assert.Equal(3, val.Min.Length);
                        Assert.Equal(3, val.Max.Length);
                        Assert.Equal(3, val.Average.Length);
                        Assert.Equal(3, val.First.Length);
                        Assert.Equal(3, val.Last.Length);

                        Assert.Equal(159, val.Min[0]);
                        Assert.Equal(179, val.Max[0]);
                        Assert.Equal(169, val.Average[0]);
                        Assert.Equal(159, val.First[0]);
                        Assert.Equal(169, val.Last[0]);
                        Assert.Equal(3, val.Count[0]);

                        Assert.Equal(259, val.Min[1]);
                        Assert.Equal(279, val.Max[1]);
                        Assert.Equal(2, val.Count[1]);
                        Assert.Equal((259d + 279) / 2, val.Average[1]);
                        Assert.Equal(259, val.First[1]);
                        Assert.Equal(279, val.Last[1]);

                        Assert.Equal(379, val.Min[2]);
                        Assert.Equal(379, val.Max[2]);
                        Assert.Equal(1, val.Count[2]);
                        Assert.Equal(379, val.Average[2]);
                        Assert.Equal(379, val.First[2]);
                        Assert.Equal(379, val.Last[2]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddDays(1);

                        Assert.Equal(expectedFrom, val.From, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(expectedTo, val.To, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[2];

                        Assert.Equal(4, val.Min.Length);
                        Assert.Equal(4, val.Max.Length);
                        Assert.Equal(4, val.Average.Length);
                        Assert.Equal(4, val.First.Length);
                        Assert.Equal(4, val.Last.Length);

                        Assert.Equal(259, val.Min[0]);
                        Assert.Equal(279, val.Max[0]);
                        Assert.Equal(269, val.Average[0]);
                        Assert.Equal(259, val.First[0]);
                        Assert.Equal(269, val.Last[0]);
                        Assert.Equal(3, val.Count[0]);

                        Assert.Equal(359, val.Min[1]);
                        Assert.Equal(369, val.Max[1]);
                        Assert.Equal(2, val.Count[1]);
                        Assert.Equal((359d + 369) / 2, val.Average[1]);
                        Assert.Equal(359, val.First[1]);
                        Assert.Equal(369, val.Last[1]);

                        Assert.Equal(459, val.Min[2]);
                        Assert.Equal(469, val.Max[2]);
                        Assert.Equal(2, val.Count[2]);
                        Assert.Equal((459d + 469) / 2, val.Average[2]);
                        Assert.Equal(459, val.First[2]);
                        Assert.Equal(469, val.Last[2]);

                        Assert.Equal(569, val.Min[3]);
                        Assert.Equal(569, val.Max[3]);
                        Assert.Equal(1, val.Count[3]);
                        Assert.Equal(569, val.Average[3]);
                        Assert.Equal(569, val.First[3]);
                        Assert.Equal(569, val.Last[3]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddDays(1);

                        Assert.Equal(expectedFrom, val.From, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(expectedTo, val.To, RavenTestHelper.DateTimeComparer.Instance);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryIncrementalTimeSeriesAggregation_WhereOnLoadedDocumentArgument()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "HR",
                        AccountsReceivable = (decimal)70.7
                    }, "companies/1");

                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";
                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30,
                            WorksAt = "companies/1"
                        }, id);

                        var tsf = session.IncrementalTimeSeriesFor(id, IncrementalTsName);

                        tsf.Increment(baseline.AddMinutes(61), 59);
                        tsf.Increment(baseline.AddMinutes(62), 79);
                        tsf.Increment(baseline.AddMinutes(63), 69);

                        tsf.Increment(baseline.AddMonths(1).AddMinutes(61), 159);
                        tsf.Increment(baseline.AddMonths(1).AddMinutes(62), 179);
                        tsf.Increment(baseline.AddMonths(1).AddMinutes(63), 169);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x, y)
{
    from x.'INC:HeartRate' between $start and $end
        where Values[0] > y.AccountsReceivable
    group by '1 month'
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
load doc.WorksAt as c
select out(doc, c)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(4, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(79, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);
                        Assert.Equal(79, val.Average[0]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(159, val.Min[0]);
                        Assert.Equal(179, val.Max[0]);
                        Assert.Equal(169, val.Average[0]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryIncrementalTimeSeriesRaw_UsingLast()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday.EnsureUtc().AddDays(-7);
                var id = "people/1";
                var totalMinutes = TimeSpan.FromDays(3).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.IncrementalTimeSeriesFor(id, IncrementalTsName);

                    for (int i = 0; i <= totalMinutes; i++)
                    {
                        tsf.Increment(baseline.AddMinutes(i), i);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<TimeSeriesRawResult>(@"
declare timeseries out(x)
{
    from x.'INC:HeartRate' last 12h
}
from People as doc
where id(doc) = $id
select out(doc)
")
                        .AddParameter("id", id)
                        .First();

                    var expectedInitialTimestamp = baseline.AddDays(3).AddHours(-12);
                    var expectedInitialValueValue = totalMinutes - TimeSpan.FromHours(12).TotalMinutes;
                    var expectedCount = totalMinutes - expectedInitialValueValue + 1;

                    Assert.Equal(expectedCount, result.Count);

                    for (int i = 0; i < expectedCount; i++)
                    {
                        Assert.Equal(expectedInitialValueValue + i, result.Results[i].Value);
                        Assert.Equal(expectedInitialTimestamp.AddMinutes(i), result.Results[i].Timestamp);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryIncrementalTimeSeriesAggregation_UsingFirst()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday.EnsureUtc().AddDays(-7);
                var id = "people/1";
                var totalMinutes = TimeSpan.FromDays(3).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.IncrementalTimeSeriesFor(id, IncrementalTsName);

                    for (int i = 0; i <= totalMinutes; i++)
                    {
                        tsf.Increment(baseline.AddMinutes(i), i);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x)
{
    from x.'INC:HeartRate'
    first 30 minutes
    group by 10 minutes
    select min(), max(), avg()
}
from People as doc
where id(doc) = $id
select out(doc)
")
                        .AddParameter("id", id)
                        .First();

                    Assert.Equal(31, result.Count);
                    Assert.Equal(4, result.Results.Length);

                    Assert.Equal(baseline, result.Results[0].From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(10), result.Results[1].From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(20), result.Results[2].From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(30), result.Results[3].From, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void CanQueryIncrementalTimeSeriesRaw_UsingScaleAndOffset()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday.EnsureUtc();
                var id = "people/1";
                var totalHours = TimeSpan.FromDays(3).TotalHours;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.IncrementalTimeSeriesFor(id, IncrementalTsName);

                    for (int i = 0; i < totalHours; i++)
                    {
                        tsf.Increment(baseline.AddHours(i), i);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var offset = TimeZoneInfo.Local.BaseUtcOffset;
                    var scale = 0.001;

                    var result = session.Advanced.RawQuery<TimeSeriesRawResult>(
                            @"
declare timeseries out(doc)
{
    from doc.'INC:HeartRate'
    between $start and $end
    scale $scale
    offset $offset
}
from People as p
where id(p) = $id
select out(p)
")
                        .AddParameter("id", id)
                        .AddParameter("start", baseline)
                        .AddParameter("scale", scale)
                        .AddParameter("offset", offset)
                        .AddParameter("end", baseline.AddDays(3))
                        .First();

                    var expectedTotalCount = TimeSpan.FromDays(3).TotalHours;

                    Assert.Equal(expectedTotalCount, result.Count);

                    var baselineWithOffset = baseline.Add(offset);

                    for (int i = 0; i < result.Results.Length; i++)
                    {
                        var expectedTimestamp = baselineWithOffset.AddHours(i);
                        Assert.Equal(expectedTimestamp, result.Results[i].Timestamp);

                        var expectedVal = i * scale;
                        var val = result.Results[i].Value;

                        Assert.True(expectedVal.AlmostEquals(val));
                        Assert.Equal(expectedVal, result.Results[i].Value);
                    }
                }
            }
        }


        [Fact]
        public async Task CanQueryIncrementalTimeSeriesUsingNamedValues()
        {
            const string tsName = Constants.Headers.IncrementalTimeSeriesPrefix + "StockPrices";
            using (var store = GetDocumentStore())
            {
                await store.TimeSeries.RegisterAsync<Company, StockPrice>(name: tsName);

                var updated = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database))).TimeSeries;

                var stock = updated.GetNames("companies", tsName);
                Assert.Equal(5, stock.Length);
                Assert.Equal(nameof(StockPrice.Open), stock[0]);
                Assert.Equal(nameof(StockPrice.Close), stock[1]);
                Assert.Equal(nameof(StockPrice.High), stock[2]);
                Assert.Equal(nameof(StockPrice.Low), stock[3]);
                Assert.Equal(nameof(StockPrice.Volume), stock[4]);

                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new Company(), "companies/1");
                    var tsf = session.IncrementalTimeSeriesFor<StockPrice>("companies/1", tsName);
                    var random = new Random();
                    for (int i = 0; i < 10; i++)
                    {
                        var rand = random.Next(1, 10) / 10.0;
                        var even = i % 2 == 0;
                        var add = even ? rand : -rand;
                        tsf.Increment(baseline.AddHours(i), new[] { 45.37 + add, 45.72 + add, 45.99 + add, 45.21 + add, 719.636 + add });
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesRawResult<StockPrice>>(@"
declare timeseries out(c){
    from c.'INC:StockPrices'
    between $start and $end
    where High > 45.99
}
from Companies as c
where id() == 'companies/1'
select out(c)
")
                        .AddParameter("start", baseline.AddDays(-1).EnsureUtc())
                        .AddParameter("end", baseline.AddDays(1).EnsureUtc());

                    var result = query.First();

                    Assert.Equal(5, result.Count);
                    foreach (var entry in result.Results)
                    {
                        Assert.True(entry.Value.High > 45.99);
                    }
                }
            }
        }


        [Fact]
        public void CanPassIncrementalTimeSeriesNameAsQueryParameter()
        {
            const string id = "users/ayende";

            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), id);

                    var tsf = session.IncrementalTimeSeriesFor(id, IncrementalTsName);

                    for (int i = 0; i < 100; i++)
                    {
                        tsf.Increment(baseline.AddMinutes(i), i);
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
                        .AddParameter("name", IncrementalTsName);

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
        public void CanPassIncrementalTimeSeriesNameAsQueryParameter_2()
        {
            const string id = "users/ayende";

            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), id);

                    var tsf = session.IncrementalTimeSeriesFor(id, IncrementalTsName);

                    for (int i = 0; i < 100; i++)
                    {
                        tsf.Increment(baseline.AddMinutes(i), i);
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
                        .AddParameter("$tsName", IncrementalTsName);

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
        public void CanQueryIncrementalTimeSeriesAggregation_UsingLinq()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Oren",
                        Age = 35
                    }, "users/ayende");

                    var tsf = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);

                    tsf.Increment(baseline.AddMinutes(61), 59);
                    tsf.Increment(baseline.AddMinutes(62), 79);
                    tsf.Increment(baseline.AddMinutes(63), 69);

                    tsf.Increment(baseline.AddMonths(1).AddMinutes(61), 159);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(62), 179);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(63), 169);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Where(u => u.Age > 21)
                        .Select(u => RavenQuery.TimeSeries(u, IncrementalTsName)
                            .GroupBy("1 month")
                            .Select(g => new
                            {
                                Avg = g.Average(),
                                Max = g.Max()
                            })
                            .ToList());

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(6, result[0].Count);

                    var agg = result[0].Results;

                    Assert.Equal(2, agg.Length);

                    Assert.Equal(79, agg[0].Max[0]);
                    Assert.Equal(69, agg[0].Average[0]);

                    Assert.Equal(179, agg[1].Max[0]);
                    Assert.Equal(169, agg[1].Average[0]);
                }
            }
        }

        [Fact]
        public void CanQueryIncrementalTimeSeriesAggregation_FromLoadedDocument_UsingLinq()
        {
            var tsName = Constants.Headers.IncrementalTimeSeriesPrefix + "Stock";

            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    var companyId = "companies/1";
                    session.Store(new Person
                    {
                        Age = 25,
                        WorksAt = companyId
                    });
                    session.Store(new Company
                    {
                        Name = "HR",
                    }, companyId);

                    var tsf = session.IncrementalTimeSeriesFor(companyId, tsName);

                    tsf.Increment(baseline.AddMinutes(61), new[] { 12.59d });
                    tsf.Increment(baseline.AddMinutes(62), new[] { 12.79d });
                    tsf.Increment(baseline.AddMinutes(63), new[] { 15.69d }); 

                    tsf.Increment(baseline.AddMonths(1).AddMinutes(61), new[] { 13.59d });
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(62), new[] { 15.79d }); 
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(63), new[] { 13.69d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from p in session.Query<Person>()
                                where p.Age > 21
                                let company = RavenQuery.Load<Company>(p.WorksAt)
                                select RavenQuery.TimeSeries(company, tsName, baseline, baseline.AddMonths(2))
                                    .Where(ts => ts.Value < 15)
                                    .GroupBy("1 month")
                                    .Select(g => new
                                    {
                                        Avg = g.Average(),
                                        Max = g.Max()
                                    })
                                    .ToList();

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(4, result[0].Count);

                    var agg = result[0].Results;

                    Assert.Equal(2, agg.Length);

                    Assert.Equal(12.79, agg[0].Max[0]);
                    Assert.Equal(12.69, agg[0].Average[0]);

                    Assert.Equal(13.69, agg[1].Max[0]);
                    Assert.Equal(13.64, agg[1].Average[0]);
                }
            }
        }

        [Fact]
        public void CanQueryIncrementalTimeSeriesAggregation_StronglyTypedGroupBy_UsingLinq()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Oren",
                        Age = 35
                    }, "users/ayende");

                    var tsf = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);

                    tsf.Increment(baseline.AddMinutes(61), 59);
                    tsf.Increment(baseline.AddMinutes(62), 79);
                    tsf.Increment(baseline.AddMinutes(63), 69);

                    tsf.Increment(baseline.AddMonths(1).AddMinutes(61), 159);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(62), 179);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(63), 169);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Where(u => u.Age > 21)
                        .Select(u => RavenQuery.TimeSeries(u, IncrementalTsName)
                            .GroupBy(g => g.Months(1))
                            .Select(g => new
                            {
                                Avg = g.Average(),
                                Max = g.Max()
                            })
                            .ToList());

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(6, result[0].Count);

                    var agg = result[0].Results;

                    Assert.Equal(2, agg.Length);

                    Assert.Equal(79, agg[0].Max[0]);
                    Assert.Equal(69, agg[0].Average[0]);

                    Assert.Equal(179, agg[1].Max[0]);
                    Assert.Equal(169, agg[1].Average[0]);
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_SimpleProjectionToAnonymousClass_UsingLinq()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "ayende",
                        Age = 30
                    }, "users/ayende");

                    var tsf = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);

                    tsf.Increment(baseline.AddMinutes(61), 59);
                    tsf.Increment(baseline.AddMinutes(62), 49);
                    tsf.Increment(baseline.AddMinutes(63), 69);

                    tsf.Increment(baseline.AddMonths(1).AddMinutes(61), 39);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(62), 179);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(63), 169);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Where(u => u.Age > 21)
                        .Select(u => new
                        {
                            u.Name,
                            Heartrate = RavenQuery.TimeSeries(u, IncrementalTsName)
                                .Where(ts => ts.Value > 50)
                                .GroupBy("1 month")
                                .Select(g => new
                                {
                                    Avg = g.Average(),
                                    Max = g.Max(),
                                    Min = g.Min()
                                })
                                .ToList()
                        });

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal("ayende", result[0].Name);

                    Assert.Equal(4, result[0].Heartrate.Count);

                    var agg = result[0].Heartrate.Results;

                    Assert.Equal(2, agg.Length);

                    Assert.Equal(69, agg[0].Max[0]);
                    Assert.Equal(59, agg[0].Min[0]);
                    Assert.Equal(64, agg[0].Average[0]);

                    Assert.Equal(179, agg[1].Max[0]);
                    Assert.Equal(169, agg[1].Min[0]);
                    Assert.Equal(174, agg[1].Average[0]);
                }
            }
        }

        [Fact]
        public void CanQueryIncrementalTimeSeriesAggregation_MultipleSeries_UsingLinq()
        {
            var tsName2 = Constants.Headers.IncrementalTimeSeriesPrefix + "BloodPressure";
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "ayende"
                    }, "people/1");

                    var tsf = session.IncrementalTimeSeriesFor("people/1", IncrementalTsName);

                    tsf.Increment(baseline.AddMinutes(61), 59);
                    tsf.Increment(baseline.AddMinutes(62), 49);
                    tsf.Increment(baseline.AddMinutes(63), 69);

                    tsf.Increment(baseline.AddMonths(1).AddMinutes(61), 39);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(62), 179);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(63), 169);

                    tsf = session.IncrementalTimeSeriesFor("people/1", tsName2);

                    tsf.Increment(baseline.AddMinutes(61), 59);
                    tsf.Increment(baseline.AddMinutes(62), 79);
                    tsf.Increment(baseline.AddMinutes(63), 69);

                    tsf.Increment(baseline.AddMonths(1).AddMinutes(61), 159);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(62), 68);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(63), 58);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Select(p => new QueryResult
                        {
                            Id = p.Id,
                            Name = p.Name,
                            HeartRate = RavenQuery.TimeSeries(p, IncrementalTsName)
                                .Where(ts => ts.Value > 50)
                                .GroupBy(g => g.Months(1))
                                .Select(g => new
                                {
                                    Avg = g.Average(),
                                    Max = g.Max()
                                })
                                .ToList(),
                            BloodPressure = RavenQuery.TimeSeries(p, tsName2)
                                .Where(ts => ts.Value > 70)
                                .GroupBy(g => g.Months(1))
                                .Select(g => new
                                {
                                    Avg = g.Average(),
                                    Max = g.Max()
                                })
                                .ToList(),
                        });

                    var result = query.First();

                    Assert.Equal("ayende", result.Name);
                    Assert.Equal("people/1", result.Id);

                    Assert.Equal(4, result.HeartRate.Count);

                    var aggregation = result.HeartRate.Results;

                    Assert.Equal(2, aggregation.Length);

                    Assert.Equal(69, aggregation[0].Max[0]);
                    Assert.Equal(64, aggregation[0].Average[0]);

                    Assert.Equal(179, aggregation[1].Max[0]);
                    Assert.Equal(174, aggregation[1].Average[0]);

                    Assert.Equal(2, result.BloodPressure.Count);

                    aggregation = result.BloodPressure.Results;

                    Assert.Equal(2, aggregation.Length);

                    Assert.Equal(79, aggregation[0].Max[0]);
                    Assert.Equal(79, aggregation[0].Average[0]);

                    Assert.Equal(159, aggregation[1].Max[0]);
                    Assert.Equal(159, aggregation[1].Average[0]);
                }
            }
        }

        [Fact]
        public async Task CanQueryIncrementalTimeSeriesAggregation_DifferentNumberOfValues_UsingLinq()
        {
            using (var store = GetDocumentStore())
            {
                var id = "users/1";
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "karmel" }, id);

                    var tsf = session.IncrementalTimeSeriesFor(id, IncrementalTsName);
                    for (int i = 0; i < 100; i++)
                    {
                        tsf.Increment(baseline.AddDays(i), new[] { 1d, 2d, 3d });
                    }

                    for (int i = 100; i < 200; i++)
                    {
                        tsf.Increment(baseline.AddDays(i), new[] { 1d, 2d });
                    }

                    for (int i = 200; i < 300; i++)
                    {
                        tsf.Increment(baseline.AddDays(i), new[] { 1d, 2d, 4d });
                    }
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var ts = (await session.IncrementalTimeSeriesFor(id, IncrementalTsName)
                        .GetAsync(DateTime.MinValue, DateTime.MaxValue)).ToArray();

                    for (int i = 0; i < 100; i++)
                    {
                        var entry = ts[i];
                        Assert.Equal(baseline.AddDays(i), entry.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(3, entry.Values.Length);
                    }

                    for (int i = 100; i < 200; i++)
                    {
                        var entry = ts[i];
                        Assert.Equal(baseline.AddDays(i), entry.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(2, entry.Values.Length);
                    }

                    for (int i = 200; i < 300; i++)
                    {
                        var entry = ts[i];
                        Assert.Equal(baseline.AddDays(i), entry.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(3, entry.Values.Length);
                    }

                    var query = session.Query<User>()
                        .Where(u => u.Id == id)
                        .Statistics(out var stats)
                        .Select(u => RavenQuery.TimeSeries(u, IncrementalTsName, baseline, DateTime.MaxValue)
                            .GroupBy(g => g.Days(15))
                            .Select(g => new
                            {
                                Max = g.Max()
                            })
                            .ToList());
                    var result = await query.ToListAsync();
                    var results = result[0].Results;
                    Assert.Equal(20, results.Length);
                    Assert.Equal(3, results[0].Count.Length);
                    Assert.Equal(2, results[8].Count.Length);
                    Assert.Equal(3, results[19].Count.Length);
                }
            }
        }

        [Fact]
        public void CanQueryIncrementalTimeSeriesAggregation_WhereOnVariable_UsingLinq()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person(), "people/1");

                    var tsf = session.IncrementalTimeSeriesFor("people/1", IncrementalTsName);

                    tsf.Increment(baseline.AddMinutes(61), 59);
                    tsf.Increment(baseline.AddMinutes(62), 79);
                    tsf.Increment(baseline.AddMinutes(63), 69);

                    tsf.Increment(baseline.AddMonths(1).AddMinutes(61), 159);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(62), 179);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(63), 169);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    double val = 70;

                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, IncrementalTsName, baseline, baseline.AddMonths(2))
                            .Where(ts => ts.Values[0] > val)
                            .GroupBy(g => g.Months(1))
                            .Select(g => new
                            {
                                Avg = g.Average(),
                                Max = g.Max(),
                                Min = g.Min()
                            })
                            .ToList());

                    // should add 'val' as query parameter
                    Assert.Contains("Values[0] > $p0", query.ToString());

                    var result = query.First();

                    Assert.Equal(4, result.Count);

                    var agg = result.Results;

                    Assert.Equal(2, agg.Length);

                    Assert.Equal(79, agg[0].Max[0]);
                    Assert.Equal(79, agg[0].Average[0]);
                    Assert.Equal(79, agg[0].Min[0]);

                    Assert.Equal(179, agg[1].Max[0]);
                    Assert.Equal(169, agg[1].Average[0]);
                    Assert.Equal(159, agg[1].Min[0]);
                }
            }
        }

        [Fact]
        public void CanQueryIncrementalTimeSeriesAggregation_SelectSingleCall_UsingLinq()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Oren",
                        Age = 35
                    }, "users/ayende");

                    var tsf = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);

                    tsf.Increment(baseline.AddMinutes(61), 59);
                    tsf.Increment(baseline.AddMinutes(62), 79);
                    tsf.Increment(baseline.AddMinutes(63), 69);

                    tsf.Increment(baseline.AddMonths(1).AddMinutes(61), 159);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(62), 179);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(63), 169);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Where(u => u.Age > 21)
                        .Select(u => RavenQuery.TimeSeries(u, IncrementalTsName)
                            .Where(ts => ts.Value > 50)
                            .GroupBy(g => g.Months(1))
                            .Select(x => x.Max())
                            .ToList());

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(6, result[0].Count);

                    var agg = result[0].Results;

                    Assert.Equal(2, agg.Length);

                    Assert.Equal(79, agg[0].Max[0]);
                    Assert.Equal(179, agg[1].Max[0]);
                }
            }
        }

        [Fact]
        public void CanCallIncrementalTimeSeriesDeclaredFunctionFromJavascriptProjection()
        {
            const string tsName = Constants.Headers.IncrementalTimeSeriesPrefix + "Stocks";
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                        Age = 60,
                        WorksAt = "companies/1"
                    }, "people/1");

                    session.Store(new Company
                    {
                        Name = "HR"
                    }, "companies/1");

                    var tsf = session.IncrementalTimeSeriesFor("people/1", tsName);

                    tsf.Increment(baseline.AddMinutes(61), 59);
                    tsf.Increment(baseline.AddMinutes(62), 79);
                    tsf.Increment(baseline.AddMinutes(63), 69);

                    tsf.Increment(baseline.AddMonths(1).AddMinutes(61), 159);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(62), 179);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(63), 99);

                    tsf = session.IncrementalTimeSeriesFor("companies/1", tsName);

                    tsf.Increment(baseline.AddMinutes(61), 559);
                    tsf.Increment(baseline.AddMinutes(62), 99);
                    tsf.Increment(baseline.AddMinutes(63), 89);

                    tsf.Increment(baseline.AddMonths(1).AddMinutes(61), 659);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(62), 679);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(63), 79);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var rawQuery = session.Advanced.RawQuery<CustomRawQueryResult2>(
@"declare timeseries out(d) 
{
    from d.'INC:Stocks' between $start and $end
    where Value < 60 OR Value > 100
}
from People as p
where p.Age > 49
load p.WorksAt as Company
select {
    Series: out(p),
    Series2: out(Company),
    Series3: out(Company).Results.map(x=>x.Values[0]),
}")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddYears(1).EnsureUtc());

                    var result = rawQuery.First();

                    Assert.Equal(3, result.Series.Count);

                    Assert.Equal(new[] { 59d }, result.Series.Results[0].Values);
                    Assert.Equal(baseline.AddMinutes(61), result.Series.Results[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 159d }, result.Series.Results[1].Values);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), result.Series.Results[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 179d }, result.Series.Results[2].Values);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(62), result.Series.Results[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(3, result.Series2.Count);

                    Assert.Equal(new[] { 559d }, result.Series2.Results[0].Values);
                    Assert.Equal(baseline.AddMinutes(61), result.Series2.Results[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 659d }, result.Series2.Results[1].Values);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), result.Series2.Results[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 679d }, result.Series2.Results[2].Values);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(62), result.Series2.Results[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(3, result.Series3.Length);

                    Assert.Equal(559d, result.Series3[0]);
                    Assert.Equal(659d, result.Series3[1]);
                    Assert.Equal(679d, result.Series3[2]);

                }
            }
        }

        [Fact]
        public void CanPassIncrementalSeriesNameAsParameterToTimeSeriesDeclaredFunction()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                        Age = 60,
                        WorksAt = "companies/1"
                    }, "people/1");

                    session.Store(new Company
                    {
                        Name = "HR"
                    }, "companies/1");

                    var tsf = session.IncrementalTimeSeriesFor("people/1", IncrementalTsName);

                    tsf.Increment(baseline.AddMinutes(61), 59);
                    tsf.Increment(baseline.AddMinutes(62), 35);
                    tsf.Increment(baseline.AddMinutes(63), 45);

                    tsf.Increment(baseline.AddMonths(1).AddMinutes(61), 159);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(62), 179);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(63), 25);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var rawQuery = session.Advanced.RawQuery<CustomRawQueryResult3>(
@"declare timeseries out(name) 
{
    from name between $start and $end
    where Values[0] > 50
}
from People as p
select {
    HeartRate: out('INC:Heartrate')
}")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddYears(1).EnsureUtc());

                    var result = rawQuery.First();

                    Assert.Equal(3, result.HeartRate.Count);

                    Assert.Equal(new[] { 59d }, result.HeartRate.Results[0].Values);
                    Assert.Equal(baseline.AddMinutes(61), result.HeartRate.Results[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 159d }, result.HeartRate.Results[1].Values);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), result.HeartRate.Results[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 179d }, result.HeartRate.Results[2].Values);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(62), result.HeartRate.Results[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void CanUseIncrementalTimeSeriesQueryResultAsArgumentToJavascriptDeclaredFunction()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                        Age = 30,
                    }, "people/1");

                    var tsf = session.IncrementalTimeSeriesFor("people/1", IncrementalTsName);

                    tsf.Increment(baseline.AddMinutes(61), 59);
                    tsf.Increment(baseline.AddMinutes(62), 79);
                    tsf.Increment(baseline.AddMinutes(63), 69);

                    tsf.Increment(baseline.AddMonths(1).AddMinutes(61), 159);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(62), 179);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(63), 169);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var rawQuery = session.Advanced.RawQuery<CustomRawQueryResult>(
@"declare function foo(tsResult) {
    var arr = tsResult.Results;
    var mid = arr.length / 2; 
    return {
        Value: arr[mid].Values[0],
        Mid : mid,
        Count : tsResult.Count                
    };
}
declare timeseries heartrate(doc){
    from doc.'INC:Heartrate' between $start and $end
}
from People as p
where p.Age > 21
select foo(heartrate(p))
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddYears(1).EnsureUtc());

                    var result = rawQuery.First();

                    Assert.Equal(6, result.Count);
                    Assert.Equal(3, result.Mid);
                    Assert.Equal(159d, result.Value);
                }
            }
        }

        [Fact]
        public void CanCallIncrementalTimeSeriesDeclaredFunctionFromJavascriptDeclaredFunction()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                        Age = 60,
                        WorksAt = "companies/1"
                    }, "people/1");

                    session.Store(new Company
                    {
                        Name = "HR"
                    }, "companies/1");

                    var tsf = session.IncrementalTimeSeriesFor("people/1", IncrementalTsName);

                    tsf.Increment(baseline.AddMinutes(61), new[] { 59d });
                    tsf.Increment(baseline.AddMinutes(62), new[] { 100d });
                    tsf.Increment(baseline.AddMinutes(63), new[] { 100d });

                    tsf.Increment(baseline.AddMonths(1).AddMinutes(61), new[] { 159d });
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(62), new[] { 179d });
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(63), new[] { 100d });

                    tsf = session.IncrementalTimeSeriesFor("people/1", "INC:Stocks");

                    tsf.Increment(baseline.AddMinutes(61), new[] { 559d });
                    tsf.Increment(baseline.AddMinutes(62), new[] { 100d });
                    tsf.Increment(baseline.AddMinutes(63), new[] { 100d });

                    tsf.Increment(baseline.AddMonths(1).AddMinutes(61), new[] { 659d });
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(62), new[] { 679d });
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(63), new[] { 100d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var rawQuery = session.Advanced.RawQuery<CustomRawQueryResult3>(
@"declare timeseries ts(name) 
{
    from name between $start and $end
    where Value != 100
}
declare function out(d) 
{
    var result = {};
    var allTsNames = d['@metadata']['@timeseries'];
    for (var i = 0; i < allTsNames.length; i++){
        var name = allTsNames[i];
        var nameWithoutPrefix = name.substr(4);
        result[nameWithoutPrefix] = ts(name);
    }
    return result;    
}
from People as p
where p.Age > 49
select out(p)")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddYears(1).EnsureUtc());

                    var result = rawQuery.First();

                    Assert.Equal(3, result.HeartRate.Count);

                    Assert.Equal(new[] { 59d }, result.HeartRate.Results[0].Values);
                    Assert.Equal(baseline.AddMinutes(61), result.HeartRate.Results[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 159d }, result.HeartRate.Results[1].Values);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), result.HeartRate.Results[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 179d }, result.HeartRate.Results[2].Values);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(62), result.HeartRate.Results[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(3, result.Stocks.Count);

                    Assert.Equal(new[] { 559d }, result.Stocks.Results[0].Values);
                    Assert.Equal(baseline.AddMinutes(61), result.Stocks.Results[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 659d }, result.Stocks.Results[1].Values);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), result.Stocks.Results[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 679d }, result.Stocks.Results[2].Values);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(62), result.Stocks.Results[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                }
            }
        }

        [Fact]
        public void IncrementalTimeSeriesAggregationInsideJsProjection_UsingLinq_FromLoadedDocument()
        {
            const string tsName = Constants.Headers.IncrementalTimeSeriesPrefix + "Stock";
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    var companyId = "companies/1";
                    session.Store(new Person
                    {
                        Name = "Oren",
                        LastName = "Ayende",
                        Age = 30,
                        WorksAt = companyId
                    });
                    session.Store(new Company
                    {
                        Name = "HR",
                    }, companyId);

                    var tsf = session.IncrementalTimeSeriesFor(companyId, tsName);

                    tsf.Increment(baseline.AddMinutes(61), new[] { 12.59d });
                    tsf.Increment(baseline.AddMinutes(62), new[] { 12.79d });
                    tsf.Increment(baseline.AddMinutes(63), new[] { 11.69d });

                    tsf.Increment(baseline.AddMonths(1).AddMinutes(61), new[] { 13.59d });
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(62), new[] { 10.79d }); 
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(63), new[] { 13.69d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from p in session.Query<Person>()
                                where p.Age > 21
                                let company = RavenQuery.Load<Company>(p.WorksAt)
                                select new
                                {
                                    Heartrate = RavenQuery.TimeSeries(company, tsName, baseline, baseline.AddMonths(2))
                                        .Where(ts => ts.Value > 12)
                                        .GroupBy(g => g.Months(1))
                                        .Select(g => new
                                        {
                                            Avg = g.Average(),
                                            Max = g.Max()
                                        })
                                        .ToList(),
                                    Name = p.Name + " " + p.LastName // creates a js projection
                                };

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal("Oren Ayende", result[0].Name);

                    Assert.Equal(4, result[0].Heartrate.Count);

                    var agg = result[0].Heartrate.Results;

                    Assert.Equal(2, agg.Length);

                    Assert.Equal(12.79, agg[0].Max[0]);
                    Assert.Equal(12.69, agg[0].Average[0]);

                    Assert.Equal(13.69, agg[1].Max[0]);
                    Assert.Equal(13.64, agg[1].Average[0]);
                }
            }
        }

        [Fact]
        public void IncrementalTimeSeriesAggregationInsideJsProjection_UsingLinq_CanDefineTmeSeriesInsideLet()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                        LastName = "Ayende",
                        Age = 30,
                    }, "people/1");

                    var tsf = session.IncrementalTimeSeriesFor("people/1", IncrementalTsName);

                    tsf.Increment(baseline.AddMinutes(61), 59);
                    tsf.Increment(baseline.AddMinutes(62), 79);
                    tsf.Increment(baseline.AddMinutes(63), 25);

                    tsf.Increment(baseline.AddMonths(1).AddMinutes(61), 35);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(62), 179);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(63), 45);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from p in session.Query<Person>()
                                where p.Age > 21
                                let heartrate = RavenQuery.TimeSeries(p, IncrementalTsName, baseline, baseline.AddMonths(2))
                                    .Where(ts => ts.Value > 50)
                                    .GroupBy(g => g.Months(1))
                                    .Select(g => new
                                    {
                                        Avg = g.Average(),
                                        Max = g.Max()
                                    })
                                    .ToList()
                                select new
                                {
                                    Heartrate = heartrate,
                                    Name = p.Name + " " + p.LastName
                                };

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal("Oren Ayende", result[0].Name);

                    Assert.Equal(3, result[0].Heartrate.Count);

                    var agg = result[0].Heartrate.Results;

                    Assert.Equal(2, agg.Length);

                    Assert.Equal(79, agg[0].Max[0]);
                    Assert.Equal(69, agg[0].Average[0]);

                    Assert.Equal(179, agg[1].Max[0]);
                    Assert.Equal(179, agg[1].Average[0]);

                }
            }
        }

        [Fact]
        public void IncrementalTimeSeriesAggregationInsideJsProjection_UsingLinq_WhenTsQueryExpressionIsNestedInsideAnotherExpression()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                        LastName = "Ayende",
                        Age = 30,
                    }, "people/1");

                    var tsf = session.IncrementalTimeSeriesFor("people/1", IncrementalTsName);

                    tsf.Increment(baseline.AddMinutes(61), 59);
                    tsf.Increment(baseline.AddMinutes(62), 79);
                    tsf.Increment(baseline.AddMinutes(63), 69);

                    tsf.Increment(baseline.AddMonths(1).AddMinutes(61), 159);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(62), 179);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(63), 169);

                    tsf = session.IncrementalTimeSeriesFor("people/1", "INC:Stocks");

                    tsf.Increment(baseline.AddMinutes(61), 559);
                    tsf.Increment(baseline.AddMinutes(62), 579);
                    tsf.Increment(baseline.AddMinutes(63), 569);

                    tsf.Increment(baseline.AddMonths(1).AddMinutes(61), 659);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(62), 679);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(63), 669);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from p in session.Query<Person>()
                                where p.Age > 21
                                let tsFunc = new Func<string, TimeSeriesEntry[]>(name =>
                                    RavenQuery.TimeSeries(p, name, baseline, baseline.AddMonths(2))
                                        .Where(ts => ts.Values[0] > 100 && ts.Values[0] < 600)
                                        .ToList()
                                        .Results)
                                select new
                                {
                                    Name = p.Name + " " + p.LastName,
                                    Heartrate = tsFunc("INC:Heartrate"),
                                    Stocks = tsFunc("INC:Stocks")
                                };

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal("Oren Ayende", result[0].Name);

                    var heartrate = result[0].Heartrate;

                    Assert.Equal(3, heartrate.Length);

                    Assert.Equal(159, heartrate[0].Value);
                    Assert.Equal(179, heartrate[1].Value);
                    Assert.Equal(169, heartrate[2].Value);

                    var stocks = result[0].Stocks;

                    Assert.Equal(3, stocks.Length);

                    Assert.Equal(559, stocks[0].Value);
                    Assert.Equal(579, stocks[1].Value);
                    Assert.Equal(569, stocks[2].Value);

                }
            }
        }

        [Fact]
        public void CanDefineCustomJsFunctionThatHasIncrementalTimeSeriesCall_UsingLinq()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                        LastName = "Ayende",
                        Age = 30
                    }, "people/1");

                    var tsf = session.IncrementalTimeSeriesFor("people/1", IncrementalTsName);

                    tsf.Increment(baseline.AddMinutes(61), 59);
                    tsf.Increment(baseline.AddMinutes(62), 79);
                    tsf.Increment(baseline.AddMinutes(63), 69);

                    tsf.Increment(baseline.AddMonths(1).AddMinutes(61), 159);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(62), 179);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(63), 169);

                    tsf.Increment(baseline.AddMonths(1).AddMinutes(61), 259);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(62), 279);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(63), 269);

                    tsf = session.IncrementalTimeSeriesFor("people/1", "INC:Stocks");

                    tsf.Increment(baseline.AddMinutes(61), 559);
                    tsf.Increment(baseline.AddMinutes(62), 579);
                    tsf.Increment(baseline.AddMinutes(63), 569);

                    tsf.Increment(baseline.AddMonths(1).AddMinutes(61), 659);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(62), 679);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(63), 669);

                    tsf.Increment(baseline.AddMonths(3).AddMinutes(61), 459);
                    tsf.Increment(baseline.AddMonths(3).AddMinutes(62), 479);
                    tsf.Increment(baseline.AddMonths(3).AddMinutes(63), 469);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from p in session.Query<Person>()
                                where p.Age > 21
                                let tsFunc = new Func<object, string, DateTime, DateTime, TimeSeriesEntry[]>((o, name, f, t) =>
                                    RavenQuery.TimeSeries(o, name, f, t)
                                        .Where(ts => ts.Values[0] > 100 && ts.Values[0] < 600)
                                        .ToList()
                                        .Results)
                                select new
                                {
                                    Name = p.Name + " " + p.LastName,
                                    Heartrate = tsFunc(p, IncrementalTsName, baseline, baseline.AddMonths(2)),
                                    Stocks = tsFunc(p, "INC:Stocks", baseline, baseline.AddMonths(4))

                                };

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal("Oren Ayende", result[0].Name);

                    var heartrate = result[0].Heartrate;

                    Assert.Equal(3, heartrate.Length);

                    Assert.Equal(159, heartrate[0].Value);
                    Assert.Equal(179, heartrate[1].Value);
                    Assert.Equal(169, heartrate[2].Value);

                    var stocks = result[0].Stocks;

                    Assert.Equal(6, stocks.Length);

                    Assert.Equal(559, stocks[0].Value);
                    Assert.Equal(579, stocks[1].Value);
                    Assert.Equal(569, stocks[2].Value);
                    Assert.Equal(459, stocks[3].Value);
                    Assert.Equal(479, stocks[4].Value);
                    Assert.Equal(469, stocks[5].Value);

                }
            }
        }

        [Fact]
        public void CanUseSlopeInIncrementalTimeSeriesQuery_Linq()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                var id = "people/1";

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.IncrementalTimeSeriesFor(id, IncrementalTsName);

                    for (int i = 0; i < TimeSpan.FromDays(1).TotalMinutes; i++)
                    {
                        tsf.Increment(baseline.AddMinutes(i), i * 100);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, IncrementalTsName)
                            .GroupBy(g => g.Hours(1))
                            .Select(x => new
                            {
                                Slope = x.Slope()
                            })
                            .ToList());

                    var result = query.First();
                    Assert.Equal(24, result.Results.Length);

                    var dx = TimeSpan.FromMinutes(60).TotalMilliseconds;
                    var dy = 5900;
                    var expected = dy / dx;
                    foreach (var rangeAggregation in result.Results)
                    {
                        Assert.Equal(expected, rangeAggregation.Slope[0]);
                    }
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, IncrementalTsName)
                            .GroupBy(g => g.Hours(1))
                            .Select(x => x.Slope())
                            .ToList());

                    var result = query.First();
                    Assert.Equal(24, result.Results.Length);

                    var dx = TimeSpan.FromMinutes(60).TotalMilliseconds;
                    var dy = 5900;
                    var expected = dy / dx;
                    foreach (var rangeAggregation in result.Results)
                    {
                        Assert.Equal(expected, rangeAggregation.Slope[0]);
                    }
                }
            }
        }

        [Fact] 
        public void CanUsePercentileInIncrementalTimeSeriesQuery_Linq()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                var id = "people/1";
                const double number = 90;

                var values = new List<double>
                {
                    43, 54, 56, 61, 62,
                    66, 68, 69, 69, 70,
                    71, 72, 77, 78, 79,
                    85, 87, 88, 89, 93,
                    95, 96, 98, 99, 99
                };

                double expected = GetExpectedPercentile(values, number);

                using (var session = store.OpenSession())
                {
                    session.Store(new Person { Name = "Oren", }, id);

                    var tsf = session.IncrementalTimeSeriesFor(id, IncrementalTsName);

                    var count = values.Count;
                    var rand = new Random();

                    for (int i = 0; i < count; i++)
                    {
                        var index = rand.Next(0, values.Count - 1);

                        tsf.Increment(baseline.AddHours(i), values[index]);

                        values.RemoveAt(index);
                    }

                    session.SaveChanges();
                }


                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, IncrementalTsName)
                            .Select(x => new
                            {
                                P = x.Percentile(number)
                            })
                            .ToList());

                    var result = query.First();
                    Assert.Equal(1, result.Results.Length);

                    Assert.Equal(expected, result.Results[0].Percentile[0]);
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, IncrementalTsName)
                            .Select(x => x.Percentile(number))
                            .ToList());

                    var result = query.First();
                    Assert.Equal(1, result.Results.Length);

                    Assert.Equal(expected, result.Results[0].Percentile[0]);
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, IncrementalTsName)
                            .Select(x => new TsResult
                            {
                                Count = x.Count(),
                                Percentile = x.Percentile(number)
                            })
                            .ToList());

                    var result = query.First();
                    Assert.Equal(1, result.Results.Length);

                    Assert.Equal(expected, result.Results[0].Percentile[0]);
                }
            }
        }

        [Fact] 
        public void CanUseStdDevInIncrementalTimeSeriesQuery_Linq()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                var id = "people/1";

                var values = new List<double>
                {
                    43, 54, 56, 61, 62,
                    66, 68, 69, 69, 70,
                    71, 72, 77, 78, 79,
                    85, 87, 88, 89, 93,
                    95, 96, 98, 99, 99
                };

                using (var session = store.OpenSession())
                {
                    session.Store(new Raven.Tests.Core.Utils.Entities.Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.IncrementalTimeSeriesFor(id, IncrementalTsName);

                    var count = values.Count;
                    var rand = new Random();

                    for (int i = 0; i < count; i++)
                    {
                        var index = rand.Next(0, values.Count - 1);

                        tsf.Increment(baseline.AddHours(i), values[index]);

                        values.RemoveAt(index);
                    }

                    session.SaveChanges();
                }

                var allValues = store.Operations
                    .Send(new GetTimeSeriesOperation(id, IncrementalTsName))
                    .Entries
                    .Select(entry => entry.Value)
                    .ToList();

                var mean = allValues.Average();
                var sigma = allValues.Sum(v => Math.Pow(v - mean, 2));
                var expected = Math.Sqrt(sigma / (allValues.Count - 1));

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Raven.Tests.Core.Utils.Entities.Person>()
                        .Select(p => RavenQuery.TimeSeries(p, IncrementalTsName)
                            .Select(x => new
                            {
                                StdDev = x.StandardDeviation()
                            })
                            .ToList());

                    var result = query.First();

                    Assert.Equal(1, result.Results.Length);
                    Assert.True(AlmostEquals(expected, result.Results[0].StandardDeviation[0]));
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Raven.Tests.Core.Utils.Entities.Person>()
                        .Select(p => RavenQuery.TimeSeries(p, IncrementalTsName)
                            .Select(x => x.StandardDeviation())
                            .ToList());

                    var result = query.First();
                    Assert.Equal(1, result.Results.Length);
                    Assert.True(AlmostEquals(expected, result.Results[0].StandardDeviation[0]));
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Raven.Tests.Core.Utils.Entities.Person>()
                        .Select(p => RavenQuery.TimeSeries(p, IncrementalTsName)
                            .Select(x => new TsResult
                            {
                                Count = x.Count(),
                                StdDev = x.StandardDeviation()
                            })
                            .ToList());

                    var result = query.First();
                    Assert.Equal(1, result.Results.Length);
                    Assert.True(AlmostEquals(expected, result.Results[0].StandardDeviation[0]));
                }
            }
        }

        [Fact]
        public void IncrementalTimeSeriesQuery_CanFillGaps_LinearInterpolation()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                string id = "people/1";

                using (var session = store.OpenSession())
                {
                    session.Store(new Person(), id);

                    var tsf = session.IncrementalTimeSeriesFor(id, IncrementalTsName);

                    tsf.Increment(baseline, 50);
                    tsf.Increment(baseline.AddHours(1), 60);
                    tsf.Increment(baseline.AddHours(4), 90);
                    tsf.Increment(baseline.AddHours(5), 100);

                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Where(p => p.Id == id)
                        .Select(p => RavenQuery.TimeSeries(p, IncrementalTsName, baseline, baseline.AddDays(1))
                            .GroupBy(g => g
                                .Hours(1)
                                .WithOptions(new TimeSeriesAggregationOptions
                                {
                                    Interpolation = InterpolationType.Linear
                                }))
                            .Select(x => x.Max())
                            .ToList());

                    var result = query.First();

                    Assert.Equal(6, result.Results.Length);

                    var aggResult = result.Results[0];
                    Assert.Equal(baseline, aggResult.From);
                    Assert.Equal(baseline.AddHours(1), aggResult.To);
                    Assert.Equal(50, aggResult.Max[0]);

                    aggResult = result.Results[1];
                    Assert.Equal(baseline.AddHours(1), aggResult.From);
                    Assert.Equal(baseline.AddHours(2), aggResult.To);
                    Assert.Equal(60, aggResult.Max[0]);

                    aggResult = result.Results[2];
                    Assert.Equal(baseline.AddHours(2), aggResult.From);
                    Assert.Equal(baseline.AddHours(3), aggResult.To);
                    Assert.Equal(70, aggResult.Max[0]);

                    aggResult = result.Results[3];
                    Assert.Equal(baseline.AddHours(3), aggResult.From);
                    Assert.Equal(baseline.AddHours(4), aggResult.To);
                    Assert.Equal(80, aggResult.Max[0]);

                    aggResult = result.Results[4];
                    Assert.Equal(baseline.AddHours(4), aggResult.From);
                    Assert.Equal(baseline.AddHours(5), aggResult.To);
                    Assert.Equal(90, aggResult.Max[0]);

                    aggResult = result.Results[5];
                    Assert.Equal(baseline.AddHours(5), aggResult.From);
                    Assert.Equal(baseline.AddHours(6), aggResult.To);
                    Assert.Equal(100, aggResult.Max[0]);
                }
            }
        }

        [Fact]
        public void IncrementalTimeSeriesDocumentQuery()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Oren",
                        Age = 35
                    }, "users/ayende");

                    var tsf = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                    tsf.Increment(baseline.AddMinutes(61), 59);
                    tsf.Increment(baseline.AddMinutes(62), 279); 
                    tsf.Increment(baseline.AddMinutes(63), 69);

                    tsf.Increment(baseline.AddMonths(1).AddMinutes(61), 259);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(62), 279);
                    tsf.Increment(baseline.AddMonths(1).AddMinutes(63), 169);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<User>()
                        .WhereGreaterThan(u => u.Age, 21)
                        .SelectTimeSeries(builder => builder
                            .From(IncrementalTsName)
                            .Between(baseline, baseline.AddMonths(3))
                            .Where(x => x.Value < 200)
                            .GroupBy(g => g.Hours(1))
                            .Select(x => x.Max())
                            .ToList());

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(3, result[0].Count);

                    var agg = result[0].Results;

                    Assert.Equal(2, agg.Length);
                    Assert.Equal(69, agg[0].Max[0]);
                    Assert.Equal(169, agg[1].Max[0]);
                }
            }
        }

        [Fact]
        public void CanQueryIncrementalTimeSeries_MultipleIncrementsOnSameTimestamp()
        {
            const string id = "users/ayende";

            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Oren",
                        Age = 35
                    }, id);

                    session.SaveChanges();
                }

                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        var tsf = session.IncrementalTimeSeriesFor(id, IncrementalTsName);

                        for (int j = 0; j < 10; j++)
                        {
                            tsf.Increment(baseline.AddMinutes(j), 1);
                        }

                        session.SaveChanges();
                    }
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Where(u => u.Age > 21)
                        .Select(u => RavenQuery.TimeSeries(u, IncrementalTsName).ToList());

                    var result = query.First();

                    Assert.Equal(10, result.Count);

                    for (var i = 0; i < result.Results.Length; i++)
                    {
                        var entry = result.Results[i];
                        Assert.Equal(baseline.AddMinutes(i), entry.Timestamp);
                        Assert.Equal(10, entry.Value);
                    }
                }
            }
        }

        [Fact]
        public async Task CanQueryIncrementalTimeSeriesRawValues_WithDuplicateTimestamps()
        {
            const string id = "users/ayende";

            using (var store = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await SetupReplicationAsync(store, store2);
                await SetupReplicationAsync(store2, store);
                await EnsureReplicatingAsync(store, store2);
                await EnsureReplicatingAsync(store2, store);

                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Oren",
                        Age = 35
                    }, id);

                    session.SaveChanges();
                }

                WaitForDocument<User>(store2, id, u => u.Name == "Oren");

                foreach (var s in new [] { store, store2})
                {
                    for (int i = 0; i < 10; i++)
                    {
                        using (var session = s.OpenSession())
                        {
                            var tsf = session.IncrementalTimeSeriesFor(id, IncrementalTsName);

                            for (int j = 0; j < 10; j++)
                            {
                                tsf.Increment(baseline.AddMinutes(j), 1);
                            }

                            session.SaveChanges();
                        }
                    }
                }

                await EnsureReplicatingAsync(store, store2);
                await EnsureReplicatingAsync(store2, store);

                foreach (var s in new[] { store, store2 })
                {
                    var ts = s.Operations.Send(new GetTimeSeriesOperation(id, IncrementalTsName, returnFullResults: true));
                    Assert.Equal(10, ts.Entries.Length);

                    foreach (var entry in ts.Entries)
                    {
                        Assert.Equal(20, entry.Value);
                        Assert.Equal(2, entry.NodeValues.Count);

                        foreach (var nodeVal in entry.NodeValues)
                        {
                            Assert.Equal(10, nodeVal.Value[0]);
                        }
                    }
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Where(u => u.Age > 21)
                        .Select(u => RavenQuery.TimeSeries(u, IncrementalTsName).ToList());

                    var result = query.First();

                    Assert.Equal(10, result.Count);

                    for (var i = 0; i < result.Results.Length; i++)
                    {
                        var entry = result.Results[i];
                        Assert.Equal(baseline.AddMinutes(i), entry.Timestamp);
                        Assert.Equal(20, entry.Value);
                    }
                }
            }
        }

        [Fact]
        public async Task CanQueryIncrementalTimeSeriesAggregation_WithDuplicateTimestamps()
        {
            const string id = "users/ayende";

            using (var store = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await SetupReplicationAsync(store, store2);
                await SetupReplicationAsync(store2, store);
                await EnsureReplicatingAsync(store, store2);
                await EnsureReplicatingAsync(store2, store);

                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Oren",
                        Age = 35
                    }, id);

                    session.SaveChanges();
                }

                WaitForDocument<User>(store2, id, u => u.Name == "Oren");

                foreach (var s in new[] { store, store2 })
                {
                    for (int i = 0; i < 10; i++)
                    {
                        using (var session = s.OpenSession())
                        {
                            var tsf = session.IncrementalTimeSeriesFor(id, IncrementalTsName);

                            for (int j = 0; j < 10; j++)
                            {
                                tsf.Increment(baseline.AddMinutes(j), j);
                                tsf.Increment(baseline.AddMonths(1).AddMinutes(j), j);
                            }

                            session.SaveChanges();
                        }
                    }
                }

                await EnsureReplicatingAsync(store, store2);
                await EnsureReplicatingAsync(store2, store);

                foreach (var s in new[] { store, store2 })
                {
                    var ts = s.Operations.Send(new GetTimeSeriesOperation(id, IncrementalTsName, returnFullResults: true));
                    Assert.Equal(20, ts.Entries.Length);

                    for (int k = 0; k < 2; k++)
                    {
                        for (var i = 0; i < 10; i++)
                        {
                            var entry = ts.Entries[i + (10 * k)];
                            Assert.Equal(20 * i, entry.Value);
                            Assert.Equal(2, entry.NodeValues.Count);

                            foreach (var nodeVal in entry.NodeValues)
                            {
                                Assert.Equal(entry.Value / 2, nodeVal.Value[0]);
                            }
                        }
                    }
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Where(u => u.Age > 21)
                        .Select(u => RavenQuery.TimeSeries(u, IncrementalTsName)
                            .GroupBy(g => g.Months(1))
                            .ToList());

                    var result = query.First();

                    Assert.Equal(20, result.Count);

                    Assert.Equal(2, result.Results.Length);

                    foreach (var aggregation in result.Results)
                    {
                        Assert.Equal(90, aggregation.Average[0]);
                        Assert.Equal(10, aggregation.Count[0]);
                        Assert.Equal(0, aggregation.First[0]);
                        Assert.Equal(180, aggregation.Last[0]);
                        Assert.Equal(0, aggregation.Min[0]);
                        Assert.Equal(180, aggregation.Max[0]);
                        Assert.Equal(900, aggregation.Sum[0]);
                    }
                }
            }
        }
    }
}
