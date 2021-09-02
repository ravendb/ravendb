using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Query
{
    public class TimeSeriesJavascriptProjections : RavenTestBase
    {
        public TimeSeriesJavascriptProjections(ITestOutputHelper output) : base(output)
        {
        }

        private class Person
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public string LastName { get; set; }

            public int Age { get; set; }

            public string WorksAt { get; set; }
        }

        private class Watch
        {
            public string Manufacturer { get; set; }

            public double Accuracy { get; set; }

        }

        private class QueryResult
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public TimeSeriesAggregationResult HeartRate { get; set; }

            public TimeSeriesAggregationResult BloodPressure { get; set; }
        }


        private class CustomRawQueryResult
        {
            public double Value { get; set; }

            public string Tag { get; set; }

            public long Count { get; set; }

            public long Mid { get; set;  }
        }


        private class CustomRawQueryResult2
        {
            public BMP[] HeartRate { get; set; }

            public long Count { get; set; }

        }

        private class BMP
        {
            public double Max { get; set; }

            public double Avg { get; set; }
        }


        private class CustomRawQueryResult3
        {
            public TimeSeriesRawResult Series { get; set; }

            public TimeSeriesRawResult Series2 { get; set; }

            public double[]  Series3 { get; set; }
        }

        private class CustomRawQueryResult4
        {
            public TimeSeriesRawResult Heartrate { get; set; }

            public TimeSeriesRawResult Stocks { get; set; }
        }

        private class CustomJsFunctionResult
        {
            public double Max { get; set; }

            public bool HasApple { get; set; }

            public List<double> Accuracies { get; set; }
        }

        internal class CustomJsFunctionResult2
        {
            public double TotalMax { get; set; }

            public double TotalMin { get; set; }

            public double AvgOfAvg { get; set; }

            public double MaxGroupSize { get; set;  }
        }


        [Fact]
        public void CanCallTimeSeriesDeclaredFunctionFromJavascriptProjection()
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

                    var tsf = session.TimeSeriesFor("people/1", "Stocks");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "tags/1");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "tags/2");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "tags/2");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "tags/2");

                    tsf = session.TimeSeriesFor("companies/1", "Stocks");

                    tsf.Append(baseline.AddMinutes(61), new[] { 559d }, "tags/1");
                    tsf.Append(baseline.AddMinutes(62), new[] { 579d }, "tags/2");
                    tsf.Append(baseline.AddMinutes(63), new[] { 569d }, "tags/2");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 659d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 679d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 669d }, "tags/2");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var rawQuery = session.Advanced.RawQuery<CustomRawQueryResult3>(
@"declare timeseries out(d) 
{
    from d.Stocks between $start and $end
    where Tag != 'tags/2'
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
                    Assert.Equal("tags/1", result.Series.Results[0].Tag);
                    Assert.Equal(baseline.AddMinutes(61), result.Series.Results[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 159d }, result.Series.Results[1].Values);
                    Assert.Equal("tags/1", result.Series.Results[1].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), result.Series.Results[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 179d }, result.Series.Results[2].Values);
                    Assert.Equal("tags/1", result.Series.Results[2].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(62), result.Series.Results[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(3, result.Series2.Count);

                    Assert.Equal(new[] { 559d }, result.Series2.Results[0].Values);
                    Assert.Equal("tags/1", result.Series2.Results[0].Tag);
                    Assert.Equal(baseline.AddMinutes(61), result.Series2.Results[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 659d }, result.Series2.Results[1].Values);
                    Assert.Equal("tags/1", result.Series2.Results[1].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), result.Series2.Results[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 679d }, result.Series2.Results[2].Values);
                    Assert.Equal("tags/1", result.Series2.Results[2].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(62), result.Series2.Results[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(3, result.Series3.Length);

                    Assert.Equal(559d, result.Series3[0]);
                    Assert.Equal(659d, result.Series3[1]);
                    Assert.Equal(679d, result.Series3[2]);

                }
            }
        }

        [Fact]
        public void CanCallTimeSeriesDeclaredFunctionFromJavascriptProjection_TagAsParameter()
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

                    var tsf = session.TimeSeriesFor("people/1", "Stocks");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "tags/1");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "tags/2");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "tags/2");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "tags/2");

                    tsf = session.TimeSeriesFor("companies/1", "Stocks");

                    tsf.Append(baseline.AddMinutes(61), new[] { 559d }, "tags/1");
                    tsf.Append(baseline.AddMinutes(62), new[] { 579d }, "tags/2");
                    tsf.Append(baseline.AddMinutes(63), new[] { 569d }, "tags/3");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 659d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 679d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 669d }, "tags/2");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var rawQuery = session.Advanced.RawQuery<CustomRawQueryResult3>(
@"declare timeseries out(d, t) 
{
    from d.Stocks between $start and $end
    where Tag != t
}
from People as p
where p.Age > 49
load p.WorksAt as Company
select {
    Series: out(p, 'tags/2'),
    Series2: out(Company, 'tags/3')
}")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddYears(1).EnsureUtc());

                    var result = rawQuery.First();

                    Assert.Equal(3, result.Series.Count);

                    Assert.Equal(new[] { 59d }, result.Series.Results[0].Values);
                    Assert.Equal("tags/1", result.Series.Results[0].Tag);
                    Assert.Equal(baseline.AddMinutes(61), result.Series.Results[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 159d }, result.Series.Results[1].Values);
                    Assert.Equal("tags/1", result.Series.Results[1].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), result.Series.Results[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 179d }, result.Series.Results[2].Values);
                    Assert.Equal("tags/1", result.Series.Results[2].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(62), result.Series.Results[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(5, result.Series2.Count);

                    Assert.Equal(new[] { 559d }, result.Series2.Results[0].Values);
                    Assert.Equal("tags/1", result.Series2.Results[0].Tag);
                    Assert.Equal(baseline.AddMinutes(61), result.Series2.Results[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 579d }, result.Series2.Results[1].Values);
                    Assert.Equal("tags/2", result.Series2.Results[1].Tag);
                    Assert.Equal(baseline.AddMinutes(62), result.Series2.Results[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 659d }, result.Series2.Results[2].Values);
                    Assert.Equal("tags/1", result.Series2.Results[2].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), result.Series2.Results[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 679d }, result.Series2.Results[3].Values);
                    Assert.Equal("tags/1", result.Series2.Results[3].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(62), result.Series2.Results[3].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 669d }, result.Series2.Results[4].Values);
                    Assert.Equal("tags/2", result.Series2.Results[4].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(63), result.Series2.Results[4].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                }
            }
        }


        [Fact]
        public void CanPassSeriesNameAsParameterToTimeSeriesDeclaredFunction()
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

                    var tsf = session.TimeSeriesFor("people/1", "Heartrate");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "tags/1");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "tags/2");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "tags/2");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "tags/2");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var rawQuery = session.Advanced.RawQuery<CustomRawQueryResult4>(
@"declare timeseries out(name) 
{
    from name between $start and $end
    where Tag != 'tags/2'
}
from People as p
select {
    Heartrate: out('Heartrate')
}")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddYears(1).EnsureUtc());

                    var result = rawQuery.First();

                    Assert.Equal(3, result.Heartrate.Count);

                    Assert.Equal(new[] { 59d }, result.Heartrate.Results[0].Values);
                    Assert.Equal("tags/1", result.Heartrate.Results[0].Tag);
                    Assert.Equal(baseline.AddMinutes(61), result.Heartrate.Results[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 159d }, result.Heartrate.Results[1].Values);
                    Assert.Equal("tags/1", result.Heartrate.Results[1].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), result.Heartrate.Results[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 179d }, result.Heartrate.Results[2].Values);
                    Assert.Equal("tags/1", result.Heartrate.Results[2].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(62), result.Heartrate.Results[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void CanPassSeriesNameAsParameterToTimeSeriesDeclaredFunction_MultipleSeries()
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

                    var tsf = session.TimeSeriesFor("people/1", "Heartrate");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "tags/1");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "tags/2");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "tags/2");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "tags/2");

                    tsf = session.TimeSeriesFor("people/1", "Stocks");

                    tsf.Append(baseline.AddMinutes(61), new[] { 559d }, "tags/1");
                    tsf.Append(baseline.AddMinutes(62), new[] { 579d }, "tags/2");
                    tsf.Append(baseline.AddMinutes(63), new[] { 569d }, "tags/2");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 659d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 679d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 669d }, "tags/2");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var rawQuery = session.Advanced.RawQuery<CustomRawQueryResult4>(
@"declare timeseries out(name) 
{
    from name between $start and $end
    where Tag != 'tags/2'
}
from People as p
select {
    Heartrate: out('Heartrate'),
    Stocks: out('Stocks')
}")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddYears(1).EnsureUtc());

                    var result = rawQuery.First();

                    Assert.Equal(3, result.Heartrate.Count);

                    Assert.Equal(new[] { 59d }, result.Heartrate.Results[0].Values);
                    Assert.Equal("tags/1", result.Heartrate.Results[0].Tag);
                    Assert.Equal(baseline.AddMinutes(61), result.Heartrate.Results[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 159d }, result.Heartrate.Results[1].Values);
                    Assert.Equal("tags/1", result.Heartrate.Results[1].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), result.Heartrate.Results[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 179d }, result.Heartrate.Results[2].Values);
                    Assert.Equal("tags/1", result.Heartrate.Results[2].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(62), result.Heartrate.Results[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(3, result.Stocks.Count);

                    Assert.Equal(new[] { 559d }, result.Stocks.Results[0].Values);
                    Assert.Equal("tags/1", result.Stocks.Results[0].Tag);
                    Assert.Equal(baseline.AddMinutes(61), result.Stocks.Results[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 659d }, result.Stocks.Results[1].Values);
                    Assert.Equal("tags/1", result.Stocks.Results[1].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), result.Stocks.Results[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 679d }, result.Stocks.Results[2].Values);
                    Assert.Equal("tags/1", result.Stocks.Results[2].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(62), result.Stocks.Results[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);


                }
            }
        }

        [Fact]
        public void CanCallTimeSeriesDeclaredFunctionFromJavascriptDeclaredFunction()
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

                    var tsf = session.TimeSeriesFor("people/1", "Heartrate");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "tags/1");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "tags/2");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "tags/2");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "tags/2");

                    tsf = session.TimeSeriesFor("people/1", "Stocks");

                    tsf.Append(baseline.AddMinutes(61), new[] { 559d }, "tags/1");
                    tsf.Append(baseline.AddMinutes(62), new[] { 579d }, "tags/2");
                    tsf.Append(baseline.AddMinutes(63), new[] { 569d }, "tags/2");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 659d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 679d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 669d }, "tags/2");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var rawQuery = session.Advanced.RawQuery<CustomRawQueryResult4>(
@"declare timeseries ts(name) 
{
    from name between $start and $end
    where Tag != 'tags/2'
}
declare function out(d) 
{
    var result = {};
    var allTsNames = d['@metadata']['@timeseries'];
    for (var i = 0; i < allTsNames.length; i++){
        var name = allTsNames[i];
        result[name] = ts(name);
    }
    return result;    
}
from People as p
where p.Age > 49
select out(p)")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddYears(1).EnsureUtc());

                    var result = rawQuery.First();

                    Assert.Equal(3, result.Heartrate.Count);

                    Assert.Equal(new[] { 59d }, result.Heartrate.Results[0].Values);
                    Assert.Equal("tags/1", result.Heartrate.Results[0].Tag);
                    Assert.Equal(baseline.AddMinutes(61), result.Heartrate.Results[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 159d }, result.Heartrate.Results[1].Values);
                    Assert.Equal("tags/1", result.Heartrate.Results[1].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), result.Heartrate.Results[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 179d }, result.Heartrate.Results[2].Values);
                    Assert.Equal("tags/1", result.Heartrate.Results[2].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(62), result.Heartrate.Results[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(3, result.Stocks.Count);

                    Assert.Equal(new[] { 559d }, result.Stocks.Results[0].Values);
                    Assert.Equal("tags/1", result.Stocks.Results[0].Tag);
                    Assert.Equal(baseline.AddMinutes(61), result.Stocks.Results[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 659d }, result.Stocks.Results[1].Values);
                    Assert.Equal("tags/1", result.Stocks.Results[1].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), result.Stocks.Results[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 679d }, result.Stocks.Results[2].Values);
                    Assert.Equal("tags/1", result.Stocks.Results[2].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(62), result.Stocks.Results[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                }
            }
        }

        [Fact]
        public void CanCallTimeSeriesDeclaredFunctionFromJavascriptDeclaredFunction_DifferentRanges()
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

                    var tsf = session.TimeSeriesFor("people/1", "Heartrate");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "tags/1");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "tags/2");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "tags/2");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "tags/2");

                    tsf.Append(baseline.AddMonths(3).AddMinutes(61), new[] { 259d }, "tags/1");
                    tsf.Append(baseline.AddMonths(3).AddMinutes(62), new[] { 279d }, "tags/1");
                    tsf.Append(baseline.AddMonths(3).AddMinutes(63), new[] { 269d }, "tags/2");

                    tsf = session.TimeSeriesFor("people/1", "Stocks");

                    tsf.Append(baseline.AddMinutes(61), new[] { 559d }, "tags/1");
                    tsf.Append(baseline.AddMinutes(62), new[] { 579d }, "tags/2");
                    tsf.Append(baseline.AddMinutes(63), new[] { 569d }, "tags/2");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 659d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 679d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 669d }, "tags/2");

                    tsf.Append(baseline.AddMonths(3).AddMinutes(61), new[] { 759d }, "tags/1");
                    tsf.Append(baseline.AddMonths(3).AddMinutes(62), new[] { 779d }, "tags/1");
                    tsf.Append(baseline.AddMonths(3).AddMinutes(63), new[] { 769d }, "tags/2");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var rawQuery = session.Advanced.RawQuery<CustomRawQueryResult4>(
@"declare timeseries ts(name, start, end) 
{
    from name between start and end
    where Tag != 'tags/2'
}
declare function out(d) 
{
    var result = {};
    var allTsNames = d['@metadata']['@timeseries'];
    for (var i = 0; i < allTsNames.length; i++){
        var name = allTsNames[i];
        var range = $ranges[i % 3];
        result[name] = ts(name, range.Start, range.End);
    }
    return result;    
}
from People as p
where p.Age > 49
select out(p)")
                        .AddParameter("ranges", new object[3]
                        {
                            new 
                            {
                                Start = baseline.EnsureUtc(),
                                End = baseline.AddMonths(2).EnsureUtc()
                            },
                            new
                            {
                                Start = baseline.AddMonths(1).EnsureUtc(),
                                End = baseline.AddMonths(6).EnsureUtc()
                            },
                            new
                            {
                                Start = baseline.EnsureUtc(),
                                End = baseline.AddYears(1).EnsureUtc()
                            }
                        });

                    var result = rawQuery.First();

                    Assert.Equal(3, result.Heartrate.Count);

                    Assert.Equal(new[] { 59d }, result.Heartrate.Results[0].Values);
                    Assert.Equal("tags/1", result.Heartrate.Results[0].Tag);
                    Assert.Equal(baseline.AddMinutes(61), result.Heartrate.Results[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 159d }, result.Heartrate.Results[1].Values);
                    Assert.Equal("tags/1", result.Heartrate.Results[1].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), result.Heartrate.Results[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 179d }, result.Heartrate.Results[2].Values);
                    Assert.Equal("tags/1", result.Heartrate.Results[2].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(62), result.Heartrate.Results[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(4, result.Stocks.Count);

                    Assert.Equal(new[] { 659d }, result.Stocks.Results[0].Values);
                    Assert.Equal("tags/1", result.Stocks.Results[0].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), result.Stocks.Results[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 679d }, result.Stocks.Results[1].Values);
                    Assert.Equal("tags/1", result.Stocks.Results[1].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(62), result.Stocks.Results[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 759d }, result.Stocks.Results[2].Values);
                    Assert.Equal("tags/1", result.Stocks.Results[2].Tag);
                    Assert.Equal(baseline.AddMonths(3).AddMinutes(61), result.Stocks.Results[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 779d }, result.Stocks.Results[3].Values);
                    Assert.Equal("tags/1", result.Stocks.Results[3].Tag);
                    Assert.Equal(baseline.AddMonths(3).AddMinutes(62), result.Stocks.Results[3].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                }
            }
        }

        [Fact]
        public void CanCallTimeSeriesDeclaredFunctionFromJavascriptProjection_MultipleValues()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                        Age = 60
                    }, "people/1");

                    var tsf = session.TimeSeriesFor("people/1", "Stocks");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "tags/1");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d, 97d }, "tags/2");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d, 96d }, "tags/2");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d, 251d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d, 271d, 372d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "tags/2");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var rawQuery = session.Advanced.RawQuery<CustomRawQueryResult3>(
@"declare timeseries out(doc) 
{
    from doc.Stocks between $start and $end
    where Values[1] != null and ( Values[0] > 70 OR Values[1] > 100 )
}
from People as p
where p.Age > 49
select {
    Series: out(p)
}")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddYears(1).EnsureUtc());

                    var result = rawQuery.First();

                    Assert.Equal(3, result.Series.Count);

                    Assert.Equal(new[] { 79d, 97d }, result.Series.Results[0].Values);
                    Assert.Equal("tags/2", result.Series.Results[0].Tag);
                    Assert.Equal(baseline.AddMinutes(62), result.Series.Results[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 159d, 251d }, result.Series.Results[1].Values);
                    Assert.Equal("tags/1", result.Series.Results[1].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), result.Series.Results[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 179d, 271d, 372d }, result.Series.Results[2].Values);
                    Assert.Equal("tags/1", result.Series.Results[2].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(62), result.Series.Results[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void CanUseTimeSeriesQueryResultAsArgumentToJavascriptDeclaredFunction()
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

                    var tsf = session.TimeSeriesFor("people/1", "Heartrate");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/apple");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/apple");

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
        Tag : arr[mid].Tag,
        Mid : mid,
        Count : tsResult.Count                
    };
}
declare timeseries heartrate(doc){
    from doc.Heartrate between $start and $end
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
                    Assert.Equal("watches/apple", result.Tag);
                    Assert.Equal(159d, result.Value);
                }
            }
        }

        [Fact]
        public void CanUseTimeSeriesAggregationResultAsArgumentToJavascriptDeclaredFunction()
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


                    var tsf = session.TimeSeriesFor("people/1", "Heartrate");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/apple");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/apple");

                    tsf.Append(baseline.AddMonths(2).AddMinutes(61), new[] { 259d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(2).AddMinutes(62), new[] { 279d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(2).AddMinutes(63), new[] { 269d }, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var rawQuery = session.Advanced.RawQuery<CustomRawQueryResult2>(
@"declare function foo(tsResult) {
    var arr = tsResult.Results;
    var result = [];
    for (var i = 0; i < arr.length; i++)
    {
        var current = arr[i];
        result[i] = {
            Max : current.Max[0],
            Avg : current.Average[0]
        };
    }
    return {
        HeartRate: result,
        Count : tsResult.Count                
    };
}
declare timeseries heartrate(doc){
    from doc.Heartrate between $start and $end
    where Tag = 'watches/fitbit'
    group by '1 month'
    select max(), avg()
}
from People as p
where p.Age > 21
select foo(heartrate(p))
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddYears(1).EnsureUtc());

                    var result = rawQuery.First();

                    Assert.Equal(5, result.Count);

                    Assert.Equal(3, result.HeartRate.Length);


                    Assert.Equal(79, result.HeartRate[0].Max);
                    Assert.Equal(69, result.HeartRate[0].Avg);

                    Assert.Equal(179, result.HeartRate[1].Max);

                    Assert.Equal(279, result.HeartRate[2].Max);
                    Assert.Equal(274, result.HeartRate[2].Avg);


                }
            }
        }

        [Fact]
        public void TimeSeriesAggregationInsideJsProjection_UsingLinq()
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

                    var tsf = session.TimeSeriesFor("people/1", "Heartrate");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/apple");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/apple");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from p in session.Query<Person>()
                                where p.Age > 21
                                select new
                                {
                                    Heartrate = RavenQuery.TimeSeries(p, "Heartrate", baseline, baseline.AddMonths(2))
                                        .Where(ts => ts.Tag == "watches/fitbit")
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
        public void TimeSeriesAggregationInsideJsProjection_UsingLinq_FromLoadedDocument()
        {
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

                    var tsf = session.TimeSeriesFor(companyId, "Stock");

                    tsf.Append(baseline.AddMinutes(61), new[] { 12.59d }, "tags/1");
                    tsf.Append(baseline.AddMinutes(62), new[] { 12.79d }, "tags/1");
                    tsf.Append(baseline.AddMinutes(63), new[] { 12.69d }, "tags/2");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 13.59d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 13.79d }, "tags/2");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 13.69d }, "tags/1");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from p in session.Query<Person>()
                                where p.Age > 21
                                let company = RavenQuery.Load<Company>(p.WorksAt)
                                select new
                                {
                                    Heartrate = RavenQuery.TimeSeries(company, "Stock", baseline, baseline.AddMonths(2))
                                        .Where(ts => ts.Tag == "tags/1")
                                        .GroupBy(g => g.Months(1))
                                        .Select(g => new
                                        {
                                            Avg = g.Average(),
                                            Max = g.Max()
                                        })
                                        .ToList(),
                                    Name = p.Name + " " + p.LastName
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
        public void TimeSeriesAggregationInsideJsProjection_UsingLinq_WithLoadedTag()
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

                    session.Store(new Watch
                    {
                        Accuracy = 2
                    }, "watches/fitbit");

                    session.Store(new Watch
                    {
                        Accuracy = 1.5
                    }, "watches/apple");

                    var tsf = session.TimeSeriesFor("people/1", "Heartrate");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/apple");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from p in session.Query<Person>()
                                where p.Age > 21
                                select new
                                {
                                    Heartrate = RavenQuery.TimeSeries(p, "Heartrate", baseline, baseline.AddMonths(2))
                                        .LoadByTag<Watch>()
                                        .Where((ts, watch) => ts.Values[0] > 70 && watch.Accuracy >= 2)
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
                    Assert.Equal(3, result[0].Heartrate.Count);
                    Assert.Equal("Oren Ayende", result[0].Name);

                    var agg = result[0].Heartrate.Results;

                    Assert.Equal(2, agg.Length);

                    Assert.Equal(79, agg[0].Max[0]);
                    Assert.Equal(79, agg[0].Average[0]);

                    Assert.Equal(179, agg[1].Max[0]);
                    Assert.Equal(174, agg[1].Average[0]);

                }
            }
        }

        [Fact]
        public void TimeSeriesAggregationInsideJsProjection_UsingLinq_MultipleSeries()
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


                    var tsf = session.TimeSeriesFor("people/1", "Heartrate");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/apple");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/apple");

                    tsf = session.TimeSeriesFor("people/1", "Stocks");

                    tsf.Append(baseline.AddMinutes(61), new[] { 559d }, "tags/1");
                    tsf.Append(baseline.AddMinutes(62), new[] { 579d }, "tags/1");
                    tsf.Append(baseline.AddMinutes(63), new[] { 569d }, "tags/2");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 659d }, "tags/2");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 679d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 669d }, "tags/2");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from p in session.Query<Person>()
                                where p.Age > 21
                                select new
                                {
                                    Name = p.Name + " " + p.LastName, // creates a js projection
                                    Heartrate = RavenQuery.TimeSeries(p, "Heartrate", baseline, baseline.AddMonths(2))
                                        .Where(ts => ts.Values[0] > 100 && ts.Tag != "watches/fitbit")
                                        .ToList(),
                                    Stocks = RavenQuery.TimeSeries(p, "Stocks", baseline, baseline.AddMonths(2))
                                        .Where(ts => ts.Tag == "tags/1" && ts.Values[0] < 600)
                                        .ToList()
                                };

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal("Oren Ayende", result[0].Name);

                    var heartrate = result[0].Heartrate.Results;

                    Assert.Equal(2, heartrate.Length);

                    Assert.Equal(159, heartrate[0].Value);
                    Assert.Equal(169, heartrate[1].Value);

                    var stocks = result[0].Stocks.Results;

                    Assert.Equal(2, stocks.Length);

                    Assert.Equal(559, stocks[0].Value);
                    Assert.Equal(579, stocks[1].Value);

                }
            }
        }

        [Fact]
        public void TimeSeriesAggregationInsideJsProjection_UsingLinq_CanDefineTmeSeriesInsideLet()
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

                    var tsf = session.TimeSeriesFor("people/1", "Heartrate");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/apple");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/apple");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from p in session.Query<Person>()
                                where p.Age > 21
                                let heartrate = RavenQuery.TimeSeries(p, "Heartrate", baseline, baseline.AddMonths(2))
                                    .Where(ts => ts.Tag == "watches/fitbit")
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
        public void TimeSeriesAggregationInsideJsProjection_UsingLinq_WithVariables()
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

                    var tsf = session.TimeSeriesFor("people/1", "Heartrate");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/apple");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/apple");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var name = "Heartrate";
                    var tag = "watches/fitbit";

                    var query = from p in session.Query<Person>()
                                where p.Age > 21
                                select new
                                {
                                    Heartrate = RavenQuery.TimeSeries(p, name, baseline, baseline.AddMonths(2))
                                        .Where(ts => ts.Tag == tag)
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
        public void CanUseTimeSeriesQueryResultAsArgumentToJavascriptFunction_UsingLinq()
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

                    session.Store(new Watch
                    {
                        Accuracy = 2.2
                    }, "watches/fitbit");

                    session.Store(new Watch
                    {
                        Accuracy = 2.5
                    }, "watches/apple");

                    session.Store(new Watch
                    {
                        Accuracy = 1.5
                    }, "watches/sony");

                    var tsf = session.TimeSeriesFor("people/1", "Heartrate");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/apple");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/sony");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from person in session.Query<Person>()
                        where person.Age > 18
                        let customFunc = new Func<TimeSeriesEntry[], CustomJsFunctionResult>(entries => new CustomJsFunctionResult
                        {
                            Max = entries.Max(entry => entry.Values[0]),
                            HasApple = entries.Select(x => x.Tag)
                                .Contains("watches/apple"),
                            Accuracies = RavenQuery.Load<Watch>(entries.Select(e => e.Tag))
                                .Select(doc => doc.Accuracy)
                                .Distinct()
                                .ToList()
                        })
                        let tsQuery = RavenQuery.TimeSeries(person, "Heartrate", baseline, baseline.AddMonths(2))
                            .LoadByTag<Watch>()
                            .Where((ts, watch) => ts.Values[0] > 70 && watch.Accuracy >= 2)
                            .ToList()
                        select new
                        {
                            Series = tsQuery,
                            Custom = customFunc(tsQuery.Results)
                        };

                    var result = query.First();

                    Assert.Equal(3, result.Series.Count);

                    var heartrate = result.Series.Results;
                    Assert.Equal(79d, heartrate[0].Value);
                    Assert.Equal("watches/fitbit", heartrate[0].Tag);
                    Assert.Equal(baseline.AddMinutes(62), heartrate[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(159d, heartrate[1].Value);
                    Assert.Equal("watches/apple", heartrate[1].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), heartrate[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(169d, heartrate[2].Value);
                    Assert.Equal("watches/fitbit", heartrate[2].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(63), heartrate[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    var custom = result.Custom;
                    Assert.Equal(169d, custom.Max);
                    Assert.True(custom.HasApple);

                    Assert.Equal(2, custom.Accuracies.Count);
                    Assert.Equal(2.2, custom.Accuracies[0]);
                    Assert.Equal(2.5, custom.Accuracies[1]);

                }
            }
        }

        [Fact]
        public void CanUseTimeSeriesAggregationResultAsArgumentToJavascriptFunction_UsingLinq()
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

                    session.Store(new Watch
                    {
                        Accuracy = 2.2
                    }, "watches/fitbit");

                    session.Store(new Watch
                    {
                        Accuracy = 2.5
                    }, "watches/apple");

                    session.Store(new Watch
                    {
                        Accuracy = 1.5
                    }, "watches/sony");

                    var tsf = session.TimeSeriesFor("people/1", "Heartrate");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/apple");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/sony");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var series = "Heartrate";
                    var query = from person in session.Query<Person>()
                                where person.Age > 18
                                let customFunc = new Func<TimeSeriesRangeAggregation[], CustomJsFunctionResult2>(ranges => new CustomJsFunctionResult2
                                {
                                    TotalMax = ranges.Max(range => range.Max[0]),
                                    TotalMin = ranges.Min(range => range.Min[0]),
                                    AvgOfAvg = ranges.Average(range => range.Average[0]),
                                    MaxGroupSize = ranges.Max(r => r.Count[0])
                                })
                                let tsQuery = RavenQuery.TimeSeries(person, series, baseline, baseline.AddMonths(2))
                                    .LoadByTag<Watch>()
                                    .Where((ts, watch) => ts.Values[0] > 70 && watch.Accuracy >= 2)
                                    .GroupBy(g => g.Months(1))
                                    .Select(x => new
                                    {
                                        Max = x.Max(),
                                        Min = x.Min(),
                                        Avg = x.Average(),
                                        Count = x.Count()
                                    })
                                    .ToList()
                                select new
                                {
                                    Series = tsQuery,
                                    Custom = customFunc(tsQuery.Results)
                                };

                    var result = query.First();

                    Assert.Equal(3, result.Series.Count);

                    var agg = result.Series.Results;
                    Assert.Equal(2, agg.Length);

                    Assert.Equal(79d, agg[0].Max[0]);
                    Assert.Equal(79d, agg[0].Min[0]);
                    Assert.Equal(79d, agg[0].Average[0]);
                    Assert.Equal(1, agg[0].Count[0]);

                    Assert.Equal(169d, agg[1].Max[0]);
                    Assert.Equal(159d, agg[1].Min[0]);
                    Assert.Equal(164d, agg[1].Average[0]);
                    Assert.Equal(2, agg[1].Count[0]);

                    var custom = result.Custom;
                    Assert.Equal(169d, custom.TotalMax);
                    Assert.Equal(79d, custom.TotalMin);
                    Assert.Equal(121.5d, custom.AvgOfAvg);
                    Assert.Equal(2, custom.MaxGroupSize);


                }
            }
        }

        [Fact]
        public void TimeSeriesAggregationInsideJsProjection_UsingLinq_WhenTsQueryExpressionIsNestedInsideAnotherExpression()
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

                    var tsf = session.TimeSeriesFor("people/1", "Heartrate");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/apple");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/apple");

                    tsf = session.TimeSeriesFor("people/1", "Stocks");

                    tsf.Append(baseline.AddMinutes(61), new[] { 559d }, "tags/1");
                    tsf.Append(baseline.AddMinutes(62), new[] { 579d }, "tags/1");
                    tsf.Append(baseline.AddMinutes(63), new[] { 569d }, "tags/2");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 659d }, "tags/2");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 679d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 669d }, "tags/2");

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
                                    Heartrate = tsFunc("Heartrate"),
                                    Stocks = tsFunc("Stocks")
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
        public void CanDefineCustomJsFunctionThatHasTimeSeriesCall_UsingLinq()
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

                    var tsf = session.TimeSeriesFor("people/1", "Heartrate");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/apple");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/apple");

                    tsf.Append(baseline.AddMonths(3).AddMinutes(61), new[] { 259d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(3).AddMinutes(62), new[] { 279d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(3).AddMinutes(63), new[] { 269d }, "watches/apple");

                    tsf = session.TimeSeriesFor("people/1", "Stocks");

                    tsf.Append(baseline.AddMinutes(61), new[] { 559d }, "tags/1");
                    tsf.Append(baseline.AddMinutes(62), new[] { 579d }, "tags/1");
                    tsf.Append(baseline.AddMinutes(63), new[] { 569d }, "tags/2");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 659d }, "tags/2");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 679d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 669d }, "tags/2");

                    tsf.Append(baseline.AddMonths(3).AddMinutes(61), new[] { 459d }, "tags/2");
                    tsf.Append(baseline.AddMonths(3).AddMinutes(62), new[] { 479d }, "tags/1");
                    tsf.Append(baseline.AddMonths(3).AddMinutes(63), new[] { 469d }, "tags/2");

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
                                    Heartrate = tsFunc(p, "Heartrate", baseline, baseline.AddMonths(2)),
                                    Stocks = tsFunc(p, "Stocks", baseline, baseline.AddMonths(4))

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
        public void CanDefineCustomJsFunctionThatHasTimeSeriesCall_UsingLinq_InvokeOnDifferentObjectInstances()
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
                        WorksAt = "companies/1"
                    }, "people/1");

                    session.Store(new Company(), "companies/1");

                    var tsf = session.TimeSeriesFor("people/1", "Heartrate");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/apple");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/apple");

                    tsf.Append(baseline.AddMonths(3).AddMinutes(61), new[] { 259d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(3).AddMinutes(62), new[] { 279d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(3).AddMinutes(63), new[] { 269d }, "watches/apple");

                    tsf = session.TimeSeriesFor("companies/1", "Stocks");

                    tsf.Append(baseline.AddMinutes(61), new[] { 559d }, "tags/1");
                    tsf.Append(baseline.AddMinutes(62), new[] { 579d }, "tags/1");
                    tsf.Append(baseline.AddMinutes(63), new[] { 569d }, "tags/2");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 659d }, "tags/2");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 679d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 669d }, "tags/2");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from p in session.Query<Person>()
                                where p.Age > 21
                                let tsFunc = new Func<object, string, TimeSeriesEntry[]>((o, name) =>
                                    RavenQuery.TimeSeries(o, name, baseline, baseline.AddMonths(2))
                                        .Where(ts => ts.Values[0] > 100 && ts.Values[0] < 600)
                                        .ToList()
                                        .Results)
                                select new
                                {
                                    Name = p.Name + " " + p.LastName,
                                    Heartrate = tsFunc(p, "Heartrate"),
                                    Stocks = tsFunc(RavenQuery.Load<Company>(p.WorksAt), "Stocks")

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
        public void CanDefineCustomJsFunctionThatHasTimeSeriesCall_UsingLinq_TimeSeriesCallWithJustNameParameter()
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
                        Age = 30
                    }, "people/1");

                    var tsf = session.TimeSeriesFor("people/1", "Heartrate");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/apple");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/apple");

                    tsf = session.TimeSeriesFor("people/1", "Stocks");

                    tsf.Append(baseline.AddMinutes(61), new[] { 559d }, "tags/1");
                    tsf.Append(baseline.AddMinutes(62), new[] { 579d }, "tags/1");
                    tsf.Append(baseline.AddMinutes(63), new[] { 569d }, "tags/2");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 659d }, "tags/2");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 679d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 669d }, "tags/2");

                    tsf.Append(baseline.AddMonths(3).AddMinutes(61), new[] { 459d }, "tags/2");
                    tsf.Append(baseline.AddMonths(3).AddMinutes(62), new[] { 479d }, "tags/1");
                    tsf.Append(baseline.AddMonths(3).AddMinutes(63), new[] { 469d }, "tags/2");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from p in session.Query<Person>()
                                where p.Age > 21
                                let tsFunc = new Func<string, TimeSeriesEntry[]>(name =>
                                    RavenQuery.TimeSeries(name)
                                        .Where(ts => ts.Values[0] > 100 && ts.Values[0] < 600)
                                        .ToList()
                                        .Results)
                                select new
                                {
                                    Name = p.Name + " " + p.LastName,
                                    Heartrate = tsFunc("Heartrate"),
                                    Stocks = tsFunc("Stocks")

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
        public void CanDefineCustomJsFunctionThatHasTimeSeriesCall_UsingLinq_WithComputationOnRawResult()
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
                        Age = 30
                    }, "people/1");

                    session.Store(new Watch(), "watches/fitbit");
                    session.Store(new Watch(), "watches/apple");

                    var tsf = session.TimeSeriesFor("people/1", "Heartrate");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/apple");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/apple");

                    tsf.Append(baseline.AddMonths(3).AddMinutes(61), new[] { 259d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(3).AddMinutes(62), new[] { 279d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(3).AddMinutes(63), new[] { 992d });

                    tsf.Append(baseline.AddMonths(5).AddMinutes(61), new[] { 559d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(5).AddMinutes(62), new[] { 579d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(5).AddMinutes(63), new[] { 569d }, "watches/apple");

                    tsf.Append(baseline.AddMonths(6).AddMinutes(61), new[] { 459d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(6).AddMinutes(62), new[] { 479d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(6).AddMinutes(63), new[] { 999d }, "watches/sony");

                    tsf.Append(baseline.AddMonths(7).AddMinutes(61), new[] { 359d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(7).AddMinutes(62), new[] { 379d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(7).AddMinutes(63), new[] { 369d }, "watches/apple");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query =
                        from p in session.Query<Person>()
                        where p.Age > 21
                        let tsFunc = new Func<string, DateTime, DateTime, double?>((name, f, t) =>
                            RavenQuery.TimeSeries(p, name, f, t)
                                .LoadByTag<Watch>()
                                .Where((ts, src) => ts.Values[0] < 500 && src != null)
                                .ToList()
                                .Results
                                .Max(entry => entry.Values[0]))
                        select new
                        {
                            Name = p.Name + " " + p.LastName,
                            TotalMax = tsFunc("Heartrate", baseline, baseline.AddYears(1))
                        };

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal("Oren Ayende", result[0].Name);

                    Assert.Equal(479d, result[0].TotalMax);

                }
            }
        }

        [Fact]
        public void CanDefineCustomJsFunctionThatHasTimeSeriesCall_UsingLinq_WithComputationOnAggregatedResult()
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
                        Age = 30
                    }, "people/1");

                    session.Store(new Watch(), "watches/fitbit");
                    session.Store(new Watch(), "watches/apple");

                    var tsf = session.TimeSeriesFor("people/1", "Heartrate");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/apple");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/apple");

                    tsf.Append(baseline.AddMonths(3).AddMinutes(61), new[] { 259d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(3).AddMinutes(62), new[] { 279d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(3).AddMinutes(63), new[] { 992d });

                    tsf.Append(baseline.AddMonths(5).AddMinutes(61), new[] { 559d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(5).AddMinutes(62), new[] { 579d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(5).AddMinutes(63), new[] { 569d }, "watches/apple");

                    tsf.Append(baseline.AddMonths(6).AddMinutes(61), new[] { 459d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(6).AddMinutes(62), new[] { 479d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(6).AddMinutes(63), new[] { 999d }, "watches/sony");

                    tsf.Append(baseline.AddMonths(7).AddMinutes(61), new[] { 359d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(7).AddMinutes(62), new[] { 379d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(7).AddMinutes(63), new[] { 369d }, "watches/apple");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var estimatedAvg = 250.456;
                    var query =
                        from p in session.Query<Person>()
                        where p.Age > 21
                        let tsFunc = new Func<string, DateTime, DateTime, IEnumerable<TimeSeriesRangeAggregation>>((name, f, t) =>
                            RavenQuery.TimeSeries(p, name, f, t)
                                .LoadByTag<Watch>()
                                .Where((ts, src) => ts.Values[0] < 500 && src != null)
                                .GroupBy(g => g.Months(1))
                                .Select(x => new {Max = x.Max(), Avg = x.Average()})
                                .ToList()
                                .Results
                                .Where(range => range.Average[0] > estimatedAvg))
                        select new
                        {
                            Name = p.Name + " " + p.LastName,
                            Series = tsFunc("Heartrate", baseline, baseline.AddYears(1))
                                .Select(r => new
                                {
                                    Max = r.Max[0], 
                                    Avg = r.Average[0]
                                })
                                .ToList()
                        };

                    var result = query.First();

                    Assert.Equal("Oren Ayende", result.Name);

                    Assert.Equal(3, result.Series.Count);
                    Assert.Equal(279, result.Series[0].Max);
                    Assert.Equal(269, result.Series[0].Avg);

                    Assert.Equal(479, result.Series[1].Max);
                    Assert.Equal(469, result.Series[1].Avg);

                    Assert.Equal(379, result.Series[2].Max);
                    Assert.Equal(369, result.Series[2].Avg);

                }
            }
        }

        [Fact]
        public void CanDefineCustomJsFunctionThatHasTimeSeriesCall_UsingLinq_WithOrderByOnTimeSeriesResultValues()
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
                        Age = 30
                    }, "people/1");

                    session.Store(new Watch
                    {
                        Accuracy = 2.5
                    }, "watches/fitbit");
                    session.Store(new Watch
                    {
                        Accuracy = 2.75
                    }, "watches/apple");

                    var tsf = session.TimeSeriesFor("people/1", "Heartrate");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/apple");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/apple");

                    tsf.Append(baseline.AddMonths(3).AddMinutes(61), new[] { 259d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(3).AddMinutes(62), new[] { 279d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(3).AddMinutes(63), new[] { 992d });

                    tsf.Append(baseline.AddMonths(5).AddMinutes(61), new[] { 559d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(5).AddMinutes(62), new[] { 579d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(5).AddMinutes(63), new[] { 569d }, "watches/apple");

                    tsf.Append(baseline.AddMonths(6).AddMinutes(61), new[] { 459d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(6).AddMinutes(62), new[] { 479d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(6).AddMinutes(63), new[] { 999d }, "watches/sony");

                    tsf.Append(baseline.AddMonths(7).AddMinutes(61), new[] { 359d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(7).AddMinutes(62), new[] { 379d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(7).AddMinutes(63), new[] { 369d }, "watches/apple");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query =
                        from p in session.Query<Person>()
                        where p.Age > 21
                        let tsFunc = new Func<string, DateTime, DateTime, TimeSeriesEntry>((name, f, t) =>
                            RavenQuery.TimeSeries(p, name, f, t)
                                .LoadByTag<Watch>()
                                .Where((ts, src) => ts.Values[0] < 500 && src != null)
                                .ToList()
                                .Results
                                .OrderBy(entry => entry.Values[0])
                                .Last())
                        let last = tsFunc("Heartrate", baseline, baseline.AddYears(1))
                        select new
                        {
                            Name = p.Name + " " + p.LastName,
                            TotalMax = last.Values[0],
                            TagOfMax = last.Tag,
                            SourceMax = RavenQuery.Load<Watch>(last.Tag)
                        };

                    var result = query.First();

                    Assert.Equal("Oren Ayende", result.Name);
                    Assert.Equal(479d, result.TotalMax);
                    Assert.Equal("watches/fitbit", result.TagOfMax);
                    Assert.Equal(2.5, result.SourceMax?.Accuracy);
                }
            }
        }

        [Fact]
        public void CanDefineCustomJsFunctionThatHasTimeSeriesCall_UsingLinq_WithComputationOnValueField()
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
                        Age = 30
                    }, "people/1");

                    session.Store(new Watch(), "watches/fitbit");
                    session.Store(new Watch(), "watches/apple");

                    var tsf = session.TimeSeriesFor("people/1", "Heartrate");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/apple");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/apple");

                    tsf.Append(baseline.AddMonths(3).AddMinutes(61), new[] { 259d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(3).AddMinutes(62), new[] { 279d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(3).AddMinutes(63), new[] { 992d });

                    tsf.Append(baseline.AddMonths(5).AddMinutes(61), new[] { 559d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(5).AddMinutes(62), new[] { 579d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(5).AddMinutes(63), new[] { 569d }, "watches/apple");

                    tsf.Append(baseline.AddMonths(6).AddMinutes(61), new[] { 459d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(6).AddMinutes(62), new[] { 479d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(6).AddMinutes(63), new[] { 999d }, "watches/sony");

                    tsf.Append(baseline.AddMonths(7).AddMinutes(61), new[] { 359d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(7).AddMinutes(62), new[] { 379d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(7).AddMinutes(63), new[] { 369d }, "watches/apple");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query =
                        from p in session.Query<Person>()
                        where p.Age > 21
                        let tsQuery = 
                            RavenQuery.TimeSeries(p, "Heartrate", baseline, baseline.AddYears(1))
                                .Where(ts => ts.Value < 500)
                                .ToList()
                        let last = tsQuery.Results
                            .OrderBy(entry => entry.Values[0])
                            .Last()
                        select new
                        {
                            Max = last.Values[0],
                            TagOfMax = last.Tag
                        };

                    var results = query.ToList();
                    var result = results.First();

                    Assert.Equal(479d, result.Max);
                    Assert.Equal("watches/fitbit", result.TagOfMax);
                }
            }
        }

        [Fact]
        public void CanCallTimeSeriesDeclaredFunctionFromJavascriptProjection_UsingLinq_TagAsParameter()
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

                    var tsf = session.TimeSeriesFor("people/1", "HeartRate");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "tags/1");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "tags/2");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "tags/2");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "tags/2");

                    tsf = session.TimeSeriesFor("companies/1", "Stocks");

                    tsf.Append(baseline.AddMinutes(61), new[] { 559d }, "tags/1");
                    tsf.Append(baseline.AddMinutes(62), new[] { 579d }, "tags/2");
                    tsf.Append(baseline.AddMinutes(63), new[] { 569d }, "tags/3");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 659d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 679d }, "tags/1");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 669d }, "tags/2");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from p in session.Query<Person>()
                        let ts = new Func<object, string, string, TimeSeriesRawResult>((o, name, t) =>
                            RavenQuery.TimeSeries(o, name, baseline, baseline.AddYears(1))
                                .Where(entry => entry.Tag != t)
                                .ToList())
                        let company = RavenQuery.Load<Company>(p.WorksAt)
                        select new
                        {
                            Series = ts(p, "HeartRate", "tags/2"), 
                            Series2 = ts(company,"Stocks", "tags/3")
                        };

                    var result = query.First();

                    Assert.Equal(3, result.Series.Count);

                    Assert.Equal(new[] { 59d }, result.Series.Results[0].Values);
                    Assert.Equal("tags/1", result.Series.Results[0].Tag);
                    Assert.Equal(baseline.AddMinutes(61), result.Series.Results[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 159d }, result.Series.Results[1].Values);
                    Assert.Equal("tags/1", result.Series.Results[1].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), result.Series.Results[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 179d }, result.Series.Results[2].Values);
                    Assert.Equal("tags/1", result.Series.Results[2].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(62), result.Series.Results[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(5, result.Series2.Count);

                    Assert.Equal(new[] { 559d }, result.Series2.Results[0].Values);
                    Assert.Equal("tags/1", result.Series2.Results[0].Tag);
                    Assert.Equal(baseline.AddMinutes(61), result.Series2.Results[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 579d }, result.Series2.Results[1].Values);
                    Assert.Equal("tags/2", result.Series2.Results[1].Tag);
                    Assert.Equal(baseline.AddMinutes(62), result.Series2.Results[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 659d }, result.Series2.Results[2].Values);
                    Assert.Equal("tags/1", result.Series2.Results[2].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), result.Series2.Results[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 679d }, result.Series2.Results[3].Values);
                    Assert.Equal("tags/1", result.Series2.Results[3].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(62), result.Series2.Results[3].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    Assert.Equal(new[] { 669d }, result.Series2.Results[4].Values);
                    Assert.Equal("tags/2", result.Series2.Results[4].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(63), result.Series2.Results[4].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                }
            }
        }

    }
}
