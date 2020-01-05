using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Tests.Core.Utils.Entities;
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



        [Fact]
        public void TimeSeriesAggregationInJsProjection_RawQuery()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                        LastName = "Ayende",
                        Age = 30,
                    }, "people/1");


                    var tsf = session.TimeSeriesFor("people/1");

                    tsf.Append("Heartrate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                    tsf.Append("Heartrate", baseline.AddMinutes(62), "watches/fitbit", new[] { 79d });
                    tsf.Append("Heartrate", baseline.AddMinutes(63), "watches/apple", new[] { 69d });

                    tsf.Append("Heartrate", baseline.AddMonths(1).AddMinutes(61), "watches/apple", new[] { 159d });
                    tsf.Append("Heartrate", baseline.AddMonths(1).AddMinutes(62), "watches/fitbit", new[] { 179d });
                    tsf.Append("Heartrate", baseline.AddMonths(1).AddMinutes(63), "watches/apple", new[] { 169d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var timeSeriesQuery = 
@"from p.Heartrate between $start and $end 
where Tag != 'watches/apple'
group by '1 month' 
select max(), avg()";

                    var rawQuery = session.Advanced.RawQuery<QueryResult>(
@"from People as p
select {
    Name : p.Name + ' ' + p.LastName ,
    Heartrate : timeseries($ts)
}
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddYears(1))
                        .AddParameter("ts", timeSeriesQuery);

                    var result = rawQuery.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal("Oren Ayende", result[0].Name);

                    Assert.Equal(3, result[0].HeartRate.Count);

                    var agg = result[0].HeartRate.Results;

                    Assert.Equal(2, agg.Length);

                    Assert.Equal(79, agg[0].Max[0]);
                    Assert.Equal(69, agg[0].Avg[0]);

                    Assert.Equal(179, agg[1].Max[0]);
                    Assert.Equal(179, agg[1].Avg[0]);

                }
            }
        }


        [Fact]
        public void TimeSeriesAggregationInJsProjection_UsingLinq()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                        LastName = "Ayende",
                        Age = 30,
                    }, "people/1");


                    var tsf = session.TimeSeriesFor("people/1");

                    tsf.Append("Heartrate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                    tsf.Append("Heartrate", baseline.AddMinutes(62), "watches/fitbit", new[] { 79d });
                    tsf.Append("Heartrate", baseline.AddMinutes(63), "watches/apple", new[] { 69d });

                    tsf.Append("Heartrate", baseline.AddMonths(1).AddMinutes(61), "watches/apple", new[] { 159d });
                    tsf.Append("Heartrate", baseline.AddMonths(1).AddMinutes(62), "watches/fitbit", new[] { 179d });
                    tsf.Append("Heartrate", baseline.AddMonths(1).AddMinutes(63), "watches/apple", new[] { 169d });

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
                    Assert.Equal(69, agg[0].Avg[0]);

                    Assert.Equal(179, agg[1].Max[0]);
                    Assert.Equal(179, agg[1].Avg[0]);

                }
            }
        }

        [Fact]
        public void TimeSeriesAggregationInJsProjection_UsingLinq_FromLoadedDocument()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

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

                    var tsf = session.TimeSeriesFor(companyId);

                    tsf.Append("Stock", baseline.AddMinutes(61), "tags/1", new[] { 12.59d });
                    tsf.Append("Stock", baseline.AddMinutes(62), "tags/1", new[] { 12.79d });
                    tsf.Append("Stock", baseline.AddMinutes(63), "tags/2", new[] { 12.69d });

                    tsf.Append("Stock", baseline.AddMonths(1).AddMinutes(61), "tags/1", new[] { 13.59d });
                    tsf.Append("Stock", baseline.AddMonths(1).AddMinutes(62), "tags/2", new[] { 13.79d });
                    tsf.Append("Stock", baseline.AddMonths(1).AddMinutes(63), "tags/1", new[] { 13.69d });

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
                                        .GroupBy("1 month")
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
                    Assert.Equal(12.69, agg[0].Avg[0]);

                    Assert.Equal(13.69, agg[1].Max[0]);
                    Assert.Equal(13.64, agg[1].Avg[0]);

                }
            }
        }

        [Fact]
        public void TimeSeriesAggregationInJsProjection_UsingLinq_WithLoadedTag()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

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

                    var tsf = session.TimeSeriesFor("people/1");

                    tsf.Append("Heartrate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                    tsf.Append("Heartrate", baseline.AddMinutes(62), "watches/fitbit", new[] { 79d });
                    tsf.Append("Heartrate", baseline.AddMinutes(63), "watches/apple", new[] { 69d });

                    tsf.Append("Heartrate", baseline.AddMonths(1).AddMinutes(61), "watches/apple", new[] { 159d });
                    tsf.Append("Heartrate", baseline.AddMonths(1).AddMinutes(62), "watches/fitbit", new[] { 179d });
                    tsf.Append("Heartrate", baseline.AddMonths(1).AddMinutes(63), "watches/fitbit", new[] { 169d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from p in session.Query<Person>()
                                where p.Age > 21
                                select new
                                {
                                    Heartrate = RavenQuery.TimeSeries(p, "Heartrate", baseline, baseline.AddMonths(2))
                                        .LoadTag<Watch>()
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
                    Assert.Equal(79, agg[0].Avg[0]);

                    Assert.Equal(179, agg[1].Max[0]);
                    Assert.Equal(174, agg[1].Avg[0]);

                }
            }
        }


    }
}
