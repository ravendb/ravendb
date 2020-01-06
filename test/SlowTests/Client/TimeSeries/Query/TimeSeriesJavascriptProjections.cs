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

        [Fact]
        public void TimeSeriesAggregationInsideJsProjection_RawQuery()
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
        public void TimeSeriesAggregationInsideJsProjection_RawQuery_WithLoadedTag()
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
                    var timeSeriesQuery =
@"from p.Heartrate between $start and $end 
load Tag as src
where Values[0] > 70 and src.Accuracy >= 2
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
                    Assert.Equal(3, result[0].HeartRate.Count);
                    Assert.Equal("Oren Ayende", result[0].Name);

                    var agg = result[0].HeartRate.Results;

                    Assert.Equal(2, agg.Length);

                    Assert.Equal(79, agg[0].Max[0]);
                    Assert.Equal(79, agg[0].Avg[0]);

                    Assert.Equal(179, agg[1].Max[0]);
                    Assert.Equal(174, agg[1].Avg[0]);

                }
            }
        }

        [Fact]
        public void TimeSeriesAggregationInsideJsProjection_RawQuery_FromLoadedDocument()
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
                    var timeSeriesQuery =
@"from company.Heartrate between $start and $end 
load Tag as src
where Tag = 'tags/1'
group by '1 month' 
select max(), avg()";

                    var rawQuery = session.Advanced.RawQuery<QueryResult>(
@"from People as p
where p.Age > 21
load p.WorksAt as company
select {
    Heartrate : timeseries($ts)
}
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddYears(1))
                        .AddParameter("ts", timeSeriesQuery);

                    var result = rawQuery.ToList();

                    Assert.Equal(1, result.Count);

                    Assert.Equal(4, result[0].HeartRate.Count);

                    var agg = result[0].HeartRate.Results;

                    Assert.Equal(2, agg.Length);

                    Assert.Equal(12.79, agg[0].Max[0]);
                    Assert.Equal(12.69, agg[0].Avg[0]);

                    Assert.Equal(13.69, agg[1].Max[0]);
                    Assert.Equal(13.64, agg[1].Avg[0]);

                }
            }
        }

        [Fact]
        public void TimeSeriesAggregationInsideJsProjection_UsingLinq()
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
        public void TimeSeriesAggregationInsideJsProjection_UsingLinq_FromLoadedDocument()
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
                    Assert.Equal(12.69, agg[0].Avg[0]);

                    Assert.Equal(13.69, agg[1].Max[0]);
                    Assert.Equal(13.64, agg[1].Avg[0]);

                }
            }
        }

        [Fact]
        public void TimeSeriesAggregationInsideJsProjection_UsingLinq_WithLoadedTag()
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

        [Fact]
        public void TimeSeriesAggregationInsideJsProjection_UsingLinq_CanDefineTmeSeriesInsideLet()
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
                    Assert.Equal(69, agg[0].Avg[0]);

                    Assert.Equal(179, agg[1].Max[0]);
                    Assert.Equal(179, agg[1].Avg[0]);

                }
            }
        }


        [Fact]
        public void CanUseTimeSeriesQueryResultAsArgumentToJavascriptDeclaredFunction()
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
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddYears(1));

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

                    tsf.Append("Heartrate", baseline.AddMonths(2).AddMinutes(61), "watches/apple", new[] { 259d });
                    tsf.Append("Heartrate", baseline.AddMonths(2).AddMinutes(62), "watches/fitbit", new[] { 279d });
                    tsf.Append("Heartrate", baseline.AddMonths(2).AddMinutes(63), "watches/fitbit", new[] { 269d });

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
            Avg : current.Avg[0]
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
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddYears(1));

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
        public void CanCallTimeSeriesDeclaredFunctionFromJavascriptProjection()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

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

                    var tsf = session.TimeSeriesFor("people/1");

                    tsf.Append("Stocks", baseline.AddMinutes(61), "tags/1", new[] { 59d });
                    tsf.Append("Stocks", baseline.AddMinutes(62), "tags/2", new[] { 79d });
                    tsf.Append("Stocks", baseline.AddMinutes(63), "tags/2", new[] { 69d });

                    tsf.Append("Stocks", baseline.AddMonths(1).AddMinutes(61), "tags/1", new[] { 159d });
                    tsf.Append("Stocks", baseline.AddMonths(1).AddMinutes(62), "tags/1", new[] { 179d });
                    tsf.Append("Stocks", baseline.AddMonths(1).AddMinutes(63), "tags/2", new[] { 169d });

                    tsf = session.TimeSeriesFor("companies/1");

                    tsf.Append("Stocks", baseline.AddMinutes(61), "tags/1", new[] { 559d });
                    tsf.Append("Stocks", baseline.AddMinutes(62), "tags/2", new[] { 579d });
                    tsf.Append("Stocks", baseline.AddMinutes(63), "tags/2", new[] { 569d });

                    tsf.Append("Stocks", baseline.AddMonths(1).AddMinutes(61), "tags/1", new[] { 659d });
                    tsf.Append("Stocks", baseline.AddMonths(1).AddMinutes(62), "tags/1", new[] { 679d });
                    tsf.Append("Stocks", baseline.AddMonths(1).AddMinutes(63), "tags/2", new[] { 669d });

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
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddYears(1));

                    var result = rawQuery.First();

                    Assert.Equal(3, result.Series.Count);

                    Assert.Equal(new [] { 59d }, result.Series.Results[0].Values);
                    Assert.Equal("tags/1", result.Series.Results[0].Tag);
                    Assert.Equal(baseline.AddMinutes(61), result.Series.Results[0].Timestamp);

                    Assert.Equal(new[] { 159d }, result.Series.Results[1].Values);
                    Assert.Equal("tags/1", result.Series.Results[1].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), result.Series.Results[1].Timestamp);

                    Assert.Equal(new[] { 179d }, result.Series.Results[2].Values);
                    Assert.Equal("tags/1", result.Series.Results[2].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(62), result.Series.Results[2].Timestamp);

                    Assert.Equal(3, result.Series2.Count);

                    Assert.Equal(new[] { 559d }, result.Series2.Results[0].Values);
                    Assert.Equal("tags/1", result.Series2.Results[0].Tag);
                    Assert.Equal(baseline.AddMinutes(61), result.Series2.Results[0].Timestamp);

                    Assert.Equal(new[] { 659d }, result.Series2.Results[1].Values);
                    Assert.Equal("tags/1", result.Series2.Results[1].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), result.Series2.Results[1].Timestamp);

                    Assert.Equal(new[] { 679d }, result.Series2.Results[2].Values);
                    Assert.Equal("tags/1", result.Series2.Results[2].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(62), result.Series2.Results[2].Timestamp);

                    Assert.Equal(3, result.Series3.Length);

                    Assert.Equal(559d, result.Series3[0]);
                    Assert.Equal(659d, result.Series3[1]);
                    Assert.Equal(679d, result.Series3[2]);

                }
            }
        }

        [Fact]
        public void CanPassSeriesNameAsParameterToTimeSeriesDeclaredFunction()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

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

                    var tsf = session.TimeSeriesFor("people/1");

                    tsf.Append("Heartrate", baseline.AddMinutes(61), "tags/1", new[] { 59d });
                    tsf.Append("Heartrate", baseline.AddMinutes(62), "tags/2", new[] { 79d });
                    tsf.Append("Heartrate", baseline.AddMinutes(63), "tags/2", new[] { 69d });

                    tsf.Append("Heartrate", baseline.AddMonths(1).AddMinutes(61), "tags/1", new[] { 159d });
                    tsf.Append("Heartrate", baseline.AddMonths(1).AddMinutes(62), "tags/1", new[] { 179d });
                    tsf.Append("Heartrate", baseline.AddMonths(1).AddMinutes(63), "tags/2", new[] { 169d });

                    tsf.Append("Stocks", baseline.AddMinutes(61), "tags/1", new[] { 559d });
                    tsf.Append("Stocks", baseline.AddMinutes(62), "tags/2", new[] { 579d });
                    tsf.Append("Stocks", baseline.AddMinutes(63), "tags/2", new[] { 569d });

                    tsf.Append("Stocks", baseline.AddMonths(1).AddMinutes(61), "tags/1", new[] { 659d });
                    tsf.Append("Stocks", baseline.AddMonths(1).AddMinutes(62), "tags/1", new[] { 679d });
                    tsf.Append("Stocks", baseline.AddMonths(1).AddMinutes(63), "tags/2", new[] { 669d });

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
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddYears(1));

                    var result = rawQuery.First();

                    Assert.Equal(3, result.Heartrate.Count);

                    Assert.Equal(new[] { 59d }, result.Heartrate.Results[0].Values);
                    Assert.Equal("tags/1", result.Heartrate.Results[0].Tag);
                    Assert.Equal(baseline.AddMinutes(61), result.Heartrate.Results[0].Timestamp);

                    Assert.Equal(new[] { 159d }, result.Heartrate.Results[1].Values);
                    Assert.Equal("tags/1", result.Heartrate.Results[1].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), result.Heartrate.Results[1].Timestamp);

                    Assert.Equal(new[] { 179d }, result.Heartrate.Results[2].Values);
                    Assert.Equal("tags/1", result.Heartrate.Results[2].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(62), result.Heartrate.Results[2].Timestamp);

                    Assert.Equal(3, result.Stocks.Count);

                    Assert.Equal(new[] { 559d }, result.Stocks.Results[0].Values);
                    Assert.Equal("tags/1", result.Stocks.Results[0].Tag);
                    Assert.Equal(baseline.AddMinutes(61), result.Stocks.Results[0].Timestamp);

                    Assert.Equal(new[] { 659d }, result.Stocks.Results[1].Values);
                    Assert.Equal("tags/1", result.Stocks.Results[1].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), result.Stocks.Results[1].Timestamp);

                    Assert.Equal(new[] { 679d }, result.Stocks.Results[2].Values);
                    Assert.Equal("tags/1", result.Stocks.Results[2].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(62), result.Stocks.Results[2].Timestamp);


                }
            }
        }

        [Fact]
        public void CanCallTimeSeriesDeclaredFunctionFromJavascriptDeclaredFunction()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

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

                    var tsf = session.TimeSeriesFor("people/1");

                    tsf.Append("Stocks", baseline.AddMinutes(61), "tags/1", new[] { 59d });
                    tsf.Append("Stocks", baseline.AddMinutes(62), "tags/2", new[] { 79d });
                    tsf.Append("Stocks", baseline.AddMinutes(63), "tags/2", new[] { 69d });

                    tsf.Append("Stocks", baseline.AddMonths(1).AddMinutes(61), "tags/1", new[] { 159d });
                    tsf.Append("Stocks", baseline.AddMonths(1).AddMinutes(62), "tags/1", new[] { 179d });
                    tsf.Append("Stocks", baseline.AddMonths(1).AddMinutes(63), "tags/2", new[] { 169d });

                    tsf = session.TimeSeriesFor("companies/1");

                    tsf.Append("Stocks", baseline.AddMinutes(61), "tags/1", new[] { 559d });
                    tsf.Append("Stocks", baseline.AddMinutes(62), "tags/2", new[] { 579d });
                    tsf.Append("Stocks", baseline.AddMinutes(63), "tags/2", new[] { 569d });

                    tsf.Append("Stocks", baseline.AddMonths(1).AddMinutes(61), "tags/1", new[] { 659d });
                    tsf.Append("Stocks", baseline.AddMonths(1).AddMinutes(62), "tags/1", new[] { 679d });
                    tsf.Append("Stocks", baseline.AddMonths(1).AddMinutes(63), "tags/2", new[] { 669d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var rawQuery = session.Advanced.RawQuery<CustomRawQueryResult3>(
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
    
}
from People as p
where p.Age > 49
load p.WorksAt as Company
select {
    Series: out(p),
    Series2: out(Company),
    Series3: out(Company).Results.map(x=>x.Values[0]),
}")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddYears(1));

                    var result = rawQuery.First();

                    Assert.Equal(3, result.Series.Count);

                    Assert.Equal(new[] { 59d }, result.Series.Results[0].Values);
                    Assert.Equal("tags/1", result.Series.Results[0].Tag);
                    Assert.Equal(baseline.AddMinutes(61), result.Series.Results[0].Timestamp);

                    Assert.Equal(new[] { 159d }, result.Series.Results[1].Values);
                    Assert.Equal("tags/1", result.Series.Results[1].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), result.Series.Results[1].Timestamp);

                    Assert.Equal(new[] { 179d }, result.Series.Results[2].Values);
                    Assert.Equal("tags/1", result.Series.Results[2].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(62), result.Series.Results[2].Timestamp);

                    Assert.Equal(3, result.Series2.Count);

                    Assert.Equal(new[] { 559d }, result.Series2.Results[0].Values);
                    Assert.Equal("tags/1", result.Series2.Results[0].Tag);
                    Assert.Equal(baseline.AddMinutes(61), result.Series2.Results[0].Timestamp);

                    Assert.Equal(new[] { 659d }, result.Series2.Results[1].Values);
                    Assert.Equal("tags/1", result.Series2.Results[1].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), result.Series2.Results[1].Timestamp);

                    Assert.Equal(new[] { 679d }, result.Series2.Results[2].Values);
                    Assert.Equal("tags/1", result.Series2.Results[2].Tag);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(62), result.Series2.Results[2].Timestamp);

                    Assert.Equal(3, result.Series3.Length);

                    Assert.Equal(559d, result.Series3[0]);
                    Assert.Equal(659d, result.Series3[1]);
                    Assert.Equal(679d, result.Series3[2]);

                }
            }
        }


    }
}
