using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Linq;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Query
{
    public class TimeSeriesLinqQuery : RavenTestBase
    {
        public TimeSeriesLinqQuery(ITestOutputHelper output) : base(output)
        {
        }

        private class Person
        {
            public string Name { get; set; }

            public int Age { get; set; }

            public string WorksAt { get; set; }
        }

        private class Watch
        {
            public string Manufacturer { get; set; }

            public double Accuracy { get; set; }

            //public AdditionalData AdditionalData { get; set; }

            public long Min { get; set; }

            public long Max { get; set; }

            public bool IsoCompliant;

            public DateTime EndOfWarranty { get; set; }

        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_UsingLinq()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Oren",
                        Age = 35
                    }, "users/ayende");

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(62), "watches/fitbit", new[] { 79d });

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMonths(1).AddMinutes(61), "watches/fitbit", new[] { 159d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMonths(1).AddMinutes(62), "watches/fitbit", new[] { 179d });

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMonths(1).AddMinutes(63), "watches/fitbit", new[] { 169d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Where(u => u.Age > 21)
                        .Select(u => RavenQuery.TimeSeries(u, "Heartrate")
                            .Where(ts => ts.Tag == "watches/fitbit")
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
                    Assert.Equal(69, agg[0].Avg[0]);

                    Assert.Equal(179, agg[1].Max[0]);
                    Assert.Equal(169, agg[1].Avg[0]);

                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WithFromAndToDates()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Oren",
                        Age = 35
                    }, "users/ayende");

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(62), "watches/fitbit", new[] { 79d });

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMonths(1).AddMinutes(61), "watches/fitbit", new[] { 159d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMonths(1).AddMinutes(62), "watches/fitbit", new[] { 179d });

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMonths(1).AddMinutes(63), "watches/fitbit", new[] { 169d });

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMonths(2).AddMinutes(61), "watches/fitbit", new[] { 259d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMonths(2).AddMinutes(62), "watches/fitbit", new[] { 279d });

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMonths(2).AddMinutes(63), "watches/fitbit", new[] { 269d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Where(u => u.Age > 21)
                        .Select(u => RavenQuery.TimeSeries(u, "Heartrate", baseline, baseline.AddMonths(2))
                            .Where(ts => ts.Tag == "watches/fitbit")
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
                    Assert.Equal(69, agg[0].Avg[0]);

                    Assert.Equal(179, agg[1].Max[0]);
                    Assert.Equal(169, agg[1].Avg[0]);

                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereIn()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Age = 25
                    }, "people/1");

                    var tsf = session.TimeSeriesFor("people/1");

                    tsf.Append("Heartrate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                    tsf.Append("Heartrate", baseline.AddMinutes(62), "watches/apple", new[] { 79d });
                    tsf.Append("Heartrate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });

                    tsf.Append("Heartrate", baseline.AddMonths(1).AddMinutes(61), "watches/apple", new[] { 159d });
                    tsf.Append("Heartrate", baseline.AddMonths(1).AddMinutes(62), "watches/sony", new[] { 179d });
                    tsf.Append("Heartrate", baseline.AddMonths(1).AddMinutes(63), "watches/fitbit", new[] { 169d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Where(p => p.Age > 21)
                        .Select(p => RavenQuery.TimeSeries(p, "Heartrate", baseline, baseline.AddMonths(2))
                            .Where(ts => ts.Tag.In(new[] {"watches/fitbit", "watches/apple"}))
                            .GroupBy("1 month")
                            .Select(g => new
                            {
                                Avg = g.Average(), 
                                Max = g.Max()
                            })
                            .ToList());

                    var result = query.First();

                    Assert.Equal(5, result.Count);

                    var agg = result.Results;

                    Assert.Equal(2, agg.Length);

                    Assert.Equal(79, agg[0].Max[0]);
                    Assert.Equal(69, agg[0].Avg[0]);
                    Assert.Equal(3, agg[0].Count[0]);

                    Assert.Equal(169, agg[1].Max[0]);
                    Assert.Equal(164, agg[1].Avg[0]);

                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesRaw_UsingLinq()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Oren",
                        Age = 35
                    }, "users/ayende");

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(62), "watches/fitbit", new[] { 79d });

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMonths(1).AddMinutes(61), "watches/apple", new[] { 159d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMonths(1).AddMinutes(62), "watches/fitbit", new[] { 179d });

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMonths(1).AddMinutes(63), "watches/fitbit", new[] { 169d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Where(u => u.Age > 21)
                        .Select(u => RavenQuery.TimeSeries(u, "Heartrate")
                            .Where(ts => ts.Tag == "watches/fitbit")
                            .ToList());

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(5, result[0].Count);

                    var timeSeriesValues = result[0].Results;

                    Assert.Equal(59, timeSeriesValues[0].Values[0]);
                    Assert.Equal(baseline.AddMinutes(61), timeSeriesValues[0].Timestamp);

                    Assert.Equal(79, timeSeriesValues[1].Values[0]);
                    Assert.Equal(baseline.AddMinutes(62), timeSeriesValues[1].Timestamp);

                    Assert.Equal(69, timeSeriesValues[2].Values[0]);
                    Assert.Equal(baseline.AddMinutes(63), timeSeriesValues[2].Timestamp);

                    Assert.Equal(179, timeSeriesValues[3].Values[0]);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(62), timeSeriesValues[3].Timestamp);

                    Assert.Equal(169, timeSeriesValues[4].Values[0]);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(63), timeSeriesValues[4].Timestamp);

                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_FromLoadedDocument()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

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
                                select RavenQuery.TimeSeries(company, "Stock", baseline, baseline.AddMonths(2))
                                    .Where(ts => ts.Tag == "tags/1")
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
                    Assert.Equal(12.69, agg[0].Avg[0]);

                    Assert.Equal(13.69, agg[1].Max[0]);
                    Assert.Equal(13.64, agg[1].Avg[0]);

                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereOnLoadedTag()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Age = 25
                    }, "people/1");

                    session.Store(new Watch
                    {
                        Accuracy = 70
                    }, "watches/fitbit");

                    session.Store(new Watch
                    {
                        Accuracy = 90
                    }, "watches/apple");

                    session.Store(new Watch
                    {
                        Accuracy = 180
                    }, "watches/sony");

                    var tsf = session.TimeSeriesFor("people/1");

                    tsf.Append("Heartrate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                    tsf.Append("Heartrate", baseline.AddMinutes(62), "watches/apple", new[] { 79d });
                    tsf.Append("Heartrate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });

                    tsf.Append("Heartrate", baseline.AddMonths(1).AddMinutes(61), "watches/apple", new[] { 159d });
                    tsf.Append("Heartrate", baseline.AddMonths(1).AddMinutes(62), "watches/sony", new[] { 179d });
                    tsf.Append("Heartrate", baseline.AddMonths(1).AddMinutes(63), "watches/fitbit", new[] { 169d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Where(p => p.Age > 21)
                        .Select(p => RavenQuery.TimeSeries(p, "Heartrate", baseline, baseline.AddMonths(2))
                            .LoadTag<Watch>()
                            .Where((ts, src) => ts.Values[0] <= src.Accuracy)
                            .GroupBy("1 month")
                            .Select(g => new
                            {
                                Avg = g.Average(),
                                Max = g.Max()
                            })
                            .ToList());

                    var result = query.First();

                    Assert.Equal(4, result.Count);

                    var agg = result.Results;

                    Assert.Equal(2, agg.Length);

                    Assert.Equal(79, agg[0].Max[0]);
                    Assert.Equal(69, agg[0].Avg[0]);
                    Assert.Equal(3, agg[0].Count[0]);

                    Assert.Equal(179, agg[1].Max[0]);
                    Assert.Equal(179, agg[1].Avg[0]);

                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_StronglyTypedGroupBy()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Oren",
                        Age = 35
                    }, "users/ayende");

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(62), "watches/fitbit", new[] { 79d });

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMonths(1).AddMinutes(61), "watches/fitbit", new[] { 159d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMonths(1).AddMinutes(62), "watches/fitbit", new[] { 179d });

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMonths(1).AddMinutes(63), "watches/fitbit", new[] { 169d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Where(u => u.Age > 21)
                        .Select(u => RavenQuery.TimeSeries(u, "Heartrate")
                            .Where(ts => ts.Tag == "watches/fitbit")
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
                    Assert.Equal(69, agg[0].Avg[0]);

                    Assert.Equal(179, agg[1].Max[0]);
                    Assert.Equal(169, agg[1].Avg[0]);

                }
            }
        }

    }
}
