using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries.TimeSeries;
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
            public string Id { get; set; }

            public string Name { get; set; }

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

        private class QueryResult2
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public TimeSeriesRawResult HeartRate { get; set; }

            public TimeSeriesRawResult BloodPressure { get; set; }
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
        public async Task CanQueryTimeSeriesAggregation_Async()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
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

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
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

                    var result = await query.ToListAsync();

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
        public void CanQueryTimeSeriesAggregation_NoAlias()
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
                        .Select(u => RavenQuery.TimeSeries("Heartrate")
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

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereTagIn()
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
        public void CanQueryTimeSeriesAggregation_WhereInOnLoadedTag()
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
                        Manufacturer = "Fitbit"
                    }, "watches/fitbit");

                    session.Store(new Watch
                    {
                        Manufacturer = "Apple"
                    }, "watches/apple");

                    session.Store(new Watch
                    {
                        Manufacturer = "Sony"
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
                            .Where((ts, src) => src.Manufacturer.In(new[] { "Fitbit", "Apple" }))
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

        [Fact]
        public void CanQueryTimeSeriesAggregation_SimpleProjectionToAnonymousClass()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "ayende",
                        Age = 30
                    }, "users/ayende");

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(62), "watches/apple", new[] { 79d });

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
                        .Select(u => new
                        {
                            u.Name,
                            Heartrate = RavenQuery.TimeSeries(u, "Heartrate")
                                .Where(ts => ts.Tag == "watches/fitbit")
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
                    Assert.Equal(64, agg[0].Avg[0]);

                    Assert.Equal(179, agg[1].Max[0]);
                    Assert.Equal(169, agg[1].Min[0]);
                    Assert.Equal(174, agg[1].Avg[0]);

                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_SimpleMemberInitProjection()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "ayende",
                        Age = 30
                    }, "users/ayende");

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(62), "watches/apple", new[] { 79d });

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
                        .Select(u => new QueryResult
                        {
                            Id = u.Id,
                            Name = u.Name,
                            HeartRate = RavenQuery.TimeSeries(u, "Heartrate")
                                .Where(ts => ts.Tag == "watches/fitbit")
                                .GroupBy(g => g.Months(1))
                                .Select(g => new
                                {
                                    Avg = g.Average(),
                                    Max = g.Max(),
                                    Min = g.Min()
                                })
                                .ToList()
                        });

                    var result = query.First();

                    Assert.Equal("ayende", result.Name);
                    Assert.Equal("users/ayende", result.Id);

                    Assert.Equal(4, result.HeartRate.Count);

                    var aggregation = result.HeartRate.Results;

                    Assert.Equal(2, aggregation.Length);

                    Assert.Equal(69, aggregation[0].Max[0]);
                    Assert.Equal(59, aggregation[0].Min[0]);
                    Assert.Equal(64, aggregation[0].Avg[0]);

                    Assert.Equal(179, aggregation[1].Max[0]);
                    Assert.Equal(169, aggregation[1].Min[0]);
                    Assert.Equal(174, aggregation[1].Avg[0]);

                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_MultipleSeries()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "ayende"
                    }, "people/1");

                    var tsf = session.TimeSeriesFor("people/1");
                    tsf.Append("Heartrate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                    tsf.Append("Heartrate", baseline.AddMinutes(62), "watches/apple", new[] { 79d });
                    tsf.Append("Heartrate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });

                    tsf.Append("Heartrate", baseline.AddMonths(1).AddMinutes(61), "watches/apple", new[] { 159d });
                    tsf.Append("Heartrate", baseline.AddMonths(1).AddMinutes(62), "watches/fitbit", new[] { 179d });
                    tsf.Append("Heartrate", baseline.AddMonths(1).AddMinutes(63), "watches/fitbit", new[] { 169d });

                    tsf.Append("BloodPressure", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                    tsf.Append("BloodPressure", baseline.AddMinutes(62), "watches/apple", new[] { 79d });
                    tsf.Append("BloodPressure", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });

                    tsf.Append("BloodPressure", baseline.AddMonths(1).AddMinutes(61), "watches/apple", new[] { 159d });
                    tsf.Append("BloodPressure", baseline.AddMonths(1).AddMinutes(62), "watches/fitbit", new[] { 179d });
                    tsf.Append("BloodPressure", baseline.AddMonths(1).AddMinutes(63), "watches/fitbit", new[] { 169d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Select(p => new QueryResult
                        {
                            Id = p.Id,
                            Name = p.Name,
                            HeartRate = RavenQuery.TimeSeries(p, "Heartrate")
                                .Where(ts => ts.Tag == "watches/fitbit")
                                .GroupBy(g => g.Months(1))
                                .Select(g => new
                                {
                                    Avg = g.Average(),
                                    Max = g.Max()
                                })
                                .ToList(),
                            BloodPressure = RavenQuery.TimeSeries(p, "BloodPressure")
                                .Where(ts => ts.Tag == "watches/apple")
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
                    Assert.Equal(64, aggregation[0].Avg[0]);

                    Assert.Equal(179, aggregation[1].Max[0]);
                    Assert.Equal(174, aggregation[1].Avg[0]);

                    Assert.Equal(2, result.BloodPressure.Count);

                    aggregation = result.BloodPressure.Results;

                    Assert.Equal(2, aggregation.Length);

                    Assert.Equal(79, aggregation[0].Max[0]);
                    Assert.Equal(79, aggregation[0].Avg[0]);

                    Assert.Equal(159, aggregation[1].Max[0]);
                    Assert.Equal(159, aggregation[1].Avg[0]);

                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereOnVariable()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person(), "people/1");

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
                    double val = 70;

                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, "Heartrate", baseline, baseline.AddMonths(2))
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
                    Assert.Equal(79, agg[0].Avg[0]);
                    Assert.Equal(79, agg[0].Min[0]);

                    Assert.Equal(179, agg[1].Max[0]);
                    Assert.Equal(169, agg[1].Avg[0]);
                    Assert.Equal(159, agg[1].Min[0]);


                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WithNameVariable()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person(), "people/1");

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
                    string timeseriesName = "Heartrate";

                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, timeseriesName)
                            .Where(ts => ts.Values[0] > 70)
                            .GroupBy(g => g.Months(1))
                            .Select(g => new
                            {
                                Avg = g.Average(),
                                Max = g.Max(),
                                Min = g.Min()
                            })
                            .ToList());

                    var result = query.First();

                    Assert.Equal(4, result.Count);

                    var agg = result.Results;

                    Assert.Equal(2, agg.Length);

                    Assert.Equal(79, agg[0].Max[0]);
                    Assert.Equal(79, agg[0].Avg[0]);
                    Assert.Equal(79, agg[0].Min[0]);

                    Assert.Equal(179, agg[1].Max[0]);
                    Assert.Equal(169, agg[1].Avg[0]);
                    Assert.Equal(159, agg[1].Min[0]);


                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereOr()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person(), "people/1");

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
                        .Select(p => RavenQuery.TimeSeries(p, "Heartrate", baseline, baseline.AddMonths(2))
                            .Where(ts => ts.Values[0] < 70 || ts.Tag == "watches/apple")
                            .GroupBy(g => g.Months(1))
                            .Select(g => new
                            {
                                Avg = g.Average(),
                                Max = g.Max(),
                                Min = g.Min()
                            })
                            .ToList());


                    var result = query.First();

                    Assert.Equal(4, result.Count);

                    var agg = result.Results;

                    Assert.Equal(2, agg.Length);

                    Assert.Equal(79, agg[0].Max[0]);
                    Assert.Equal(69, agg[0].Avg[0]);
                    Assert.Equal(59, agg[0].Min[0]);

                    Assert.Equal(159, agg[1].Max[0]);
                    Assert.Equal(159, agg[1].Avg[0]);
                    Assert.Equal(159, agg[1].Min[0]);


                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereAnd()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person(), "people/1");

                    session.Store(new Watch
                    {
                        Accuracy = 2.5
                    }, "watches/fitbit");

                    session.Store(new Watch
                    {
                        Accuracy = 1.8
                    }, "watches/apple");

                    var tsf = session.TimeSeriesFor("people/1");

                    tsf.Append("Heartrate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                    tsf.Append("Heartrate", baseline.AddMinutes(62), "watches/apple", new[] { 79d });
                    tsf.Append("Heartrate", baseline.AddMinutes(63), null, new[] { 69d });

                    tsf.Append("Heartrate", baseline.AddMonths(1).AddMinutes(61), null, new[] { 159d });
                    tsf.Append("Heartrate", baseline.AddMonths(1).AddMinutes(62), "watches/sony", new[] { 179d });
                    tsf.Append("Heartrate", baseline.AddMonths(1).AddMinutes(63), "watches/fitbit", new[] { 169d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, "Heartrate", baseline, baseline.AddMonths(2))
                            .LoadTag<Watch>()
                            .Where((ts, src) => src != null && src.Accuracy > 2.2)
                            .GroupBy(g => g.Months(1))
                            .Select(g => new
                            {
                                Avg = g.Average(),
                                Max = g.Max(),
                                Min = g.Min()
                            })
                            .ToList());

                    var result = query.First();

                    Assert.Equal(2, result.Count);

                    var agg = result.Results;

                    Assert.Equal(2, agg.Length);

                    Assert.Equal(59, agg[0].Max[0]);
                    Assert.Equal(59, agg[0].Avg[0]);
                    Assert.Equal(59, agg[0].Min[0]);

                    Assert.Equal(169, agg[1].Max[0]);
                    Assert.Equal(169, agg[1].Avg[0]);
                    Assert.Equal(169, agg[1].Min[0]);


                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_SelectSingleCall()
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
        public void ShouldUseQueryParameters()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person(), "people/1");

                    session.Store(new Watch
                    {
                        Accuracy = 2.5
                    }, "watches/fitbit");

                    session.Store(new Watch
                    {
                        Accuracy = 1.8
                    }, "watches/apple");

                    var tsf = session.TimeSeriesFor("people/1");

                    tsf.Append("Heartrate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                    tsf.Append("Heartrate", baseline.AddMinutes(62), "watches/apple", new[] { 79d });
                    tsf.Append("Heartrate", baseline.AddMinutes(63), null, new[] { 69d });

                    tsf.Append("Heartrate", baseline.AddMonths(1).AddMinutes(61), null, new[] { 159d });
                    tsf.Append("Heartrate", baseline.AddMonths(1).AddMinutes(62), "watches/sony", new[] { 179d });
                    tsf.Append("Heartrate", baseline.AddMonths(1).AddMinutes(63), "watches/fitbit", new[] { 169d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var d = 70d;
                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, "Heartrate", baseline, baseline.AddMonths(2))
                            .LoadTag<Watch>()
                            .Where((ts, src) => (src != null && src.Accuracy > 2.2) || 
                                                (ts.Tag != "watches/sony" && ts.Values[0] > d))
                            .GroupBy(g => g.Months(1))
                            .Select(g => new
                            {
                                Avg = g.Average(),
                                Max = g.Max(),
                                Min = g.Min()
                            })
                            .ToList());


                    var queryString = query.ToString();

                    Assert.Contains("src != $p0", queryString);
                    Assert.Contains("src.Accuracy > $p1", queryString);
                    Assert.Contains("Tag != $p2", queryString);
                    Assert.Contains("Values[0] > $p3", queryString);
                    Assert.Contains("between $p4 and $p5", queryString);

                    var result = query.First();

                    Assert.Equal(3, result.Count);

                    var agg = result.Results;

                    Assert.Equal(2, agg.Length);

                    Assert.Equal(79, agg[0].Max[0]);
                    Assert.Equal(69, agg[0].Avg[0]);
                    Assert.Equal(59, agg[0].Min[0]);

                    Assert.Equal(169, agg[1].Max[0]);
                    Assert.Equal(169, agg[1].Avg[0]);
                    Assert.Equal(169, agg[1].Min[0]);

                }
            }
        }

        [Fact]
        public void ShouldThrowOnGroupByWithoutSelect()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, "Heartrate")
                            .Where(ts=> ts.Tag != "watches/sony")
                            .GroupBy(g => g.Months(1))
                            .ToList());

                    var ex = Assert.Throws<InvalidOperationException>(() => query.ToList());
                    Assert.NotNull(ex.InnerException);
                    Assert.Contains("Missing Select call. Cannot have GroupBy without Select in Time Series functions ", ex.InnerException.Message);

                }
            }
        }

        [Fact]
        public void ShouldThrowOnMultipleCalls()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, "Heartrate")
                            .Where(ts => ts.Tag != "watches/sony")
                            .LoadTag<Watch>()
                            .Where((tag, src) => src != null)
                            .GroupBy(g => g.Months(1))
                            .ToList());

                    var ex = Assert.Throws<InvalidOperationException>(() => query.ToList());
                    Assert.NotNull(ex.InnerException);
                    Assert.Contains("Cannot have multiple Where calls in TimeSeries functions ", ex.InnerException.Message);

                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, "Heartrate")
                            .Where(ts => ts.Tag != "watches/sony")
                            .GroupBy(g => g.Months(1))
                            .Select(g => new
                            {
                                Max = g.Max()
                            })
                            .Select(m => m.First())
                            .ToList());

                    var ex = Assert.Throws<InvalidOperationException>(() => query.ToList());
                    Assert.NotNull(ex.InnerException);
                    Assert.Contains("Cannot have multiple Select calls in TimeSeries functions ", ex.InnerException.Message);

                }
            }
        }

        [Fact]
        public void ShouldThrowOnBadReturnType()
        {
            using (var store = GetDocumentStore())
            {
                var msg = $"Time Series query expressions must return type '{nameof(TimeSeriesRawResult)}' or " +
                          $"'{nameof(TimeSeriesAggregationResult)}'. Did you forget to call 'ToList'?";

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Select(p => new
                        {
                            HeartRate = RavenQuery.TimeSeries(p, "Heartrate")
                        });

                    var ex = Assert.Throws<InvalidOperationException>(() => query.ToList());
                    Assert.Contains(msg, ex.Message);
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, "Heartrate")
                                .Where(ts => ts.Tag != "watches/fitbit"));

                    var ex = Assert.Throws<InvalidOperationException>(() => query.ToList());
                    Assert.Contains(msg, ex.Message);
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, "Heartrate")
                                .LoadTag<Watch>());

                    var ex = Assert.Throws<InvalidOperationException>(() => query.ToList());
                    Assert.Contains(msg, ex.Message);
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Select(p => new
                        {
                            HeartRate = RavenQuery.TimeSeries(p, "Heartrate")
                                .LoadTag<Watch>()
                                .Where((ts, tag) => tag != null && ts.Tag == "watches/fitbit")
                        });

                    var ex = Assert.Throws<InvalidOperationException>(() => query.ToList());
                    Assert.Contains(msg, ex.Message);
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Select(p => new
                        {
                            HeartRate = RavenQuery.TimeSeries(p, "Heartrate")
                                .Where(ts => ts.Tag != "watches/fitbit")
                                .GroupBy(g => g.Months(1))
                        });

                    var ex = Assert.Throws<InvalidOperationException>(() => query.ToList());
                    Assert.Contains(msg, ex.Message);
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Select(p => new
                        {
                            HeartRate = RavenQuery.TimeSeries(p, "Heartrate")
                                .GroupBy(g => g.Months(1))
                                .Select(x => new
                                {
                                    Max = x.Max(),
                                    Min = x.Min()
                                })
                        });

                    var ex = Assert.Throws<InvalidOperationException>(() => query.ToList());
                    Assert.Contains(msg, ex.Message);
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
                        .Append("Heartrate", baseline.AddMinutes(62), "watches/apple", new[] { 79d });

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
                            .ToList());

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(6, result[0].Count);

                    var timeSeriesValues = result[0].Results;

                    Assert.Equal(59, timeSeriesValues[0].Values[0]);
                    Assert.Equal(baseline.AddMinutes(61), timeSeriesValues[0].Timestamp);
                    Assert.Equal("watches/fitbit", timeSeriesValues[0].Tag);

                    Assert.Equal(79, timeSeriesValues[1].Values[0]);
                    Assert.Equal(baseline.AddMinutes(62), timeSeriesValues[1].Timestamp);
                    Assert.Equal("watches/apple", timeSeriesValues[1].Tag);

                    Assert.Equal(69, timeSeriesValues[2].Values[0]);
                    Assert.Equal(baseline.AddMinutes(63), timeSeriesValues[2].Timestamp);
                    Assert.Equal("watches/fitbit", timeSeriesValues[2].Tag);

                    Assert.Equal(159, timeSeriesValues[3].Values[0]);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), timeSeriesValues[3].Timestamp);
                    Assert.Equal("watches/apple", timeSeriesValues[3].Tag);

                    Assert.Equal(179, timeSeriesValues[4].Values[0]);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(62), timeSeriesValues[4].Timestamp);
                    Assert.Equal("watches/fitbit", timeSeriesValues[4].Tag);

                    Assert.Equal(169, timeSeriesValues[5].Values[0]);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(63), timeSeriesValues[5].Timestamp);
                    Assert.Equal("watches/fitbit", timeSeriesValues[5].Tag);

                }
            }
        }

        [Fact]
        public async Task CanQueryTimeSeriesRaw_Async()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
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

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Query<User>()
                        .Where(u => u.Age > 21)
                        .Select(u => RavenQuery.TimeSeries(u, "Heartrate")
                            .Where(ts => ts.Tag == "watches/fitbit")
                            .ToList());

                    var result = await query.ToListAsync();

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
        public void CanQueryTimeSeriesRaw_WithFromAndToDates()
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
                            .ToList());

                    var result = query.First();

                    Assert.Equal(5, result.Count);

                    var timeSeriesValues = result.Results;

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
        public void CanQueryTimeSeriesRaw_WhereTagIn()
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
                            .Where(ts => ts.Tag.In(new[] { "watches/fitbit", "watches/apple" }))
                            .ToList());

                    var result = query.First();
                    Assert.Equal(5, result.Count);

                    var timeSeriesValues = result.Results;

                    Assert.Equal(59, timeSeriesValues[0].Values[0]);
                    Assert.Equal(baseline.AddMinutes(61), timeSeriesValues[0].Timestamp);

                    Assert.Equal(79, timeSeriesValues[1].Values[0]);
                    Assert.Equal(baseline.AddMinutes(62), timeSeriesValues[1].Timestamp);

                    Assert.Equal(69, timeSeriesValues[2].Values[0]);
                    Assert.Equal(baseline.AddMinutes(63), timeSeriesValues[2].Timestamp);

                    Assert.Equal(159, timeSeriesValues[3].Values[0]);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), timeSeriesValues[3].Timestamp);

                    Assert.Equal(169, timeSeriesValues[4].Values[0]);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(63), timeSeriesValues[4].Timestamp);

                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesRaw_FromLoadedDocument()
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
                                    .ToList();

                    var queryResult = query.First();

                    Assert.Equal(4, queryResult.Count);

                    var timeSeriesValues = queryResult.Results;

                    Assert.Equal(12.59, timeSeriesValues[0].Values[0]);
                    Assert.Equal(baseline.AddMinutes(61), timeSeriesValues[0].Timestamp);

                    Assert.Equal(12.79, timeSeriesValues[1].Values[0]);
                    Assert.Equal(baseline.AddMinutes(62), timeSeriesValues[1].Timestamp);

                    Assert.Equal(13.59, timeSeriesValues[2].Values[0]);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), timeSeriesValues[2].Timestamp);

                    Assert.Equal(13.69, timeSeriesValues[3].Values[0]);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(63), timeSeriesValues[3].Timestamp);

                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesRaw_WhereOnLoadedTag()
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
                            .ToList());

                    var queryResult = query.First();

                    Assert.Equal(4, queryResult.Count);

                    var timeSeriesValues = queryResult.Results;

                    Assert.Equal(59, timeSeriesValues[0].Values[0]);
                    Assert.Equal(baseline.AddMinutes(61), timeSeriesValues[0].Timestamp);

                    Assert.Equal(79, timeSeriesValues[1].Values[0]);
                    Assert.Equal(baseline.AddMinutes(62), timeSeriesValues[1].Timestamp);

                    Assert.Equal(69, timeSeriesValues[2].Values[0]);
                    Assert.Equal(baseline.AddMinutes(63), timeSeriesValues[2].Timestamp);

                    Assert.Equal(179, timeSeriesValues[3].Values[0]);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(62), timeSeriesValues[3].Timestamp);

                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesRaw_WhereInOnLoadedTag()
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
                        Manufacturer = "Fitbit"
                    }, "watches/fitbit");

                    session.Store(new Watch
                    {
                        Manufacturer = "Apple"
                    }, "watches/apple");

                    session.Store(new Watch
                    {
                        Manufacturer = "Sony"
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
                            .Where((ts, src) => src.Manufacturer.In(new[] { "Fitbit", "Apple" }))
                            .ToList());

                    var queryResult = query.First();

                    Assert.Equal(5, queryResult.Count);

                    var timeSeriesValues = queryResult.Results;

                    Assert.Equal(59, timeSeriesValues[0].Values[0]);
                    Assert.Equal(baseline.AddMinutes(61), timeSeriesValues[0].Timestamp);

                    Assert.Equal(79, timeSeriesValues[1].Values[0]);
                    Assert.Equal(baseline.AddMinutes(62), timeSeriesValues[1].Timestamp);

                    Assert.Equal(69, timeSeriesValues[2].Values[0]);
                    Assert.Equal(baseline.AddMinutes(63), timeSeriesValues[2].Timestamp);

                    Assert.Equal(159, timeSeriesValues[3].Values[0]);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), timeSeriesValues[3].Timestamp);

                    Assert.Equal(169, timeSeriesValues[4].Values[0]);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(63), timeSeriesValues[4].Timestamp);

                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesRaw_WhereOnVariable()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person(), "people/1");

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
                    double val = 70;

                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, "Heartrate", baseline, baseline.AddMonths(2))
                            .Where(ts => ts.Values[0] > val)
                            .ToList());

                    // should add 'val' as query parameter  
                    Assert.Contains("Values[0] > $p0", query.ToString());

                    var queryResult = query.First();
                    Assert.Equal(4, queryResult.Count);

                    var timeSeriesValues = queryResult.Results;

                    Assert.Equal(79, timeSeriesValues[0].Values[0]);
                    Assert.Equal(baseline.AddMinutes(62), timeSeriesValues[0].Timestamp);

                    Assert.Equal(159, timeSeriesValues[1].Values[0]);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), timeSeriesValues[1].Timestamp);

                    Assert.Equal(179, timeSeriesValues[2].Values[0]);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(62), timeSeriesValues[2].Timestamp);

                    Assert.Equal(169, timeSeriesValues[3].Values[0]);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(63), timeSeriesValues[3].Timestamp);

                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesRaw_MultipleSeries()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "ayende"
                    }, "people/1");

                    var tsf = session.TimeSeriesFor("people/1");
                    tsf.Append("Heartrate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                    tsf.Append("Heartrate", baseline.AddMinutes(62), "watches/apple", new[] { 79d });
                    tsf.Append("Heartrate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });

                    tsf.Append("Heartrate", baseline.AddMonths(1).AddMinutes(61), "watches/apple", new[] { 159d });
                    tsf.Append("Heartrate", baseline.AddMonths(1).AddMinutes(62), "watches/fitbit", new[] { 179d });
                    tsf.Append("Heartrate", baseline.AddMonths(1).AddMinutes(63), "watches/fitbit", new[] { 169d });

                    tsf.Append("BloodPressure", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                    tsf.Append("BloodPressure", baseline.AddMinutes(62), "watches/apple", new[] { 79d });
                    tsf.Append("BloodPressure", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });

                    tsf.Append("BloodPressure", baseline.AddMonths(1).AddMinutes(61), "watches/apple", new[] { 159d });
                    tsf.Append("BloodPressure", baseline.AddMonths(1).AddMinutes(62), "watches/fitbit", new[] { 179d });
                    tsf.Append("BloodPressure", baseline.AddMonths(1).AddMinutes(63), "watches/fitbit", new[] { 169d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Select(p => new QueryResult2
                        {
                            Id = p.Id,
                            Name = p.Name,
                            HeartRate = RavenQuery.TimeSeries(p, "Heartrate")
                                .Where(ts => ts.Tag == "watches/fitbit")
                                .ToList(),
                            BloodPressure = RavenQuery.TimeSeries(p, "BloodPressure")
                                .Where(ts => ts.Tag == "watches/apple")
                                .ToList(),

                        });

                    var queryResult = query.First();

                    Assert.Equal("ayende", queryResult.Name);
                    Assert.Equal("people/1", queryResult.Id);

                    Assert.Equal(4, queryResult.HeartRate.Count);

                    var heartrateValues = queryResult.HeartRate.Results;

                    Assert.Equal(59, heartrateValues[0].Values[0]);
                    Assert.Equal(69, heartrateValues[1].Values[0]);
                    Assert.Equal(179, heartrateValues[2].Values[0]);
                    Assert.Equal(169, heartrateValues[3].Values[0]);

                    Assert.Equal(2, queryResult.BloodPressure.Count);

                    var bloodPressureValues = queryResult.BloodPressure.Results;

                    Assert.Equal(79, bloodPressureValues[0].Values[0]);
                    Assert.Equal(159, bloodPressureValues[1].Values[0]);

                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesRaw_WhereOnValue()
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
                            .Where(ts => ts.Value > 75 && ts.Value < 175)
                            .ToList());

                    var result = query.First();
                    Assert.Equal(3, result.Count);

                    var timeSeriesValues = result.Results;

                    Assert.Equal(79, timeSeriesValues[0].Values[0]);
                    Assert.Equal(baseline.AddMinutes(62), timeSeriesValues[0].Timestamp);

                    Assert.Equal(159, timeSeriesValues[1].Values[0]);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(61), timeSeriesValues[1].Timestamp);

                    Assert.Equal(169, timeSeriesValues[2].Values[0]);
                    Assert.Equal(baseline.AddMonths(1).AddMinutes(63), timeSeriesValues[2].Timestamp);
                }
            }
        }

    }
}
