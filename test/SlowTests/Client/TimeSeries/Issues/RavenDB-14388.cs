using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_14388 : RavenTestBase
    {
        public RavenDB_14388(ITestOutputHelper output) : base(output)
        {
        }

        private class Person
        {
            public string Id { get; set; }

            public string Name { get; set; }
        }

        [Fact]
        public void TimeSeriesSelectShouldAffectQueryEtag()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "ayende"
                    }, "people/1");

                    var tsf = session.TimeSeriesFor("people/1", "Heartrate");
                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Select(p => new
                        {
                            p.Id,
                            p.Name,
                            HeartRate = RavenQuery.TimeSeries(p, "Heartrate")
                                .Where(ts => ts.Tag == "watches/fitbit")
                                .GroupBy(g => g.Months(1))
                                .Select(g => new
                                {
                                    Max = g.Max(),
                                    Count = g.Count()
                                })
                                .ToList()
                        });

                    var result = query.First();

                    Assert.Equal("ayende", result.Name);
                    Assert.Equal("people/1", result.Id);

                    Assert.Equal(4, result.HeartRate.Count);

                    var aggregation = result.HeartRate.Results;

                    Assert.Equal(2, aggregation.Length);

                    Assert.Equal(69, aggregation[0].Max[0]);
                    Assert.Equal(2, aggregation[0].Count[0]);

                    Assert.Equal(179, aggregation[1].Max[0]);
                    Assert.Equal(2, aggregation[1].Count[0]);

                }


                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor("people/1", "Heartrate");

                    tsf.Append(baseline.AddMinutes(64), 89, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(65), 99, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(64), 189, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // re run the query
                    // result should not be served from cache

                    var query = session.Query<Person>()
                        .Select(p => new
                        {
                            p.Id,
                            p.Name,
                            HeartRate = RavenQuery.TimeSeries(p, "Heartrate")
                                .Where(ts => ts.Tag == "watches/fitbit")
                                .GroupBy(g => g.Months(1))
                                .Select(g => new
                                {
                                    Max = g.Max(),
                                    Count = g.Count()
                                })
                                .ToList()
                        });

                    var result = query.First();

                    Assert.Equal("ayende", result.Name);
                    Assert.Equal("people/1", result.Id);

                    Assert.Equal(6, result.HeartRate.Count);

                    var aggregation = result.HeartRate.Results;

                    Assert.Equal(2, aggregation.Length);

                    Assert.Equal(89, aggregation[0].Max[0]);
                    Assert.Equal(3, aggregation[0].Count[0]);

                    Assert.Equal(189, aggregation[1].Max[0]);
                    Assert.Equal(3, aggregation[1].Count[0]);

                }
            }
        }

    }
}
