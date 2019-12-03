using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Query
{
    public class TimeSeriesQueryClientApi : RavenTestBase
    {
        public TimeSeriesQueryClientApi(ITestOutputHelper output) : base(output)
        {
        }

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

        private class UsersIndex : AbstractIndexCreationTask<User>
        {
            public UsersIndex()
            {
                Map = users => from u in users
                               select new
                                {
                                    u.Age
                                };
            }
        }


        private class Person
        {
            public string Name { get; set; }

            public int Age { get; set; }

            public string WorksAt { get; set; }
        }

        public class RawQueryResult
        {
            public TimeSeriesAggregation HeartRate { get; set; }

            public TimeSeriesAggregation BloodPressure { get; set; }

            public TimeSeriesAggregation Stocks { get; set; }

            public string Name { get; set; }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_UsingClientApi()
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
                                Avg = RavenQuery.TimeSeriesAggregations.Average(),
                                Max = RavenQuery.TimeSeriesAggregations.Max()
                            }));

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
