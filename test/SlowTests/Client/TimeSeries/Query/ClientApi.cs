using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
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

        public class TimeSeriesRangeAggregation
        {
#pragma warning disable 649
            public long[] Count;
            public double?[] Max, Min, Last, First, Avg;
            public DateTime To, From;
#pragma warning restore 649
        }

        public class TimeSeriesAggregation
        {
            public long Count { get; set; }
            public TimeSeriesRangeAggregation[] Results { get; set; }
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
        public void CanQueryTimeSeriesAggregation_DeclareSyntax_AllDocsQuery()
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
                        .Append("Heartrate", baseline.AddMonths(1).AddMinutes(61), "watches/fitbit", new[] { 59d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMonths(1).AddMinutes(62), "watches/fitbit", new[] { 79d });

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMonths(1).AddMinutes(63), "watches/fitbit", new[] { 69d });


                    session.SaveChanges();
                }


                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Where(u => u.Age < 21)
                        .Select(u => RavenQuery.TimeSeries(u, "Heartrate")
                            .Where(ts => ts.Tag == "watches/fitbit")
                            .GroupBy(ts => "1 month")
                            .Select(g => new
                            {
                                Avg = RavenQuery.TimeSeriesAggregations.Average(),
                                Max = RavenQuery.TimeSeriesAggregations.Max()
                            }));


                    var query2 = session.Query<User>()
                        .Where(u => u.Age < 21)
                        .Select(u => RavenQuery.TimeSeries(
@"from u.Heartrate 
where Tag != 'watches/fitbit'
group by '1 month'
select avg(), max()
"));

                    var agg = query.First();


                }
            }
        }

    }
}
