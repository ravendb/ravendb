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
    public class TimeSeriesDocumentQuery : RavenTestBase
    {
        public TimeSeriesDocumentQuery(ITestOutputHelper output) : base(output)
        {
        }

        private class Person
        {
            public int Age { get; set; }

            public string WorksAt { get; set; }
        }

        [Fact]
        public void CanQueryTimeSeriesUsingDocumentQuery()
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
                        .Append("Heartrate", baseline.AddMonths(1).AddMinutes(62), "watches/apple", new[] { 179d });

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMonths(1).AddMinutes(63), "watches/fitbit", new[] { 169d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    string tsQueryText = @"
from Heartrate between $start and $end
where Tag = 'watches/fitbit'
group by '1 month'
select min(), max(), avg()
";

                    var query = session.Advanced.DocumentQuery<User>()
                        .WhereGreaterThan(u => u.Age, 21)
                        .SelectTimeSeries(builder => builder.Raw(tsQueryText))
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddMonths(3));

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(3, result[0].Count);

                    var agg = result[0].Results;

                    Assert.Equal(2, agg.Length);

                    Assert.Equal(69, agg[0].Max[0]);
                    Assert.Equal(59, agg[0].Min[0]);
                    Assert.Equal(64, agg[0].Avg[0]);

                    Assert.Equal(169, agg[1].Max[0]);
                    Assert.Equal(169, agg[1].Min[0]);
                    Assert.Equal(169, agg[1].Avg[0]);

                }
            }
        }
    }
}
