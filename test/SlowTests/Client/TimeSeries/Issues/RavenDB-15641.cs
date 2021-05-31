using System;
using System.Linq;
using FastTests;
using FastTests.Server.Replication;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_15641 : ReplicationTestBase
    {
        public RavenDB_15641(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanSelectAverageWithInterpolationInTimeSeriesQuery()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                var id = "people/1";

                using (var session = store.OpenSession())
                {
                    session.Store(new Person { Name = "Oren", }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    tsf.Append(baseline, 0);
                    tsf.Append(baseline.AddMinutes(1), 1);
                    tsf.Append(baseline.AddMinutes(2), 2);
                    tsf.Append(baseline.AddMinutes(3), 3);
                    tsf.Append(baseline.AddMinutes(4), 4);
                    tsf.Append(baseline.AddMinutes(5), 5);
                    tsf.Append(baseline.AddMinutes(6), 6);
                    tsf.Append(baseline.AddMinutes(7), 7);
                    tsf.Append(baseline.AddMinutes(8), 8);
                    tsf.Append(baseline.AddMinutes(9), 9);

                    tsf.Append(baseline.AddMinutes(20), 20);
                    tsf.Append(baseline.AddMinutes(21), 21);
                    tsf.Append(baseline.AddMinutes(22), 22);
                    tsf.Append(baseline.AddMinutes(23), 23);
                    tsf.Append(baseline.AddMinutes(24), 24);
                    tsf.Append(baseline.AddMinutes(25), 25);
                    tsf.Append(baseline.AddMinutes(26), 26);
                    tsf.Append(baseline.AddMinutes(27), 27);
                    tsf.Append(baseline.AddMinutes(28), 28);
                    tsf.Append(baseline.AddMinutes(29), 29);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Person>()
                        .Select(p => RavenQuery.TimeSeries(p, "HeartRate")
                            .GroupBy(g => g
                                .Minutes(10)
                                .WithOptions(new TimeSeriesAggregationOptions
                                {
                                    Interpolation = InterpolationType.Linear
                                }))
                            .Select(x => new
                            {
                                Sum = x.Sum(), 
                                Avg = x.Average()
                            })
                            .ToList());

                    var result = query.First();
                    Assert.Equal(3, result.Results.Length);

                    var range = result.Results[0]; // 00:00-00:10
                    Assert.Equal(10, range.Count[0]);
                    Assert.Equal(45, range.Sum[0]);
                    Assert.Equal(4.5, range.Average[0]);

                    range = result.Results[2]; // 00:20-00:30
                    Assert.Equal(10, range.Count[0]);
                    Assert.Equal(245, range.Sum[0]);
                    Assert.Equal(24.5, range.Average[0]);

                    //gap : 00:10-00:20
                    range = result.Results[1];
                    Assert.Equal(10, range.Count[0]);
                    Assert.Equal(145, range.Sum[0]);

                    var expectedAvg = (24.5 + 4.5) / 2;
                    Assert.Equal(expectedAvg, range.Average[0]);
                }
            }
        }
    }
}
