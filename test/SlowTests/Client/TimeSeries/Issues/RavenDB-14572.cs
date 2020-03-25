using System;
using System.Linq;
using FastTests.Server.Replication;
using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_14572 : ReplicationTestBase
    {
        public RavenDB_14572(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void GroupByOneMinuteShouldHaveNoGaps()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "zzz/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var d = new DateTime(1980, 1, 1);

                    var r = new Random();
                    var previous = 0.0;

                    for (var i = 0; i < 1000; i++)
                    {
                        var nextDouble = previous * 0.9 + 0.1 * r.NextDouble();

                        session.TimeSeriesFor("zzz/1", "small")
                            .Append(d, nextDouble);
                        d = d.AddMinutes(1);

                        previous = nextDouble;
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Query<User>()
                        .Select(u => new
                        {
                            HeartRate = RavenQuery.TimeSeries("small")
                                .GroupBy(g => g.Minutes(1))
                                .Select(x => new
                                {
                                    Max = x.Max(), 
                                    Min = x.Min(), 
                                    Average = x.Average()
                                })
                                .ToList()
                        })
                        .First();

                    Assert.Equal(1000, result.HeartRate.Count);

                    DateTime lastTo = default;

                    foreach (var rangeAggregation in result.HeartRate.Results)
                    {
                        Assert.Equal(1, rangeAggregation.Count[0]);

                        if (lastTo != default)
                        {
                            var currentFrom = rangeAggregation.From;
                            Assert.Equal(lastTo, currentFrom);
                        }

                        lastTo = rangeAggregation.To;
                    }

                }
            }
        }

    }
}
