using System;
using Raven.Client.Documents.Queries.TimeSeries;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_15341 : ReplicationTestBase
    {
        public RavenDB_15341(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.TimeSeries)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void ShouldNotThrowWhenUsingLastOnNonExistingTimeSeries(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.TimeSeriesFor("users/1", "HeartRate").Append(DateTime.Now, 1);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/2");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<TimeSeriesRawResult>(@"from Users select timeseries(from HeartRate last 1 min)")
                        .ToList();

                    Assert.Equal(2, result.Count);
                    Assert.Equal(1, result[0].Count);
                    Assert.Equal(0, result[1].Count);
                }
            }

        }

        [Fact]
        public void ShouldNotThrowWhenUsingLastOnNonExistingTimeSeries2()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<TimeSeriesRawResult>(@"from Users select timeseries(from Stocks last 1 min)")
                        .ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(0, result[0].Count);
                }

            }

        }
    }
}
