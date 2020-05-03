using System;
using FastTests.Server.Replication;
using Raven.Client.Exceptions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_14800 : ReplicationTestBase
    {
        public RavenDB_14800(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldThrowOnAttemptToCreateSeriesWithNameThatContainsRollupSeparatorChar()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "aviv"}, "users/aviv");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/aviv", "Heartrate@2019").Append(DateTime.UtcNow, value: 65);
                    var ex = Assert.Throws<RavenException>(() => session.SaveChanges());
                    Assert.Contains("Illegal time series name : 'Heartrate@2019'", ex.Message);

                }
            }
        }

    }
}
