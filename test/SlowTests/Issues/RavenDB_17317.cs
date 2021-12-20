using System;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17317 : ReplicationTestBase
    {
        public RavenDB_17317(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanReplicateEntireSegmentOnUpdate_IncrementalTimeSeries()
        {
            var baseline = DateTime.UtcNow;

            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "user/1" }, "users/ayende");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new User { Name = "user/1" }, "users/ayende");
                    session.SaveChanges();
                }


                for (int i = 0; i < 10; i++)
                {
                    using (var session = storeA.OpenSession())
                    {
                        var ts = session.IncrementalTimeSeriesFor("users/ayende", "INC:Downloads");

                        for (int j = 0; j < 10; j++)
                            ts.Increment(baseline.AddMinutes(j), 1);

                        session.SaveChanges();
                    }
                }

                for (int i = 0; i < 10; i++)
                {
                    using (var session = storeB.OpenSession())
                    {
                        var ts = session.IncrementalTimeSeriesFor("users/ayende", "INC:Downloads");

                        for (int j = 0; j < 10; j++)
                            ts.Increment(baseline.AddMinutes(j), 1);

                        session.SaveChanges();
                    }
                }

                await SetupReplicationAsync(storeA, storeB);
                await SetupReplicationAsync(storeB, storeA);

                await EnsureReplicatingAsync(storeA, storeB);
                await EnsureReplicatingAsync(storeB, storeA);

                await EnsureNoReplicationLoop(Server, storeA.Database);
                await EnsureNoReplicationLoop(Server, storeB.Database);

                using (var session = storeA.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", "INC:Downloads");
                    ts.Increment(baseline, 1);
                    session.SaveChanges();
                }

                await EnsureReplicatingAsync(storeA, storeB);
                await EnsureReplicatingAsync(storeB, storeA);

                await EnsureNoReplicationLoop(Server, storeA.Database);
                await EnsureNoReplicationLoop(Server, storeB.Database);

                using (var sessionA = storeA.OpenSession())
                using (var sessionB = storeB.OpenSession())
                {
                    var tsA = sessionA.IncrementalTimeSeriesFor("users/ayende", "INC:Downloads").Get();
                    var tsB = sessionB.IncrementalTimeSeriesFor("users/ayende", "INC:Downloads").Get();

                    Assert.Equal(tsA.Length, tsB.Length);
                    Assert.Equal(21, tsA[0].Value);
                    Assert.Equal(21, tsB[0].Value);

                    for (int i = 1; i < tsA.Length; i++)
                    {
                        Assert.Equal(20, tsA[i].Value);
                        Assert.Equal(20, tsB[i].Value);
                    }
                }
            }
        }

        [Fact]
        public async Task CanReplicateEntireSegmentOnUpdate_TimeSeries()
        {
            var baseline = DateTime.UtcNow;

            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "user/1" }, "users/ayende");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new User { Name = "user/1" }, "users/ayende");
                    session.SaveChanges();
                }


                using (var session = storeA.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRates");

                    for (int j = 0; j < 10; j++)
                        ts.Append(baseline.AddMinutes(j), 100);

                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRates");

                    for (int j = 0; j < 10; j++)
                        ts.Append(baseline.AddMinutes(j), 100);

                    session.SaveChanges();
                }

                await SetupReplicationAsync(storeA, storeB);
                await SetupReplicationAsync(storeB, storeA);

                await EnsureReplicatingAsync(storeA, storeB);
                await EnsureReplicatingAsync(storeB, storeA);

                await EnsureNoReplicationLoop(Server, storeA.Database);
                await EnsureNoReplicationLoop(Server, storeB.Database);

                using (var session = storeA.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRates");
                    ts.Append(baseline, 50);
                    session.SaveChanges();
                }

                await EnsureReplicatingAsync(storeA, storeB);
                await EnsureReplicatingAsync(storeB, storeA);

                await EnsureNoReplicationLoop(Server, storeA.Database);
                await EnsureNoReplicationLoop(Server, storeB.Database);

                using (var sessionA = storeA.OpenSession())
                using (var sessionB = storeB.OpenSession())
                {
                    var tsA = sessionA.TimeSeriesFor("users/ayende", "HeartRates").Get();
                    var tsB = sessionB.TimeSeriesFor("users/ayende", "HeartRates").Get();

                    Assert.Equal(tsA.Length, tsB.Length);
                    Assert.Equal(50, tsA[0].Value);
                    Assert.Equal(50, tsB[0].Value);

                    for (int i = 1; i < tsA.Length; i++)
                    {
                        Assert.Equal(100, tsA[i].Value);
                        Assert.Equal(100, tsB[i].Value);
                    }
                }
            }
        }

        [Fact]
        public async Task CanDeleteEntireSegment()
        {
            var baseline = DateTime.UtcNow;

            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "user/1" }, "users/ayende");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new User { Name = "user/1" }, "users/ayende");
                    session.SaveChanges();
                }

                using (var session = storeA.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRates");

                    for (int j = 0; j < 10; j++)
                        ts.Append(baseline.AddMinutes(j), 100);

                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRates");

                    for (int j = 0; j < 10; j++)
                        ts.Append(baseline.AddMinutes(j), 100);

                    session.SaveChanges();
                }

                await SetupReplicationAsync(storeA, storeB);
                await SetupReplicationAsync(storeB, storeA);

                await EnsureReplicatingAsync(storeA, storeB);
                await EnsureReplicatingAsync(storeB, storeA);

                await EnsureNoReplicationLoop(Server, storeA.Database);
                await EnsureNoReplicationLoop(Server, storeB.Database);

                using (var session = storeA.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRates");
                    ts.Delete();
                    session.SaveChanges();
                }

                await EnsureReplicatingAsync(storeA, storeB);
                await EnsureReplicatingAsync(storeB, storeA);

                await EnsureNoReplicationLoop(Server, storeA.Database);
                await EnsureNoReplicationLoop(Server, storeB.Database);

                using (var sessionA = storeA.OpenSession())
                using (var sessionB = storeB.OpenSession())
                {
                    var tsA = sessionA.TimeSeriesFor("users/ayende", "HeartRates").Get();
                    var tsB = sessionB.TimeSeriesFor("users/ayende", "HeartRates").Get();

                    Assert.Null(tsA);
                    Assert.Null(tsB);
                }
            }
        }

        private class User
        {
            public string Name;
        }
    }
}
