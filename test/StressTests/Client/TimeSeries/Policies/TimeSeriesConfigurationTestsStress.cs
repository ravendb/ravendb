using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Client.TimeSeries.Replication;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Client.TimeSeries.Policies
{
    public class TimeSeriesConfigurationTestsStress : ReplicationTestBase
    {
        public TimeSeriesConfigurationTestsStress(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task RapidRetentionAndRollupInACluster()
        {
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 3
            }))
            {
                var raw = new RawTimeSeriesPolicy(TimeValue.FromSeconds(15));

                var p1 = new TimeSeriesPolicy("By1", TimeValue.FromSeconds(1), raw.RetentionTime * 2);
                var p2 = new TimeSeriesPolicy("By2", TimeValue.FromSeconds(2), raw.RetentionTime * 3);
                var p3 = new TimeSeriesPolicy("By4", TimeValue.FromSeconds(4), raw.RetentionTime * 4);
                var p4 = new TimeSeriesPolicy("By8", TimeValue.FromSeconds(8), raw.RetentionTime * 5);

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = raw,
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p1,p2,p3,p4
                            }
                        },
                    },
                    PolicyCheckFrequency = TimeSpan.FromSeconds(1)
                };

                var now = DateTime.UtcNow;
                var baseline = now.AddSeconds(-15 * 3);
                var total = ((TimeSpan)TimeValue.FromSeconds(15 * 3)).TotalMilliseconds;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "users/karmel");

                    for (int i = 0; i <= total; i++)
                    {
                        session.TimeSeriesFor("users/karmel", "Heartrate")
                            .Append(baseline.AddMilliseconds(i), new[] { 29d * i, i }, "watches/fitbit");
                    }
                    session.SaveChanges();

                    session.Store(new User { Name = "Karmel" }, "marker");
                    session.SaveChanges();

                    Assert.True(await WaitForDocumentInClusterAsync<User>(cluster.Nodes, store.Database, "marker", null, TimeSpan.FromSeconds(15)));
                }

                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));
                await Task.Delay((TimeSpan)(p4.RetentionTime * 2));
                // nothing should be left

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "marker/2");
                    session.SaveChanges();

                    Assert.True(await WaitForDocumentInClusterAsync<User>(cluster.Nodes, store.Database, "marker/2", null, TimeSpan.FromSeconds(15)));
                }

                foreach (var node in cluster.Nodes)
                {
                    using (var nodeStore = GetDocumentStore(new Options
                    {
                        Server = node,
                        CreateDatabase = false,
                        DeleteDatabaseOnDispose = false,
                        ModifyDocumentStore = s => s.Conventions = new DocumentConventions
                        {
                            DisableTopologyUpdates = true
                        },
                        ModifyDatabaseName = _ => store.Database
                    }))
                    {
                        using (var session = nodeStore.OpenSession())
                        {
                            var user = session.Load<User>("users/karmel");
                            Assert.Equal(0, session.Advanced.GetTimeSeriesFor(user)?.Count ?? 0);
                            var db = await node.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                            await TimeSeriesReplicationTests.AssertNoLeftOvers(db);
                        }
                    }
                }
            }
        }

        [Fact]
        public async Task RapidRetentionAndRollup()
        {
            using (var store = GetDocumentStore())
            {
                var raw = new RawTimeSeriesPolicy(TimeValue.FromSeconds(15));

                var p1 = new TimeSeriesPolicy("By1", TimeValue.FromSeconds(1), raw.RetentionTime * 2);
                var p2 = new TimeSeriesPolicy("By2", TimeValue.FromSeconds(2), raw.RetentionTime * 3);
                var p3 = new TimeSeriesPolicy("By4", TimeValue.FromSeconds(4), raw.RetentionTime * 4);
                var p4 = new TimeSeriesPolicy("By8", TimeValue.FromSeconds(8), raw.RetentionTime * 5);

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = raw,
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p1,p2,p3,p4
                            }
                        },
                    },
                    PolicyCheckFrequency = TimeSpan.FromSeconds(1)
                };

                var now = DateTime.UtcNow;
                var baseline = now.AddSeconds(-15 * 3);
                var total = ((TimeSpan)TimeValue.FromSeconds(15 * 3)).TotalMilliseconds;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "users/karmel");

                    for (int i = 0; i <= total; i++)
                    {
                        session.TimeSeriesFor("users/karmel", "Heartrate")
                            .Append(baseline.AddMilliseconds(i), new[] { 29d * i, i }, "watches/fitbit");
                    }
                    session.SaveChanges();
                }

                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));
                WaitForUserToContinueTheTest(store);

                await Task.Delay((TimeSpan)(p4.RetentionTime + TimeValue.FromSeconds(10)));
                // nothing should be left

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/karmel");
                    Assert.Equal(0, session.Advanced.GetTimeSeriesFor(user)?.Count ?? 0);
                }
            }
        }

        [Fact]
        public async Task RavenDB_15840()
        {
            var dbA = GetDatabaseName();
            var dbB = GetDatabaseName();

            using (var storeA = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => dbA
            }))
            using (var storeB = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => dbB
            }))
            {
                var raw = new RawTimeSeriesPolicy(TimeValue.FromSeconds(15));

                var p1 = new TimeSeriesPolicy("By1", TimeValue.FromSeconds(1), raw.RetentionTime * 2);
                var p2 = new TimeSeriesPolicy("By2", TimeValue.FromSeconds(2), raw.RetentionTime * 3);
                var p3 = new TimeSeriesPolicy("By4", TimeValue.FromSeconds(4), raw.RetentionTime * 4);
                var p4 = new TimeSeriesPolicy("By8", TimeValue.FromSeconds(8), raw.RetentionTime * 5);

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = raw,
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p1,p2,p3,p4
                            }
                        }
                    }
                };

                // create same TS on both databases
                var now = DateTime.UtcNow;
                CreateTimeSeriesForDatabase(storeA, now);
                CreateTimeSeriesForDatabase(storeB, now);

                await storeA.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));
                await storeB.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                var databaseA = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(dbA);
                var databaseB = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(dbB);

                var cts = new CancellationTokenSource();
                cts.CancelAfter((TimeSpan)(p4.RetentionTime * 2));

                // run rollup and retention until one of the policies (except raw policy) is deleted (from retention)
                while (cts.IsCancellationRequested == false)
                {
                    await databaseB.TimeSeriesPolicyRunner.RunRollups();

                    using (var sessionB = storeB.OpenSession())
                    {
                        var user = sessionB.Load<User>("users/karmel");
                        var ts = sessionB.Advanced.GetTimeSeriesFor(user);

                        if (ts.Contains("Heartrate@By1") == false ||
                            ts.Contains("Heartrate@By2") == false ||
                            ts.Contains("Heartrate@By4") == false ||
                            ts.Contains("Heartrate@By8") == false)
                            break;
                    }

                    await databaseA.TimeSeriesPolicyRunner.DoRetention();
                }

                // run some rollups and retentions from the other database.
                // we are doing so because we want to reach the path of 'AppendEntireSegment'
                // after we set up the replication between the databases (for that the conflict status must be 'Update')
                for (int i = 0; i < 6; i++)
                {
                    await databaseA.TimeSeriesPolicyRunner.RunRollups();
                    using (var sessionA = storeA.OpenSession())
                    {
                        var user = sessionA.Load<User>("users/karmel");
                        var ts = sessionA.Advanced.GetTimeSeriesFor(user);

                        if (ts.Contains("Heartrate") == false)
                            break;
                    }

                    await databaseA.TimeSeriesPolicyRunner.DoRetention();
                }

                await SetupReplicationAsync(storeB, storeA);
                await EnsureReplicatingAsync(storeB, storeA);

                await SetupReplicationAsync(storeA, storeB);
                await EnsureReplicatingAsync(storeA, storeB);

                cts = new CancellationTokenSource();
                cts.CancelAfter((TimeSpan)(p4.RetentionTime * 2));

                int count = 0;
                long rolled = 0;
                while (cts.IsCancellationRequested == false)
                {
                    rolled = await databaseB.TimeSeriesPolicyRunner.RunRollups();
                    await databaseB.TimeSeriesPolicyRunner.DoRetention();

                    using (var sessionB = storeB.OpenSession())
                    {
                        var user = sessionB.Load<User>("users/karmel");
                        count = sessionB.Advanced.GetTimeSeriesFor(user)?.Count ?? 0;
                        if (count == 0 && rolled == 0)
                            break;
                    }
                }

                Assert.Equal(0, count);
                Assert.Equal(0, rolled);

                using (var sessionB = storeB.OpenSession())
                {
                    sessionB.Store(new User { Name = "Karmel" }, "marker/2");
                    sessionB.SaveChanges();
                }

                Assert.True(WaitForDocument(storeA, "marker/2", timeout: 30_000, dbA));

                using (var sessionA = storeA.OpenSession())
                {
                    var user = sessionA.Load<User>("users/karmel");
                    Assert.Equal(0, sessionA.Advanced.GetTimeSeriesFor(user)?.Count ?? 0);
                }
            }
        }

        private void CreateTimeSeriesForDatabase(DocumentStore store, DateTime now)
        {
            var baseline = now.AddSeconds(-15 * 3);
            var total = ((TimeSpan)TimeValue.FromSeconds(15 * 3)).TotalMilliseconds;

            using (var session = store.OpenSession())
            {
                session.Store(new User { Name = "Karmel" }, "users/karmel");

                for (int i = 0; i <= total; i++)
                {
                    session.TimeSeriesFor("users/karmel", "Heartrate")
                        .Append(baseline.AddMilliseconds(i), new[] { 29d * i, i }, "watches/fitbit");
                }
                session.SaveChanges();
            }
        }
    }
}
