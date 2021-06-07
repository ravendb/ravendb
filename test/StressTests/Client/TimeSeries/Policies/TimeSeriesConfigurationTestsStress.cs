using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Client.TimeSeries.Replication;
using Sparrow;
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

                    Assert.True(await WaitForDocumentInClusterAsync<User>((DocumentSession)session, "marker", null, TimeSpan.FromSeconds(15)));
                }

                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));
                await Task.Delay((TimeSpan)(p4.RetentionTime * 2));
                // nothing should be left

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
    }
}
