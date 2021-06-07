using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15430 : ReplicationTestBase
    {
        public RavenDB_15430(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task MarkPolicyAfterRollup()
        {
            DefaultClusterSettings[RavenConfiguration.GetKey(x => x.Tombstones.CleanupInterval)] = 1.ToString();
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using (var store = GetDocumentStore(new Options { Server = cluster.Leader, ReplicationFactor = 3, RunInMemory = false }))
            {
                var raw = new RawTimeSeriesPolicy();
                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = raw,
                            Policies = new List<TimeSeriesPolicy> { new TimeSeriesPolicy("ByMinute", TimeSpan.FromMinutes(10)) }
                        }
                    },
                    PolicyCheckFrequency = TimeSpan.FromSeconds(1)
                };

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var firstNode = record.Topology.Members[0];
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                var now = new DateTime(2021, 6, 1, 10, 7, 29, DateTimeKind.Utc);
                foreach (var server in Servers)
                {
                    var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    database.Time.UtcDateTime = () => now;
                }

                var baseline = now.Add(-TimeSpan.FromMinutes(15));

                using (var session = store.OpenSession())
                {
                    var id = "users/karmel/0";
                    session.Store(new User { Name = "Karmel" }, id);
                    for (int i = 0; i < 15; i++)
                    {
                        session.TimeSeriesFor(id, "Heartrate")
                            .Append(baseline.AddMinutes(i), i);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "marker");
                    session.SaveChanges();
                    Assert.True(await WaitForDocumentInClusterAsync<User>((DocumentSession)session, "marker", null, TimeSpan.FromSeconds(15)));
                }

                var res = new Dictionary<string, int>();
                foreach (var server in Servers)
                {
                    var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    var tss = database.DocumentsStorage.TimeSeriesStorage;
                    await database.TimeSeriesPolicyRunner.RunRollups();

                    using (var session = store.OpenSession())
                    {
                        var name = config.Collections["Users"].Policies[0].GetTimeSeriesName("Heartrate");
                        WaitForValue(() =>
                        {
                            var val = session.TimeSeriesFor("users/karmel/0", name)
                                .Get(DateTime.MinValue, DateTime.MaxValue);
                            return val != null;
                        }, true);

                        var val = session.TimeSeriesFor("users/karmel/0", name)
                            .Get(DateTime.MinValue, DateTime.MaxValue).Length;
                        res.Add(server.ServerStore.NodeTag, val);
                        Assert.True(val > 0);
                    }
                }

                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var firstNode2 = record.Topology.Members[0];
                Assert.Equal(firstNode2, firstNode);

                record = store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database)).Result;
                var list = record.Topology.Members;
                list.Reverse();
                await store.Maintenance.Server.SendAsync(new ReorderDatabaseMembersOperation(store.Database, list));
                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                firstNode2 = record.Topology.Members[0];
                Assert.NotEqual(firstNode2, firstNode);

                await Task.Delay(1000);

                foreach (var server in Servers)
                {
                    var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    database.Time.UtcDateTime = () => now.AddMinutes(10);
                    await database.TimeSeriesPolicyRunner.RunRollups();
                }

                foreach (var server in Servers)
                {
                    WaitForValue(() =>
                    {
                        using (var session = store.OpenSession())
                        {
                            var name = config.Collections["Users"].Policies[0].GetTimeSeriesName("Heartrate");
                            var val = session.TimeSeriesFor("users/karmel/0", name)
                                .Get(DateTime.MinValue, DateTime.MaxValue);
                            return val.Length > res[server.ServerStore.NodeTag];
                        }
                    }, true);
                }

                using (var session = store.OpenSession())
                {
                    var name = config.Collections["Users"].Policies[0].GetTimeSeriesName("Heartrate");
                    var val = session.TimeSeriesFor("users/karmel/0", name)
                        .Get(DateTime.MinValue, DateTime.MaxValue);

                    Assert.True(val.Length > res[Servers[0].ServerStore.NodeTag]);
                }

                record = store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database)).Result;
                firstNode2 = record.Topology.Members[0];
                Assert.NotEqual(firstNode2, firstNode);
            }
        }

        [Fact]
        public async Task RequiredForNextPolicyTest()
        {
            DefaultClusterSettings[RavenConfiguration.GetKey(x => x.Tombstones.CleanupInterval)] = 1.ToString();
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using (var store = GetDocumentStore(new Options { Server = cluster.Leader, ReplicationFactor = 3, RunInMemory = false }))
            {
                DateTime start = default;
                var retention = TimeSpan.FromSeconds(180);
                var raw = new RawTimeSeriesPolicy(retention);
                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = raw,
                            Policies = new List<TimeSeriesPolicy> { new TimeSeriesPolicy("ByMinute", TimeSpan.FromSeconds(60)) }
                        }
                    },
                    PolicyCheckFrequency = TimeSpan.FromSeconds(1)
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                var now = new DateTime(2021, 6, 1, 18, 45, 0, 999, DateTimeKind.Utc);
                foreach (var server in Servers)
                {
                    var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    database.Time.UtcDateTime = () => now;
                }

                var baseline = now.AddSeconds(-120);
                using (var session = store.OpenSession())
                {
                    var id = "users/karmel/0";
                    session.Store(new User { Name = "Karmel" }, id);
                    for (int i = 0; i < 120; i++)
                    {
                        session.TimeSeriesFor(id, "Heartrate")
                            .Append(baseline.AddSeconds(i), i);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "marker");
                    session.SaveChanges();
                    Assert.True(await WaitForDocumentInClusterAsync<User>((DocumentSession)session, "marker", null, TimeSpan.FromSeconds(15)));
                }

                foreach (var server in Servers)
                {
                    var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    database.Time.UtcDateTime = () => now.AddMinutes(3);
                }

                var sp = Stopwatch.StartNew();
                var check = true;
                while (check)
                {
                    Assert.True(sp.Elapsed < retention.Add(TimeSpan.FromMinutes(-2)),  $"too long has passed {sp.Elapsed}, retention is {retention}");
                    await Task.Delay(200);
                    check = false;
                    foreach (var server in Servers)
                    {
                        var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                        var tss = database.DocumentsStorage.TimeSeriesStorage;

                        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                        using (ctx.OpenReadTransaction())
                        {
                            var id = $"users/karmel/0";
                            var stats = tss.Stats.GetStats(ctx, id, "Heartrate");

                            TimeSeriesReader reader;
                            if (stats == default || stats.Count == 0)
                            {
                                var name = config.Collections["Users"].Policies[0].GetTimeSeriesName("Heartrate");
                                stats = tss.Stats.GetStats(ctx, id, name);
                                reader = tss.GetReader(ctx, id, name, DateTime.MinValue, DateTime.MaxValue);

                                Assert.True(stats.Count > 0);
                                Assert.Equal(stats.Start, reader.First().Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                                Assert.Equal(stats.End, reader.Last().Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                                continue;
                            }
                            check = true;
                            reader = tss.GetReader(ctx, id, "Heartrate", DateTime.MinValue, DateTime.MaxValue);
                            Assert.Equal(stats.Start, reader.First().Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                            Assert.Equal(stats.End, reader.Last().Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        }
                    }
                }
            }
        }

        [Fact]
        public async Task SingleResultTest()
        {
            DefaultClusterSettings[RavenConfiguration.GetKey(x => x.Tombstones.CleanupInterval)] = 1.ToString();
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using (var store = GetDocumentStore(new Options { Server = cluster.Leader, ReplicationFactor = 3, RunInMemory = false }))
            {
                var raw = new RawTimeSeriesPolicy();
                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = raw,
                            Policies = new List<TimeSeriesPolicy> { new TimeSeriesPolicy("ByMinute", TimeSpan.FromSeconds(60)) }
                        }
                    },
                    PolicyCheckFrequency = TimeSpan.FromSeconds(1)
                };

                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                var now = DateTime.UtcNow;

                var baseline = now.AddSeconds(-120);
                using (var session = store.OpenSession())
                {
                    var id = "users/karmel/0";
                    session.Store(new User { Name = "Karmel" }, id);
                    for (int i = 0; i < 120; i++)
                    {
                        session.TimeSeriesFor(id, "Heartrate")
                            .Append(baseline.AddSeconds(i), i);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "marker");
                    session.SaveChanges();
                    Assert.True(await WaitForDocumentInClusterAsync<User>((DocumentSession)session, "marker", null, TimeSpan.FromSeconds(15)));
                }

                var database = await Servers[0].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                {
                    using (var tx = ctx.OpenReadTransaction())
                    {
                        var name = config.Collections["Users"].Policies[0].GetTimeSeriesName("Heartrate");
                        var reader = database.DocumentsStorage.TimeSeriesStorage.GetReader(ctx, "users/karmel/0", "Heartrate", baseline.AddSeconds(120), DateTime.MaxValue);
                        Assert.True(reader.Last() == null);
                    }
                }

            }
        }
    }
}
