using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Client.TimeSeries
{
    public class TimeSeriesStress : ReplicationTestBase
    {

        public TimeSeriesStress(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task RapidRetention()
        {
            var cluster = await CreateRaftCluster(3);
            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 3
            }))
            {
                var retention = TimeValue.FromSeconds(120);
                var raw = new RawTimeSeriesPolicy(retention);
                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = raw,
                        }
                    },
                    PolicyCheckFrequency = TimeSpan.FromSeconds(1)
                };

                var now = DateTime.UtcNow;
                var baseline = now.Add(-retention * 3);
                var total = (int)((TimeSpan)retention).TotalMilliseconds * 3;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "users/karmel");

                    for (int i = 0; i < total; i++)
                    {
                        session.TimeSeriesFor("users/karmel", "Heartrate")
                            .Append(baseline.AddMilliseconds(i), new[] { 29d * i, i, i * 0.01, i * 0.1 }, "watches/fitbit");
                    }
                    session.SaveChanges();

                    session.Store(new User(), "marker");
                    session.SaveChanges();

                    await WaitForDocumentInClusterAsync<User>((DocumentSession)session, "marker", null, TimeSpan.FromSeconds(15));
                }

                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                var sp = Stopwatch.StartNew();
                await Task.Delay((TimeSpan)retention / 2);
                
                WaitForUserToContinueTheTest(store);

                var check = true;
                while (check)
                {
                    Assert.True(sp.Elapsed < ((TimeSpan)retention).Add((TimeSpan)retention / 10),$"too long has passed {sp.Elapsed}, retention is {retention}");
                    await Task.Delay(100);
                    check = false;
                    foreach (var server in Servers)
                    {
                        var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                        using (ctx.OpenReadTransaction())
                        {
                            var tss = database.DocumentsStorage.TimeSeriesStorage;
                            var stats = tss.Stats.GetStats(ctx, "users/karmel", "Heartrate");
                            var reader = tss.GetReader(ctx, "users/karmel", "Heartrate", DateTime.MinValue, DateTime.MaxValue);

                            if (stats.Count == 0)
                                continue;

                            check = true;

                            Assert.Equal(stats.Start, reader.First().Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                            Assert.Equal(stats.End, reader.Last().Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        }
                    }
                }

                Assert.True(sp.Elapsed < (TimeSpan)retention + (TimeSpan)retention / 10);
                await Task.Delay(3000); // let the dust to settle

                await EnsureNoReplicationLoop(Servers[0], store.Database);
                await EnsureNoReplicationLoop(Servers[1], store.Database);
                await EnsureNoReplicationLoop(Servers[2], store.Database);
            }
        }


        [Fact]
        public async Task ManyTimeSeriesRetention()
        {
            DefaultClusterSettings[RavenConfiguration.GetKey(x => x.Tombstones.CleanupInterval)] = 1.ToString();
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 3,
                RunInMemory = false
            }))
            {
                var retention = TimeSpan.FromSeconds(60);
                var raw = new RawTimeSeriesPolicy(retention);
                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = raw,
                            Policies = new List<TimeSeriesPolicy>
                            {
                                new TimeSeriesPolicy("ByMinute", TimeSpan.FromSeconds(60))
                            }
                        }
                    },
                    PolicyCheckFrequency = TimeSpan.FromSeconds(1)
                };
                
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                for (int j = 0; j < 1024; j++)
                {
                    var now = DateTime.UtcNow;
                    var baseline = now.Add(-retention * 2);
                    var total = (int)retention.TotalSeconds * 2;

                    using (var session = store.OpenSession())
                    {
                        var id = "users/karmel/" + j;
                        session.Store(new User {Name = "Karmel"}, id);

                        for (int i = 0; i < total; i++)
                        {
                            session.TimeSeriesFor(id, "Heartrate")
                                .Append(baseline.AddSeconds(i), i);
                        }
                        session.SaveChanges();
                    }
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new User(),"marker");
                    session.SaveChanges();

                    await WaitForDocumentInClusterAsync<User>((DocumentSession)session, "marker", null, TimeSpan.FromSeconds(15));
                }

                var sp = Stopwatch.StartNew();
                await Task.Delay(retention / 2);

                WaitForUserToContinueTheTest(store);

                var check = true;
                while (check)
                {
                    Assert.True(sp.Elapsed < retention.Add(retention / 10),$"too long has passed {sp.Elapsed}, retention is {retention}");
                    await Task.Delay(100);
                    check = false;
                    foreach (var server in Servers)
                    {
                        var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                        var tss = database.DocumentsStorage.TimeSeriesStorage;

                        for (int j = 0; j < 1024; j++)
                        {
                            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                            using (ctx.OpenReadTransaction())
                            {
                                var id = $"users/karmel/{j}";
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

                await EnsureNoReplicationLoop(Servers[0], store.Database);
                await EnsureNoReplicationLoop(Servers[1], store.Database);
                await EnsureNoReplicationLoop(Servers[2], store.Database);
            }
        }
    }
}
