using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session;
using Raven.Server;
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

                var check = true;
                while (check)
                {
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

                            Assert.Equal(stats.Start, reader.First().Timestamp);
                            Assert.Equal(stats.End, reader.Last().Timestamp);
                        }
                    }
                }

                Assert.True(sp.Elapsed < (TimeSpan)retention + (TimeSpan)retention / 10);
            }
        }
    }
}
