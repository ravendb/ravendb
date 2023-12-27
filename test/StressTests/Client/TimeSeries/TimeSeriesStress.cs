using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Client.TimeSeries.Patch;
using SlowTests.Client.TimeSeries.Replication;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace StressTests.Client.TimeSeries
{
    public class TimeSeriesStress : ReplicationTestBase
    {
        public TimeSeriesStress(ITestOutputHelper output) : base(output)
        {
        }

        [MultiplatformFact(RavenArchitecture.AllX64)]
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

                    Assert.True(await WaitForDocumentInClusterAsync<User>(cluster.Nodes, store.Database, "marker", null, TimeSpan.FromSeconds(15)));
                }

                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                var sp = Stopwatch.StartNew();
                await Task.Delay((TimeSpan)retention / 2);

                var debug = new Dictionary<string, (long Count, DateTime Start, DateTime End)>();
                var check = true;
                while (check)
                {
                    await AssertTimeElapsed(store, sp, retention, debug);

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
                            {
                                debug.Remove(server.ServerStore.NodeTag);
                                continue;
                            }

                            check = true;
                            Assert.Equal(stats.Start, reader.First().Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                            Assert.Equal(stats.End, reader.Last().Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                            debug[server.ServerStore.NodeTag] = stats;
                        }
                    }
                }
                Assert.Empty(debug);
                Assert.True(sp.Elapsed < (TimeSpan)retention + (TimeSpan)retention);
                await Task.Delay(3000); // let the dust to settle

                await EnsureNoReplicationLoop(Servers[0], store.Database);
                await EnsureNoReplicationLoop(Servers[1], store.Database);
                await EnsureNoReplicationLoop(Servers[2], store.Database);

                foreach (var server in Servers)
                {
                    var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    await TimeSeriesReplicationTests.AssertNoLeftOvers(database);
                }
            }

            async Task AssertTimeElapsed(DocumentStore store, Stopwatch sp, TimeValue retention, Dictionary<string, (long Count, DateTime Start, DateTime End)> debug)
            {
                try
                {
                    Assert.True(sp.Elapsed < ((TimeSpan)retention).Add((TimeSpan)retention),
                        $"too long has passed {sp.Elapsed}, retention is {retention} {Environment.NewLine}" +
                        $"debug: {string.Join(',', debug.Select(kvp => $"{kvp.Key}: ({kvp.Value.Count},{kvp.Value.Start},{kvp.Value.End})"))}");
                }
                catch (TrueException e)
                {
                    var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    var errorMessage = e.Message +
                                       $"{Environment.NewLine}Database topology: Members: [{string.Join(", ", record.Topology.Members)}], Rehabs: [{string.Join(", ", record.Topology.Rehabs)}]";
                    throw new TrueException(errorMessage, false);
                }
         
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

                        session.Store(new User { Name = "Karmel" }, id);

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
                    session.Store(new User(), "marker");
                    session.SaveChanges();
                    Assert.True(await WaitForDocumentInClusterAsync<User>(cluster.Nodes, store.Database, "marker", null, TimeSpan.FromSeconds(15)));
                }

                var sp = Stopwatch.StartNew();
                await Task.Delay(retention / 2);

                var check = true;
                while (check)
                {
                    Assert.True(sp.Elapsed < retention.Add(retention * 5), $"too long has passed {sp.Elapsed}, retention is {retention}");
                    await Task.Delay(200);
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

        [MultiplatformFact(RavenArchitecture.AllX64)]
        public async Task PatchTimestamp_IntegrationTest_x64()
        {
            await PatchTimestamp_IntegrationTest(8_192);
        }

        [MultiplatformFact(RavenArchitecture.AllX86)]
        public async Task PatchTimestamp_IntegrationTest_x86()
        {
            await PatchTimestamp_IntegrationTest(4_096);
        }

     
        public async Task PatchTimestamp_IntegrationTest(int docAmount)
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;

            string[] tags = { "tag/1", "tag/2", "tag/3", "tag/4", null };
            const string timeseries = "Heartrate";
            const int timeSeriesPointsAmount = 128;

            using (var store = GetDocumentStore())
            {
                await using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 0; i < docAmount; i++)
                    {
                        await bulkInsert.StoreAsync(new TimeSeriesPatchTests.TimeSeriesResultHolder(), $"TimeSeriesResultHolders/{i}");
                    }
                }

                var baseTime = new DateTime(2020, 2, 12);
                var randomValues = new Random(2020);
                var toAppend = Enumerable.Range(0, timeSeriesPointsAmount)
                    .Select(i =>
                    {
                        return new TimeSeriesEntry
                        {
                            Tag = tags[i % tags.Length],
                            Timestamp = baseTime.AddSeconds(i).AddSeconds(.1 * (randomValues.NextDouble() - .5)),
                            Values = new[] { 256 + 16 * randomValues.NextDouble() }
                        };
                    }).ToArray();

                var appendOperation = store
                    .Operations
                    .Send(new PatchByQueryOperation(new IndexQuery
                    {
                        QueryParameters = new Parameters
                        {
                            {"timeseries", timeseries},
                            {"toAppend", toAppend},
                        },
                        Query = @"
from TimeSeriesResultHolders as c
update
{
    for(var i = 0; i < $toAppend.length; i++){
        timeseries(this, $timeseries).append($toAppend[i].Timestamp, $toAppend[i].Values, $toAppend[i].Tag);
    }
}"
                    }));

#if DEBUG
                TimeSpan time = TimeSpan.FromMinutes(8);
#else
                TimeSpan time = TimeSpan.FromMinutes(5);
#endif
                await appendOperation.WaitForCompletionAsync(time);
                var deleteFrom = toAppend[timeSeriesPointsAmount * 1 / 3].Timestamp;
                var deleteTo = toAppend[timeSeriesPointsAmount * 3 / 4].Timestamp;
                var deleteOperation = store
                    .Operations
                    .Send(new PatchByQueryOperation(new IndexQuery
                    {
                        QueryParameters = new Parameters
                        {
                            {"timeseries", timeseries},
                            {"from", deleteFrom},
                            {"to", deleteTo}
                        },
                        Query = @"
from TimeSeriesResultHolders as c
update
{
  timeseries(this, $timeseries).delete($from, $to);
}"
                    }));
                await deleteOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                var getFrom = toAppend[timeSeriesPointsAmount * 1 / 5].Timestamp;
                var getTo = toAppend[timeSeriesPointsAmount * 4 / 5].Timestamp;
                var getOperation = store
                    .Operations
                    .Send(new PatchByQueryOperation(new IndexQuery
                    {
                        QueryParameters = new Parameters
                        {
                            {"timeseries", timeseries},
                            {"from", getFrom},
                            {"to", getTo}
                        },
                        Query = @"
from TimeSeriesResultHolders as c
update
{
  this.Result = timeseries(this, $timeseries).get($from, $to);
}"
                    }));
                await getOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                using (var session = store.OpenAsyncSession())
                {
                    var docs = await session
                        .Query<TimeSeriesPatchTests.TimeSeriesResultHolder>()
                        .Customize(x => x.WaitForNonStaleResults(TimeSpan.FromSeconds(5)))
                        .ToArrayAsync();

                    foreach (var doc in docs)
                    {
                        var expectedList = toAppend
                            .Where(s => s.Timestamp >= getFrom && s.Timestamp <= getTo)
                            .Where(s => s.Timestamp < deleteFrom || s.Timestamp > deleteTo)
                            .ToArray();

                        Assert.Equal(expectedList.Length, doc.Result.Length);
                        for (int i = 0; i < expectedList.Length; i++)
                        {
                            var expected = expectedList[i];
                            var actual = doc.Result[i];
                            if (expected.Timestamp < getFrom || expected.Timestamp > getTo)
                                continue;
                            if (expected.Timestamp >= deleteFrom || expected.Timestamp <= deleteTo)
                                continue;

                            Assert.Equal(expected.Timestamp, actual.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                            Assert.Equal(expected.Values, actual.Values);
                            Assert.Equal(expected.Tag, actual.Tag);
                        }
                    }
                }
            }
        }
    }
}
