using System;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17702 : ReplicationTestBase
    {
        public RavenDB_17702(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.TimeSeries | RavenTestCategory.Cluster | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task InsertOldSegmentAfterDeletionProblem1(Options options)
        {
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using (var store = GetDocumentStore(new Options(options)
            {
                Server = cluster.Leader,
                ReplicationFactor = 3,
            }))
            {
                var now = DateTime.UtcNow;
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "users/1");
                    session.SaveChanges();
                }

                for (int i = 0; i < 31; i += 30)
                {
                    var ts1 = now.AddDays(i);
                    var ts2 = now.AddDays(i).AddSeconds(1);
                    using (var session = store.OpenSession())
                    {
                        session.TimeSeriesFor("users/1", "Heartrate")
                             .Append(ts1, 1);
                        session.TimeSeriesFor("users/1", "Heartrate")
                            .Append(ts2, 1);
                        session.SaveChanges();
                    }
                    using (var session = store.OpenSession())
                    {
                        session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);

                        session.TimeSeriesFor("users/1", "Heartrate")
                            .Delete(now.AddDays(i), now.AddDays(i).AddSeconds(1));
                        session.SaveChanges();
                    }
                    foreach (var server in cluster.Nodes)
                    {
                        var database = await GetDocumentDatabaseInstanceForAsync(store, options.DatabaseMode, "users/1", server);
                        var tss = database.DocumentsStorage.TimeSeriesStorage;
                        var res = await WaitForValueAsync(() =>
                        {
                            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                            using (ctx.OpenReadTransaction())
                            {
                                var id = $"users/1";
                                var stats = tss.Stats.GetStats(ctx, id, "Heartrate");
                                return stats.Count;
                            }
                        }, 0, 5000);
                        Assert.Equal(0, res);
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.TimeSeries | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task InsertOldSegmentAfterDeletionProblem2(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                var now = DateTime.UtcNow;
                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "users/1");
                    session.SaveChanges();
                }
                using (var session = store2.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "users/1");
                    session.SaveChanges();
                }
                var ts1 = now;
                var ts2 = now.AddDays(30);
                using (var session = store1.OpenSession())
                {
                    session.TimeSeriesFor("users/1", "Heartrate")
                        .Append(ts1, 1);
                    session.TimeSeriesFor("users/1", "Heartrate")
                        .Append(ts2, 1);
                    session.SaveChanges();
                }

                using (var session = store1.OpenSession())
                {
                    session.TimeSeriesFor("users/1", "Heartrate")
                        .Delete(now, now.AddDays(29));
                    session.SaveChanges();
                }


                ts1 = now.AddSeconds(2);
                ts2 = now.AddSeconds(3);
                using (var session = store2.OpenSession())
                {
                    session.TimeSeriesFor("users/1", "Heartrate")
                        .Append(ts1, 1);
                    session.TimeSeriesFor("users/1", "Heartrate")
                        .Append(ts2, 1);
                    session.SaveChanges();
                }
                await SetupReplicationAsync(store2, store1);
                await EnsureReplicatingAsync(store2, store1);

                await SetupReplicationAsync(store1, store2);
                var res = await WaitForValueAsync(() =>
                {
                    using (var session = store2.OpenSession())
                    {
                        var ts = session.TimeSeriesFor("users/1", "Heartrate")
                            .Get(DateTime.MinValue, DateTime.MaxValue);

                        return ts.Length;
                    }
                }, 1);
                res = await WaitForValueAsync(() =>
                {
                    using (var session = store1.OpenSession())
                    {
                        var ts = session.TimeSeriesFor("users/1", "Heartrate")
                            .Get(DateTime.MinValue, DateTime.MaxValue);

                        return ts.Length;
                    }
                }, 1);
            }
        }
    }
}
