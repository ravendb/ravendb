using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Client.Counters
{
    public class CountersCluster : ClusterTestBase
    {
        [Fact]
        public async Task IncrementCounter()
        {
            var leader = await CreateRaftClusterAndGetLeader(3);
            var dbName = GetDatabaseName();
            var db = await CreateDatabaseInCluster(dbName, 3, leader.WebUrl);

            var stores = db.Servers.Select(s => new DocumentStore
            {
                Database = dbName,
                Urls = new[] { s.WebUrl },
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            .ToList();

            try
            {
                using (var s = stores[0].OpenSession())
                {
                    s.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    s.Store(new User { Name = "Aviv" }, "users/1");
                    s.SaveChanges();
                }
                var tasks = new List<Task>();
                foreach (var store in stores)
                {
                    var task = store.Counters.IncrementAsync("users/1", "likes", 10);
                    tasks.Add(task);
                }

                Task.WaitAll(tasks.ToArray());

                foreach (var store in stores)
                {
                    long? val = null;
                    for (int i = 0; i < 100; i++)
                    {
                        val = store.Counters.Get("users/1", "likes");
                        if (val == 30)
                            break;
                        Thread.Sleep(50);
                    }

                    Assert.Equal(30, val);
                }
            }
            finally
            {
                foreach (var item in stores)
                {
                    item.Dispose();
                }
            }
        }

        [Fact]
        public async Task DeleteCounter()
        {
            var leader = await CreateRaftClusterAndGetLeader(3);
            var dbName = GetDatabaseName();
            var db = await CreateDatabaseInCluster(dbName, 3, leader.WebUrl);

            var stores = db.Servers.Select(s => new DocumentStore
                {
                    Database = dbName,
                    Urls = new[] { s.WebUrl },
                    Conventions = new DocumentConventions
                    {
                        DisableTopologyUpdates = true
                    }
                }.Initialize())
                .ToList();

            try
            {
                using (var s = stores[0].OpenSession())
                {
                    s.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    s.Store(new User { Name = "Aviv" }, "users/1");
                    s.Store(new User { Name = "Rotem" }, "users/2");

                    s.Advanced.Counters.Increment("users/1", "likes", 30);
                    s.Advanced.Counters.Increment("users/2", "downloads", 100);
                    s.SaveChanges();
                }

                long? val;
                foreach (var store in stores)
                {
                    val = store.Counters.Get("users/1", "likes");
                    Assert.Equal(30, val);
                    val = store.Counters.Get("users/2", "downloads");
                    Assert.Equal(100, val);
                }

                using (var s = stores[0].OpenSession())
                {
                    s.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    s.Advanced.Counters.Delete("users/1", "likes");
                    s.Advanced.Counters.Delete("users/2", "downloads");
                    s.SaveChanges();
                }

                foreach (var server in db.Servers)
                {
                    var documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(dbName);
                    using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var tombstones = documentDatabase.DocumentsStorage.GetTombstonesFrom(ctx, 0, 0, int.MaxValue).ToList();
                        Assert.Equal(2, tombstones.Count);
                        Assert.Equal(DocumentTombstone.TombstoneType.Counter, tombstones[0].Type);
                        Assert.Equal(DocumentTombstone.TombstoneType.Counter, tombstones[1].Type);
                    }
                }

                foreach (var store in stores)
                {
                    val = store.Counters.Get("users/1", "likes");
                    Assert.Null(val);
                    val = store.Counters.Get("users/2", "downloads");
                    Assert.Null(val);
                }
            }
            finally
            {
                foreach (var item in stores)
                {
                    item.Dispose();
                }
            }
        }

    }
}
