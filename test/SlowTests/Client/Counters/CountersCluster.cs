using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.ServerWide;
using Raven.Server.Config;
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
        public async Task IncrementCounterShouldNotCreateRevisions()
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
                    s.Store(new User { Name = "Aviv", AddressId = new string('c', 1024 * 128) }, "users/1");
                    s.SaveChanges();
                }
                var tasks = new List<Task>();

                foreach (var store in stores)
                {
                    var task = Task.Run(async () =>
                    {
                        for (var i = 0; i < 100; i++)
                        {
                            using (var s = store.OpenAsyncSession())
                            {
                                s.CountersFor("users/1").Increment($"likes/{i}", 10);
                                await s.SaveChangesAsync();
                            }
                        }
                    });

                    tasks.Add(task);
                }

                Task.WaitAll(tasks.ToArray());

                using (var session = stores[0].OpenSession())
                {
                    Assert.Equal(0, session.Advanced.Revisions.GetFor<User>("users/1").Count);
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
                    var task = store.Operations.SendAsync(new CounterBatchOperation(new CounterBatch
                    {
                        Documents = new List<DocumentCountersOperation>
                        {
                            new DocumentCountersOperation
                            {
                                DocumentId = "users/1",
                                Operations = new List<CounterOperation>
                                {
                                    new CounterOperation
                                    {
                                        Type = CounterOperationType.Increment,
                                        CounterName = "likes",
                                        Delta = 10
                                    }
                                }
                            }
                        }
                    }));

                    tasks.Add(task);
                }

                Task.WaitAll(tasks.ToArray());

                foreach (var store in stores)
                {
                    long? val = null;
                    for (int i = 0; i < 100; i++)
                    {
                        val = store.Operations
                            .Send(new GetCountersOperation("users/1", new[] {"likes"}))
                            .Counters[0]?.TotalValue;

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

                    s.CountersFor("users/1").Increment("likes", 30);
                    s.CountersFor("users/2").Increment("downloads", 100);
                    s.SaveChanges();
                }

                long? val;
                foreach (var store in stores)
                {
                    val = store.Operations
                        .Send(new GetCountersOperation("users/1", new[] { "likes" }))
                        .Counters[0]?.TotalValue;
                    Assert.Equal(30, val);

                    val = store.Operations
                        .Send(new GetCountersOperation("users/2", new[] { "downloads" }))
                        .Counters[0]?.TotalValue;
                    Assert.Equal(100, val);
                }

                using (var s = stores[0].OpenSession())
                {
                    s.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    s.CountersFor("users/1").Delete("likes");
                    s.CountersFor("users/2").Delete("downloads");
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
                        Assert.Equal(Tombstone.TombstoneType.Counter, tombstones[0].Type);
                        Assert.Equal(Tombstone.TombstoneType.Counter, tombstones[1].Type);
                    }
                }

                foreach (var store in stores)
                {
                    Assert.Equal(0, store.Operations.Send(new GetCountersOperation("users/1", new[] { "likes" })).Counters.Count);
                    Assert.Equal(0, store.Operations.Send(new GetCountersOperation("users/2", new[] { "downloads" })).Counters.Count);
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
