using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Conventions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_19152 : ReplicationTestBase
    {
        public RavenDB_19152(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Cluster, Skip = "Skip until RavenDB-21796 is resolved (revision schema upgrade)")]
        public async Task ShouldNotSkipConflictedRevisions_InternalReplication()
        {
            var cluster = await CreateRaftCluster(3);
            var database = GetDatabaseName();

            var server1 = cluster.Nodes[0].ServerStore;
            var server2 = cluster.Nodes[1].ServerStore;

            using var store1 = GetDocumentStore(new Options
            {
                Server = server1.Server,
                CreateDatabase = false,
                ModifyDocumentStore = s => s.Conventions = new DocumentConventions { DisableTopologyUpdates = true },
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Replication.MaxItemsCount)] = "1",
                ModifyDatabaseName = _ => database
            });
            using var store2 = GetDocumentStore(new Options
            {
                Server = server2.Server,
                CreateDatabase = false,
                ModifyDocumentStore = s => s.Conventions = new DocumentConventions { DisableTopologyUpdates = true },
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Replication.MaxItemsCount)] = "1",
                ModifyDatabaseName = _ => database
            });

            store1.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(database)
            {
                Topology = new DatabaseTopology
                {
                    Members =
                    [
                        server1.NodeTag,
                        server2.NodeTag
                    ]
                },
                Settings = new Dictionary<string, string>()
                {
                    {
                        "Replication.MaxItemsCount",
                        "1"
                    }
                }
            }, 2));

            var dbA = await server1.DatabasesLandlord.TryGetOrCreateResourceStore(database);
            var dbB = await server2.DatabasesLandlord.TryGetOrCreateResourceStore(database);

            await EnsureReplicatingAsync(store1, store2);
            await EnsureReplicatingAsync(store2, store1);

            var rep1 = await BreakReplication(server1, database);
            var rep2 = await BreakReplication(server2, database);

            var id = "foo/bar";

            // create a conflict 

            using (var session = store1.OpenAsyncSession())
            {
                var user = new User { Name = "1-A" };
                await session.StoreAsync(user, id);
                await session.SaveChangesAsync();
            }

            using (var session = store2.OpenAsyncSession())
            {
                var user = new User { Name = "1-B" };
                await session.StoreAsync(user, id);
                await session.SaveChangesAsync();
            }

            rep1.Mend();
            rep2.Mend();

            // wait until the conflict is resolved

            await AssertWaitForValueAsync(async () =>
            {
                using (var session1 = store1.OpenAsyncSession())
                {
                    var rev = await session1.Advanced.Revisions.GetMetadataForAsync(id);
                    return rev.Count;
                }
            }, 3);

            await AssertWaitForValueAsync(async () =>
            {
                using (var session2 = store2.OpenAsyncSession())
                {
                    var rev = await session2.Advanced.Revisions.GetMetadataForAsync(id);
                    return rev.Count;
                }
            }, 3);

            // halt replication to simulate out-of-sync updates

            dbA.ReplicationLoader.DebugWaitAndRunReplicationOnce = new ManualResetEventSlim();
            dbB.ReplicationLoader.DebugWaitAndRunReplicationOnce = new ManualResetEventSlim();

            using (var session = store1.OpenAsyncSession())
            {
                var user = new User { Name = "2-A" };
                await session.StoreAsync(user, id);
                await session.SaveChangesAsync();
            }

            using (var session = store2.OpenAsyncSession())
            {
                var user = new User { Name = "2-B" };
                await session.StoreAsync(user, id);
                await session.SaveChangesAsync();
            }

            // send document "foo/bar" with `Name="2-A"` from A to B
            dbA.ReplicationLoader.DebugWaitAndRunReplicationOnce.Set();

            using (var session = store1.OpenAsyncSession())
            {
                var user = new User { Name = "3-A" };
                await session.StoreAsync(user, id);
                await session.SaveChangesAsync();
            }

            // send document "foo/bar" with `Name="2-B"` from B to A
            dbB.ReplicationLoader.DebugWaitAndRunReplicationOnce.Set();

            using (var session = store1.OpenAsyncSession())
            {
                var user = new User { Name = "4-A" };
                await session.StoreAsync(user, id);
                await session.SaveChangesAsync();
            }

            dbA.ReplicationLoader.DebugWaitAndRunReplicationOnce.Set();
            using (var session = store1.OpenAsyncSession())
            {
                var user = new User { Name = "foo4-A" };
                await session.StoreAsync(user, "foo/bar/4");
                await session.SaveChangesAsync();
            }

            // resume replication from A to B
            dbA.ReplicationLoader.DebugWaitAndRunReplicationOnce.Set();
            dbA.ReplicationLoader.DebugWaitAndRunReplicationOnce = null;

            // wait for heartbeat message from B to A 
            await Task.Delay(3000);

            // resume replication from B to A
            dbB.ReplicationLoader.DebugWaitAndRunReplicationOnce.Set();
            dbB.ReplicationLoader.DebugWaitAndRunReplicationOnce = null;

            await EnsureReplicatingAsync(store1, store2);
            await EnsureReplicatingAsync(store2, store1);

            using (var session1 = store1.OpenAsyncSession())
            using (var session2 = store2.OpenAsyncSession())
            {
                var doc1 = await session1.LoadAsync<User>(id);
                var doc2 = await session2.LoadAsync<User>(id);

                Assert.Equal(doc1.Name, doc2.Name);

                var rev1 = await session1.Advanced.Revisions.GetMetadataForAsync(id, pageSize: int.MaxValue);
                var rev2 = await session2.Advanced.Revisions.GetMetadataForAsync(id, pageSize: int.MaxValue);

                Assert.Equal(rev1.Count, rev2.Count);
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Cluster, Skip = "Skip until RavenDB-21796 is resolved (revision schema upgrade)")]
        public async Task ShouldUpdateSiblingsChangeVector()
        {
            var cluster = await CreateRaftCluster(3, watcherCluster: true, leaderIndex: 0);
            var database = GetDatabaseName();

            var server1 = cluster.Nodes[0].ServerStore;
            var server2 = cluster.Nodes[1].ServerStore;
            var server3 = cluster.Nodes[2].ServerStore;

            using var store1 = GetDocumentStore(new Options
            {
                Server = server1.Server,
                CreateDatabase = false,
                ModifyDocumentStore = s => s.Conventions = new DocumentConventions { DisableTopologyUpdates = true },
                ModifyDatabaseName = _ => database
            });
            using var store2 = GetDocumentStore(new Options
            {
                Server = server2.Server,
                CreateDatabase = false,
                ModifyDocumentStore = s => s.Conventions = new DocumentConventions { DisableTopologyUpdates = true },
                ModifyDatabaseName = _ => database
            });
            using var store3 = GetDocumentStore(new Options
            {
                Server = server3.Server,
                CreateDatabase = false,
                ModifyDocumentStore = s => s.Conventions = new DocumentConventions { DisableTopologyUpdates = true },
                ModifyDatabaseName = _ => database
            });

            store1.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(database)
            {
                Topology = new DatabaseTopology
                {
                    Members =
                    [
                        server1.NodeTag,
                        server2.NodeTag,
                        server3.NodeTag
                    ]
                }
            }, 3));

            // perform writes only on A 

            using (var session = store1.OpenAsyncSession())
            {
                var user = new User { Name = "1-A" };
                await session.StoreAsync(user, "foo/bar");
                await session.SaveChangesAsync();
            }

            Assert.True(WaitForDocument(store2, "foo/bar"));
            Assert.True(WaitForDocument(store3, "foo/bar"));

            Assert.True(await WaitForChangeVectorInClusterAsync(cluster.Nodes, database));

            var dbA = await server1.DatabasesLandlord.TryGetOrCreateResourceStore(database);
            var dbB = await server2.DatabasesLandlord.TryGetOrCreateResourceStore(database);
            var dbC = await server3.DatabasesLandlord.TryGetOrCreateResourceStore(database);

            using (dbA.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var changeVector = DocumentsStorage.GetDatabaseChangeVector(ctx);

                var etagA = ChangeVectorUtils.GetEtagById(changeVector, dbA.DbBase64Id);
                var etagB = ChangeVectorUtils.GetEtagById(changeVector, dbB.DbBase64Id);
                var etagC = ChangeVectorUtils.GetEtagById(changeVector, dbC.DbBase64Id);

                // ensure that all nodes have the same Etag (indicating consistent replication state)

                Assert.Equal(1, etagA);
                Assert.Equal(1, etagB);
                Assert.Equal(1, etagC);
            }

            using (var session = store1.OpenAsyncSession())
            {
                var user = new User { Name = "2-A" };
                await session.StoreAsync(user, "foo/bar2");
                await session.SaveChangesAsync();
            }

            Assert.True(WaitForDocument(store2, "foo/bar2"));
            Assert.True(WaitForDocument(store3, "foo/bar2"));

            Assert.True(await WaitForChangeVectorInClusterAsync(cluster.Nodes, database));

            using (dbA.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var changeVector = DocumentsStorage.GetDatabaseChangeVector(ctx);

                var etagA = ChangeVectorUtils.GetEtagById(changeVector, dbA.DbBase64Id);
                var etagB = ChangeVectorUtils.GetEtagById(changeVector, dbB.DbBase64Id);
                var etagC = ChangeVectorUtils.GetEtagById(changeVector, dbC.DbBase64Id);

                Assert.Equal(2, etagA);
                Assert.Equal(2, etagB);
                Assert.Equal(2, etagC);
            }
        }
    }
}
