using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Nito.AsyncEx;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15535 : ClusterTestBase
    {
        public RavenDB_15535(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task MissingRevisions()
        {
            var (node, leader) = await CreateRaftCluster(3, watcherCluster: true);
            var database = GetDatabaseName();
            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 3, leader.WebUrl, null);
            using (var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                var configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        PurgeOnDelete = false,
                        MinimumRevisionsToKeep = 30
                    }
                };
                long index;
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var configurationJson = EntityToBlittable.ConvertCommandToBlittable(configuration, context);
                    (index, _) = await leader.ServerStore.ModifyDatabaseRevisions(context, database, configurationJson, Guid.NewGuid().ToString());
                }
                await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));

                await StoreInTransactionMode(store, 1);
                await StoreInRegularMode(store, 10);
                await DeleteInTransactionMode(store, 1);

                RavenServer testServer = Servers.FirstOrDefault(server => server.ServerStore.IsLeader() == false);

                var result = await DisposeServerAndWaitForFinishOfDisposalAsync(testServer);

                var val = await WaitForValueAsync(async () => await GetMembersCount(store, database), 2, 20000);
                Assert.Equal(2, val);
                testServer = GetNewServer(new ServerCreationOptions
                {
                    DeletePrevious = true,
                    RunInMemory = false,
                    CustomSettings = new Dictionary<string, string>
                    {
                        [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = result.Url
                    }
                });

                using (var session = store.OpenSession(new SessionOptions {}))
                {
                      session.Store(new User() { Name = "userT" }, "users/1");
                      session.SaveChanges();
                      await WaitForDocumentInClusterAsync<User>((DocumentSession)session, "users/1", null, TimeSpan.FromSeconds(15));
                }

                var documentDatabase = await testServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                {
                     var res =  WaitForValue(() =>
                     {
                         using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                         using (context.OpenReadTransaction())
                             return documentDatabase.DocumentsStorage.RevisionsStorage.GetRevisionsCount(context, "users/1");
                     }, 13, 15000);

                    Assert.Equal(13, res);
                }
            }
        }

        [Fact]
        public async Task MissingRevisions2()
        {
            var (node, leader) = await CreateRaftCluster(3, watcherCluster: true);
            var database = GetDatabaseName();
            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 3, leader.WebUrl, null);
            using (var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                var configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        PurgeOnDelete = false,
                        MinimumRevisionsToKeep = 20
                    }
                };
                long index;
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var configurationJson = EntityToBlittable.ConvertCommandToBlittable(configuration, context);
                    (index, _) = await leader.ServerStore.ModifyDatabaseRevisions(context, database, configurationJson, Guid.NewGuid().ToString());
                }

                await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));

                var tasks = new List<Task>
                {
                    DeleteInTransactionMode(store, 1),
                    StoreInRegularMode(store, 3),
                };
                await tasks.WhenAll();

                using (var session = store.OpenSession())
                {
                     session.Store(new User() { Name = "Toli" }, "users/2");
                     session.SaveChanges();
                     await WaitForDocumentInClusterAsync<User>((DocumentSession)session, "users/2", null, TimeSpan.FromSeconds(15));
                }

                var revisionCountList = new List<long>();
                foreach (var server in Servers)
                {
                    var documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                    using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        revisionCountList.Add(documentDatabase.DocumentsStorage.RevisionsStorage.GetRevisionsCount(context, "users/1"));
                    }
                }

                Assert.Equal(revisionCountList[0], revisionCountList[1]);
                Assert.Equal(revisionCountList[0], revisionCountList[2]);
            }
        }

        [Fact]
        public async Task MissingRevisions3()
        {
            var (node, leader) = await CreateRaftCluster(3, watcherCluster: true);
            var database = GetDatabaseName();
            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 3, leader.WebUrl, null);
            using (var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                var configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        PurgeOnDelete = false,
                        MinimumRevisionsToKeep = 30
                    }
                };
                long index;
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var configurationJson = EntityToBlittable.ConvertCommandToBlittable(configuration, context);
                    (index, _) = await leader.ServerStore.ModifyDatabaseRevisions(context, database, configurationJson, Guid.NewGuid().ToString());
                }
                await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));

                await StoreInTransactionMode(store, 1);
                await StoreInRegularMode(store, 3);
                await DeleteInTransactionMode(store, 1);

                RavenServer testServer = Servers.FirstOrDefault(server => server.ServerStore.IsLeader() == false);

                var result = await leader.ServerStore.SendToLeaderAsync(new DeleteDatabaseCommand(database, Guid.NewGuid().ToString())
                {
                    HardDelete = true,
                    FromNodes = new[] { testServer.ServerStore.NodeTag },
                });

                await WaitForRaftIndexToBeAppliedInCluster(result.Index, TimeSpan.FromSeconds(10));

                var val = await WaitForValueAsync(async () => await GetMembersCount(store, database), 2, 20000);
                Assert.Equal(2, val);

                var delCount = WaitForValue(() =>
                {
                    var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(database));
                    return record.DeletionInProgress.Count;
                }, 0, 15000);
                Assert.Equal(0, delCount);

                await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(database, testServer.ServerStore.NodeTag));


                var documentDatabase = await testServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                {
                    var res = WaitForValue(() =>
                    {
                        using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                            return documentDatabase.DocumentsStorage.RevisionsStorage.GetRevisionsCount(context, "users/1");
                    }, 5, 15000);

                    Assert.Equal(5, res);
                }
            }
        }

        [Fact]
        public async Task MissingRevisions4()
        {
            var (node, leader) = await CreateRaftCluster(3, watcherCluster: true);
            var database = GetDatabaseName();
            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 3, leader.WebUrl, null);
            using (var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                var configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        PurgeOnDelete = false,
                        MinimumRevisionsToKeep = 30
                    }
                };
                long index;
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var configurationJson = EntityToBlittable.ConvertCommandToBlittable(configuration, context);
                    (index, _) = await leader.ServerStore.ModifyDatabaseRevisions(context, database, configurationJson, Guid.NewGuid().ToString());
                }
                await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Store(new User() { Name = "userT1"}, "users/1");
                    session.Delete("users/2");
                    session.SaveChanges();
                    await WaitForDocumentInClusterAsync<User>((DocumentSession)session, "users/1", null, TimeSpan.FromSeconds(15));
                }

                var revisionCountList = new List<long>();
                foreach (var server in Servers)
                {
                    var documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                    using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        revisionCountList.Add(documentDatabase.DocumentsStorage.RevisionsStorage.GetRevisionsCount(context, "users/1"));
                    }
                }

                Assert.Equal(revisionCountList[0], revisionCountList[1]);
                Assert.Equal(revisionCountList[0], revisionCountList[2]);
            }
        }

        [Fact]
        public async Task MissingRevisions5()
        {
            var (node, leader) = await CreateRaftCluster(3, watcherCluster: true);
            var database = GetDatabaseName();
            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 3, leader.WebUrl, null);
            using (var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                var configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        PurgeOnDelete = false,
                        MinimumRevisionsToKeep = 20
                    }
                };
                long index;
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var configurationJson = EntityToBlittable.ConvertCommandToBlittable(configuration, context);
                    (index, _) = await leader.ServerStore.ModifyDatabaseRevisions(context, database, configurationJson, Guid.NewGuid().ToString());
                }

                await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));

                var tasks = new List<Task>
                {
                    DeleteAndStoreInTransactionMode(store, 1),
                    StoreInRegularMode(store, 3),
                };
                await tasks.WhenAll();

                using (var session = store.OpenSession())
                {
                    session.Store(new User() { Name = "Toli" }, "users/3");
                    session.SaveChanges();
                    await WaitForDocumentInClusterAsync<User>((DocumentSession)session, "users/3", null, TimeSpan.FromSeconds(15));
                }

                var revisionCountList = new List<long>();
                foreach (var server in Servers)
                {
                    var documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                    using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        revisionCountList.Add(documentDatabase.DocumentsStorage.RevisionsStorage.GetRevisionsCount(context, "users/1"));
                    }
                }

                Assert.Equal(revisionCountList[0], revisionCountList[1]);
                Assert.Equal(revisionCountList[0], revisionCountList[2]);

                revisionCountList.Clear();
                foreach (var server in Servers)
                {
                    var documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                    using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        revisionCountList.Add(documentDatabase.DocumentsStorage.RevisionsStorage.GetRevisionsCount(context, "users/2"));
                    }
                }

                Assert.Equal(revisionCountList[0], revisionCountList[1]);
                Assert.Equal(revisionCountList[0], revisionCountList[2]);
            }
        }

        [Fact]
        public async Task MissingRevisions6()
        {
            var (node, leader) = await CreateRaftCluster(3, watcherCluster: true);
            var database = GetDatabaseName();
            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 3, leader.WebUrl, null);
            using (var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                var configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        PurgeOnDelete = false,
                        MinimumRevisionsToKeep = 20
                    }
                };
                long index;
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var configurationJson = EntityToBlittable.ConvertCommandToBlittable(configuration, context);
                    (index, _) = await leader.ServerStore.ModifyDatabaseRevisions(context, database, configurationJson, Guid.NewGuid().ToString());
                }

                await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));

                var tasks = new List<Task>
                {
                    StoreInRegularMode(store, 1),
                    DeleteAndStoreInTransactionMode(store, 1),
                    StoreInRegularMode(store, 1),
                };
                await tasks.WhenAll();

                using (var session = store.OpenSession())
                {
                    session.Store(new User() { Name = "Toli" }, "users/3");
                    session.SaveChanges();
                    await WaitForDocumentInClusterAsync<User>((DocumentSession)session, "users/3", null, TimeSpan.FromSeconds(15));
                }

                var revisionCountList = new List<long>();
                foreach (var server in Servers)
                {
                    var documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                    using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        revisionCountList.Add(documentDatabase.DocumentsStorage.RevisionsStorage.GetRevisionsCount(context, "users/1"));
                    }
                }

                Assert.Equal(revisionCountList[0], revisionCountList[1]);
                Assert.Equal(revisionCountList[0], revisionCountList[2]);

                revisionCountList.Clear();
                foreach (var server in Servers)
                {
                    var documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                    using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        revisionCountList.Add(documentDatabase.DocumentsStorage.RevisionsStorage.GetRevisionsCount(context, "users/2"));
                    }
                }

                Assert.Equal(revisionCountList[0], revisionCountList[1]);
                Assert.Equal(revisionCountList[0], revisionCountList[2]);
            }
        }

        private static async Task<int> GetMembersCount(IDocumentStore store, string databaseName)
        {
            var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
            if (res == null)
            {
                return -1;
            }
            return res.Topology.Members.Count;
        }

        private async Task StoreInTransactionMode(IDocumentStore store, int n)
        {
            for (int i = 0; i < n; i++)
            {
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    await session.StoreAsync(new User() { Name = "userT" + i }, "users/1");
                    await session.SaveChangesAsync();
                }
            }
        }

        private async Task DeleteInTransactionMode(IDocumentStore store, int n)
        {
            for (int i = 0; i < n; i++)
            {
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Delete( "users/1");
                    await session.SaveChangesAsync();
                }
            }

        }
        private async Task DeleteAndStoreInTransactionMode(IDocumentStore store, int n)
        {
            for (int i = 0; i < n; i++)
            {
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Delete("users/1");
                    await session.StoreAsync(new User() { Name = "userT" + i }, "users/2");
                    await session.SaveChangesAsync();
                }
            }

        }
        private async Task StoreInRegularMode(IDocumentStore store, int n)
        {
            for (int i = 0; i < n; i++)
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User() { Name = "userR" + i }, "users/1");
                    await session.SaveChangesAsync();
                }
            }

        }

        private async Task DeleteInRegularMode(IDocumentStore store, int n)
        {
            for (int i = 0; i < n; i++)
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.Delete( "users/1");
                    await session.SaveChangesAsync();
                }
            }

        }
    }
}
