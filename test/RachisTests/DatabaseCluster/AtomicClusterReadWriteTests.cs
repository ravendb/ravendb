using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.Util;
using Raven.Server;
using Sparrow.Server;
using Xunit;
using Xunit.Abstractions;

namespace RachisTests.DatabaseCluster
{
    public class AtomicClusterReadWriteTests : ReplicationTestBase
    {
        public AtomicClusterReadWriteTests(ITestOutputHelper output) : base(output)
        {
        }

        class TestObj
        {
            public string Id { get; set; }
            public string Prop { get; set; }
        }

        [Fact]
        public async Task ClusterWideTransaction_WhenStore_ShouldCreateCompareExchange()
        {
            var (nodes, leader) = await CreateRaftCluster(3);
            using var store = GetDocumentStore(new Options {Server = leader, ReplicationFactor = nodes.Count});

            var entity = new TestObj();
            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(entity);
                await session.SaveChangesAsync();
            }

            var result = await store.Operations.SendAsync(new GetCompareExchangeValuesOperation<TestObj>(""));
            Assert.Single(result);
            Assert.EndsWith(entity.Id, result.Single().Key, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public async Task ClusterWideTransaction_WhenDisableAndStore_ShouldNotCreateCompareExchange()
        {
            var (nodes, leader) = await CreateRaftCluster(3);
            using var store = GetDocumentStore(new Options {Server = leader, ReplicationFactor = nodes.Count});

            var entity = new TestObj();
            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide, DisableAtomicDocumentWritesInClusterWideTransaction = true}))
            {
                await session.StoreAsync(entity);
                await session.SaveChangesAsync();
            }

            var result = await store.Operations.SendAsync(new GetCompareExchangeValuesOperation<TestObj>(""));
            Assert.Empty(result);
        }

        [Fact]
        public async Task ClusterWideTransaction_WhenLoadAndUpdateInParallel_ShouldSucceedOnlyInTheFirst()
        {
            var (nodes, leader) = await CreateRaftCluster(3);
            using var documentStore = GetDocumentStore(new Options {Server = leader, ReplicationFactor = nodes.Count});

            using var disposable = LocalGetDocumentStores(nodes, documentStore.Database, out var stores);

            var entity = new TestObj();
            using (var session = documentStore.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(entity);
                await session.SaveChangesAsync();
            }
            await WaitForDocumentInClusterAsync<TestObj>(documentStore.GetRequestExecutor().Topology.Nodes, entity.Id, u => u != null, TimeSpan.FromSeconds(10));

            var barrier = new Barrier(3);
            var exceptions = new ConcurrentBag<Exception>();
            var tasks = Enumerable.Range(0, stores.Length)
                .Select(i => Task.Run(async () =>
                {
                    using var session = stores[i].OpenAsyncSession(new SessionOptions {TransactionMode = TransactionMode.ClusterWide});
                    var loaded = await session.LoadAsync<TestObj>(entity.Id);
                    barrier.SignalAndWait();

                    loaded.Prop = "Change" + i;

                    try
                    {
                        await session.SaveChangesAsync();
                    }
                    catch (Exception e)
                    {
                        exceptions.Add(e);
                    }
                }));

            await Task.WhenAll(tasks);
            Assert.Equal(2, exceptions.Count);
            foreach (var exception in exceptions)
            {
                Assert.IsType<ConcurrencyException>(exception);
            }
        }

        [Fact]
        public async Task ClusterWideTransaction_WhenLoadAndDeleteWhileUpdated_ShouldFailDeletion()
        {
            var (nodes, leader) = await CreateRaftCluster(3, shouldRunInMemory: false);

            using var documentStore = GetDocumentStore(new Options {Server = leader, ReplicationFactor = nodes.Count});

            var entity = new TestObj();
            using (var session = documentStore.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(entity);
                await session.SaveChangesAsync();
            }

            await LoadAndDeleteWhileUpdated(nodes, documentStore.Database, entity.Id);
        }

        [Fact]
        public async Task ClusterWideTransaction_WhenImportThenLoadAndDeleteWhileUpdated_ShouldFailDeletion()
        {
            var (nodes, leader) = await CreateRaftCluster(3);
            using var documentStore = GetDocumentStore(new Options { Server = leader, ReplicationFactor = nodes.Count });

            var entity = new TestObj();
            using (var source = GetDocumentStore())
            {
                using var session = source.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide });
                await session.StoreAsync(entity);
                await session.SaveChangesAsync();

                var operation = await source.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), documentStore.Smuggler);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            }

            await LoadAndDeleteWhileUpdated(nodes, documentStore.Database, entity.Id);
        }

        private async Task LoadAndDeleteWhileUpdated(List<RavenServer> nodes, string database, string entityId)
        {
            using var disposable = LocalGetDocumentStores(nodes, database, out var stores);
            foreach (IDocumentStore store in stores)
            {
                WaitForDocument<object>(store, entityId, o => o != null);
            }

            var amre = new AsyncManualResetEvent();
            var amre2 = new AsyncManualResetEvent();
            var task = Task.Run(async () =>
            {
                using var session = stores[0].OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide });
                var loaded = await session.LoadAsync<TestObj>(entityId);
                amre.Set();

                session.Delete(loaded);

                await amre2.WaitAsync();
                await Assert.ThrowsAnyAsync<ConcurrencyException>(() => session.SaveChangesAsync());
            });
            await amre.WaitAsync();
            using (var session = stores[1].OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var loaded = await session.LoadAsync<TestObj>(entityId);
                loaded.Prop = "Changed";
                await session.SaveChangesAsync();
                amre2.Set();
            }

            await task;
        }

        [Theory]
        [InlineData(1)]
        [InlineData(1, false)]
        [InlineData(2 * 1024)]// DatabaseDestination.DatabaseCompareExchangeActions.BatchSize
        public async Task ClusterWideTransaction_WhenRestoreFromIncrementalBackupAfterStoreAndDelete_ShouldDeleteInTheDestination(int count, bool withLoad = true)
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");

            var (nodes, leader) = await CreateRaftCluster(3);
            using var documentStore = GetDocumentStore(new Options { Server = leader, ReplicationFactor = nodes.Count });

            var notDelete = $"TestObjs/{count}";
            using (var source = GetDocumentStore())
            {
                var config = new PeriodicBackupConfiguration { LocalSettings = new LocalSettings { FolderPath = backupPath }, IncrementalBackupFrequency = "0 0 */12 * *" };
                var backupTaskId = (await source.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;

                using (var session = source.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide}))
                {
                    for (int i = 0; i < count; i++)
                    {
                        await session.StoreAsync(new TestObj(), $"TestObjs/{i}");
                    }
                    await session.StoreAsync(new TestObj(), notDelete);
                    await session.SaveChangesAsync();
                }

                var backupStatus = await source.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                await backupStatus.WaitForCompletionAsync();

                using (var session = source.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.MaxNumberOfRequestsPerSession += count;
                    for (int i = 0; i < count; i++)
                    {
                        if(withLoad)
                            await session.LoadAsync<TestObj>($"TestObjs/{i}");
                        session.Delete($"TestObjs/{i}");
                    }
                    
                    await session.SaveChangesAsync();
                }

                var backupStatus2 = await source.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                await backupStatus2.WaitForCompletionAsync();

                await documentStore.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(), Directory.GetDirectories(backupPath).First());
            }

            await AssertClusterWaitForNotNull(nodes, documentStore.Database, async s =>
            {
                using var session = s.OpenAsyncSession();
                return await session.LoadAsync<TestObj>(notDelete);
            });

            var r = await AssertWaitForSingleAsync(async () => await documentStore.Operations.SendAsync(new GetCompareExchangeValuesOperation<TestObj>("")));
            Assert.EndsWith(notDelete, r.Single().Key, StringComparison.OrdinalIgnoreCase);
        }

        [Theory]
        [InlineData(1)]
        [InlineData(2 * 1024)]// DatabaseDestination.DatabaseCompareExchangeActions.BatchSize
        public async Task ClusterWideTransaction_WhenRestoreFromIncrementalBackupAfterStoreAndUpdate_ShouldCompleteImportWithNoException(int count)
        {
            const string modified = "Modified";
            var backupPath = NewDataPath(suffix: "BackupFolder");

            var (nodes, leader) = await CreateRaftCluster(3);
            using var documentStore = GetDocumentStore(new Options { Server = leader, ReplicationFactor = nodes.Count });

            var notToModify = $"TestObjs/{count}";
            using (var source = GetDocumentStore())
            {
                var config = new PeriodicBackupConfiguration { LocalSettings = new LocalSettings { FolderPath = backupPath }, IncrementalBackupFrequency = "0 0 */12 * *" };
                var backupTaskId = (await source.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;

                using (var session = source.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide}))
                {
                    for (int i = 0; i < count; i++)
                    {
                        await session.StoreAsync(new TestObj(), $"TestObjs/{i}");
                    }
                    await session.StoreAsync(new TestObj(), notToModify);
                    await session.SaveChangesAsync();
                }

                var backupStatus = await source.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                await backupStatus.WaitForCompletionAsync();

                using (var session = source.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.MaxNumberOfRequestsPerSession += count;

                    for (int i = 0; i < count; i++)
                    {
                        var r = await session.LoadAsync<TestObj>($"TestObjs/{i}");
                        r.Prop = modified;
                        await session.StoreAsync(r);
                    }

                    await session.SaveChangesAsync();
                }

                var backupStatus2 = await source.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                await backupStatus2.WaitForCompletionAsync();

                await documentStore.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(), Directory.GetDirectories(backupPath).First());
            }

            await AssertClusterWaitForNotNull(nodes, documentStore.Database, async s =>
            {
                using var session = s.OpenAsyncSession();
                return await session.LoadAsync<TestObj>(notToModify);
            });
            
            await AssertClusterWaitForValue(nodes, documentStore.Database, async s =>
            {
                using var session = s.OpenAsyncSession();
                var loadAsync = await session.LoadAsync<TestObj>($"TestObjs/{count - 1}");
                return loadAsync?.Prop;
            }, modified);
            
            await AssertWaitForCountAsync(async () => await documentStore.Operations.SendAsync(new GetCompareExchangeValuesOperation<TestObj>("")), count + 1);
        }

        [Fact]
        public async Task ClusterWideTransaction_WhenRestoreFromIncrementalBackupAfterStoreAndUpdateWithoutLoad_ShouldFail()
        {
            const string docId = "TestObjs/1";
            using var source = GetDocumentStore();
            using (var session = source.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(new TestObj(), docId);
                await session.SaveChangesAsync();
            }

            using (var session = source.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(new TestObj { Prop = "Modified" }, docId);
                await Assert.ThrowsAnyAsync<ConcurrencyException>(async () => await session.SaveChangesAsync());
            }
        }

        [Fact]
        public async Task ClusterWideTransaction_WhenLoadAndUpdateWhileDeleted_ShouldFailUpdate()
        {
            var (nodes, leader) = await CreateRaftCluster(3, shouldRunInMemory: false);

            using var documentStore = GetDocumentStore(new Options {Server = leader, ReplicationFactor = nodes.Count});

            var entity = new TestObj();
            using (var session = documentStore.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(entity);
                await session.SaveChangesAsync();
            }

            await LoadAndUpdateWhileDeleted(nodes, documentStore.Database, entity.Id);
        }
        
        [Fact]
        public async Task ClusterWideTransaction_WhenImportThenLoadAndUpdateWhileDeleted_ShouldFailUpdate()
        {
            var (nodes, leader) = await CreateRaftCluster(3);
            using var documentStore = GetDocumentStore(new Options { Server = leader, ReplicationFactor = nodes.Count });

            var entity = new TestObj();
            using (var source = GetDocumentStore())
            {
                using var session = source.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide });
                await session.StoreAsync(entity);
                await session.SaveChangesAsync();

                var operation = await source.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), documentStore.Smuggler);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            }

            await LoadAndUpdateWhileDeleted(nodes, documentStore.Database, entity.Id);
        }

        private async Task LoadAndUpdateWhileDeleted(List<RavenServer> nodes, string database, string entityId)
        {
            using var disposable = LocalGetDocumentStores(nodes, database, out var stores);

            foreach (IDocumentStore store in stores)
            {
                WaitForDocument<object>(store, entityId, o => o != null);
            }
            
            var amre = new AsyncManualResetEvent();
            var amre2 = new AsyncManualResetEvent();
            var task = Task.Run(async () =>
            {
                using var session = stores[0].OpenAsyncSession(new SessionOptions {TransactionMode = TransactionMode.ClusterWide});
                var loaded = await session.LoadAsync<TestObj>(entityId);
                amre.Set();

                loaded.Prop = "Changed";

                await amre2.WaitAsync();
                await Assert.ThrowsAnyAsync<ConcurrencyException>(() => session.SaveChangesAsync());
            });
            await amre.WaitAsync();
            using (var session = stores[1].OpenAsyncSession(new SessionOptions {TransactionMode = TransactionMode.ClusterWide}))
            {
                var loaded = await session.LoadAsync<TestObj>(entityId);
                session.Delete(loaded);
                await session.SaveChangesAsync();

                amre2.Set();
            }

            await task;
        }

        private static IDisposable LocalGetDocumentStores(List<RavenServer> nodes, string database, out IDocumentStore[] stores)
        {
            var urls = nodes.Select(n => n.WebUrl).ToArray();

            return LocalGetDocumentStores(urls, database, out stores);
        }

        private static IDisposable LocalGetDocumentStores(string[] urls, string database, out IDocumentStore[] stores)
        {
            stores = new IDocumentStore[urls.Length];
            var internalStore = stores;
            var disposable = new DisposableAction(() =>
            {
                foreach (var s in internalStore)
                {
                    try
                    {
                        s?.Dispose();
                    }
                    catch
                    {
                        // ignored
                    }
                }
            });

            for (int i = 0; i < urls.Length; i++)
            {
                var store = new DocumentStore {Urls = new[] {urls[i]}, Database = database, Conventions = new DocumentConventions {DisableTopologyUpdates = true}}.Initialize();
                stores[i] = store;
            }

            return disposable;
        }
    }
}
