using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Voron.Util;
using Newtonsoft.Json;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Config;
using Sparrow.Extensions;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using Directory = System.IO.Directory;

namespace RachisTests.DatabaseCluster
{
    public class AtomicClusterReadWriteTests : ReplicationTestBase
    {
        public AtomicClusterReadWriteTests(ITestOutputHelper output) : base(output)
        {
        }

        protected IDocumentStore InternalGetDocumentStore(Options options = null, string caller = null)
        {
            return GetDocumentStore(options, caller);
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
                var store = new DocumentStore { Urls = new[] { urls[i] }, Database = database, Conventions = new DocumentConventions { DisableTopologyUpdates = true } }
                    .Initialize();
                stores[i] = store;
            }

            return disposable;
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

                await amre2.WaitAsync(TimeSpan.FromSeconds(10));
                await Assert.ThrowsAnyAsync<ConcurrencyException>(() => session.SaveChangesAsync());
            });

            await amre.WaitAsync(TimeSpan.FromSeconds(10));
            using (var session = stores[1].OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var loaded = await session.LoadAsync<TestObj>(entityId);
                loaded.Prop = "Changed";
                await session.SaveChangesAsync();
                amre2.Set();
            }

            await task;
        }

        private async Task LoadAndUpdateWhileDeleted(List<RavenServer> nodes, string database, string entityId)
        {
            using var disposable = LocalGetDocumentStores(nodes, database, out var stores);

            foreach (IDocumentStore store in stores)
            {
                WaitForDocument<object>(store, entityId, o => o != null);
            }

            using var session = stores[0].OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide });
            var loaded = await session.LoadAsync<TestObj>(entityId);

            using (var deleteSession = stores[1].OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var toDelete = await deleteSession.LoadAsync<TestObj>(entityId);
                deleteSession.Delete(toDelete);
                await deleteSession.SaveChangesAsync();
            }

            loaded.Prop = "Changed";
            await Assert.ThrowsAnyAsync<ConcurrencyException>(() => session.SaveChangesAsync());
        }

        class TestObj
        {
            public string Id { get; set; }
            public string Prop { get; set; }
        }

        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ClusterWideTransaction_WhenStore_ShouldCreateCompareExchange(Options options)
        {
            var (nodes, leader) = await CreateRaftCluster(3);

            options.Server = leader;
            options.ReplicationFactor = nodes.Count;
            using var store = InternalGetDocumentStore(options);

            var entity = new TestObj();
            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(entity);
                await session.SaveChangesAsync();
            }

            Dictionary<string, CompareExchangeValue<TestObj>> result = null;
            await AssertWaitForValueAsync(async () =>
            {
                result = await store.Operations.SendAsync(new GetCompareExchangeValuesOperation<TestObj>(""));
                return result.Count;
            }, 1);

            Assert.EndsWith(entity.Id, result.Single().Key, StringComparison.OrdinalIgnoreCase);
        }

        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ClusterWideTransaction_WhenDisableAndStore_ShouldNotCreateCompareExchange(Options options)
        {
            var (nodes, leader) = await CreateRaftCluster(3);
            options.Server = leader;
            options.ReplicationFactor = nodes.Count;
            using var store = InternalGetDocumentStore(options);

            var entity = new TestObj();
            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide, DisableAtomicDocumentWritesInClusterWideTransaction = true }))
            {
                await session.StoreAsync(entity);
                await session.SaveChangesAsync();
            }

            var result = await store.Operations.SendAsync(new GetCompareExchangeValuesOperation<TestObj>(""));
            Assert.Empty(result);
        }

        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ClusterWideTransaction_WhenLoadAndUpdateInParallel_ShouldSucceedOnlyInTheFirst(Options options)
        {
            var (nodes, leader) = await CreateRaftCluster(3);
            options.Server = leader;
            options.ReplicationFactor = nodes.Count;
            using var documentStore = InternalGetDocumentStore(options);

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
                    using var session = stores[i].OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide });
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
                Assert.IsType<ClusterTransactionConcurrencyException>(exception);
            }
        }

        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ClusterWideTransaction_WhenLoadAndDeleteWhileUpdated_ShouldFailDeletion(Options options)
        {
            var (nodes, leader) = await CreateRaftCluster(3, shouldRunInMemory: false);
            options.Server = leader;
            options.ReplicationFactor = nodes.Count;
            using var documentStore = InternalGetDocumentStore(options);

            var entity = new TestObj();
            using (var session = documentStore.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(entity);
                await session.SaveChangesAsync();
            }

            await LoadAndDeleteWhileUpdated(nodes, documentStore.Database, entity.Id);
        }

        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ClusterWideTransaction_WhenImportThenLoadAndDeleteWhileUpdated_ShouldFailDeletion(Options options)
        {
            var (nodes, leader) = await CreateRaftCluster(3);
            options.Server = leader;
            options.ReplicationFactor = nodes.Count;
            using var documentStore = InternalGetDocumentStore(options);

            var entity = new TestObj();
            using (var source = InternalGetDocumentStore())
            {
                using var session = source.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide });
                await session.StoreAsync(entity);
                await session.SaveChangesAsync();

                var operation = await source.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), documentStore.Smuggler);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            }

            await LoadAndDeleteWhileUpdated(nodes, documentStore.Database, entity.Id);
        }

        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanRestoreAfterRecreation(Options options)
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var count = 1;
            List<SmugglerResult> importResults = new();

            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);
            options.Server = leader;
            options.ReplicationFactor = nodes.Count;
            using var documentStore = InternalGetDocumentStore(options);

            var notDelete = $"TestObjs/{count}";
            using (var source = InternalGetDocumentStore())
            {
                var config = new PeriodicBackupConfiguration { LocalSettings = new LocalSettings { FolderPath = backupPath }, IncrementalBackupFrequency = "0 0 */12 * *" };
                var backupTaskId = (await source.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                Backup.WaitForResponsibleNodeUpdate(Server.ServerStore, source.Database, backupTaskId);

                using (var session = source.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    for (int i = 0; i < count; i++)
                    {
                        await session.StoreAsync(new TestObj(), $"TestObjs/{i}");
                    }
                    await session.SaveChangesAsync();
                }

                var backupStatus = await source.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                await backupStatus.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                using (var session = source.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.MaxNumberOfRequestsPerSession += count;
                    for (int i = 0; i < count; i++)
                    {
                        session.Delete($"TestObjs/{i}");
                    }

                    await session.SaveChangesAsync();
                }

                var backupStatus2 = await source.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                await backupStatus2.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                using (var session = source.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    for (int i = 0; i < count; i++)
                    {
                        await session.StoreAsync(new TestObj(), $"TestObjs/{i}");
                    }
                    await session.StoreAsync(new TestObj(), notDelete);
                    await session.SaveChangesAsync();
                }

                var backupStatus3 = await source.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                await backupStatus3.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                var files = await Backup.GetBackupFilesAndAssertCountAsync(backupPath, 3, backupStatus3.Id, source.Database);

                var smugglerOptions = new DatabaseSmugglerImportOptions();
                DatabaseSmuggler.ConfigureOptionsForIncrementalImport(smugglerOptions);

                foreach (var file in files)
                {
                    var op = await documentStore.Smuggler.ImportAsync(smugglerOptions, file);
                    var result = await op.WaitForCompletionAsync();
                    importResults.Add(result as SmugglerResult);
                }
            }

            // await AssertClusterWaitForNotNull(nodes, documentStore.Database, async s =>
            // {
            //     using var session = s.OpenAsyncSession();
            //     return await session.LoadAsync<TestObj>(notDelete);
            // });

            //Additional information for investigating RavenDB-17823 
            //Additional information for investigating RavenDB-16884
            async Task<string> AddDebugInfoToErrorMessage(IDocumentStore store)
            {
                var sb = new StringBuilder()
                    .AppendLine("failed on ClusterWaitForNotNull");

                var results = await ClusterWaitFor(nodes, store.Database, async s =>
                {
                    using var session = s.OpenAsyncSession();
                    return (await session.LoadAsync<TestObj>(notDelete), await session.Query<TestObj>().CountAsync());
                });

                foreach (var tuple in results)
                {
                    sb.AppendLine($"is {notDelete} null:{tuple.Item1 == null}, actual count {tuple.Item2}, expected {count + 1}");
                }

                var compareExchangeItems = await store.Operations.SendAsync(new GetCompareExchangeValuesOperation<TestObj>(""));
                sb.AppendLine($"CompareExchange count : {compareExchangeItems.Count}");

                sb.AppendLine("import results :");
                for (int i = 0; i < importResults.Count; i++)
                {
                    sb.AppendLine($"file #{i + 1}");
                    var result = importResults[i];
                    if (result == null)
                        continue;
                    sb.AppendLine(JsonConvert.SerializeObject(result));
                }

                return sb.ToString();
            }

            var waitResults = await ClusterWaitForNotNull(nodes, documentStore.Database, async s =>
            {
                using var session = s.OpenAsyncSession();
                return await session.LoadAsync<TestObj>(notDelete);
            });

            var nullCount = waitResults.Count(r => r == null);
            Assert.True(nullCount == 0, await AddDebugInfoToErrorMessage(documentStore));

            await AssertWaitForCountAsync(async () => await documentStore.Operations.SendAsync(new GetCompareExchangeValuesOperation<TestObj>("")), count + 1);
        }

        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [RavenData(1, true, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(1, false, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(2 * 1024, true, DatabaseMode = RavenDatabaseMode.All)] // DatabaseDestination.DatabaseCompareExchangeActions.BatchSize
        public async Task ClusterWideTransaction_WhenRestoreFromIncrementalBackupAfterStoreAndDelete_ShouldDeleteInTheDestination(Options options, int count, bool withLoad)
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");

            var (nodes, leader) = await CreateRaftCluster(3);
            options.Server = leader;
            options.ReplicationFactor = nodes.Count;
            using var documentStore = InternalGetDocumentStore(options);

            var notDelete = $"TestObjs/{count}";
            using (var source = InternalGetDocumentStore())
            {
                var config = new PeriodicBackupConfiguration { LocalSettings = new LocalSettings { FolderPath = backupPath }, IncrementalBackupFrequency = "0 0 */12 * *" };
                var backupTaskId = (await source.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                Backup.WaitForResponsibleNodeUpdate(Server.ServerStore, source.Database, backupTaskId);

                using (var session = source.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    for (int i = 0; i < count; i++)
                    {
                        await session.StoreAsync(new TestObj(), $"TestObjs/{i}");
                    }
                    await session.StoreAsync(new TestObj(), notDelete);
                    await session.SaveChangesAsync();
                }

                var backupStatus = await source.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                await backupStatus.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                using (var session = source.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.MaxNumberOfRequestsPerSession += count;
                    for (int i = 0; i < count; i++)
                    {
                        if (withLoad)
                            await session.LoadAsync<TestObj>($"TestObjs/{i}");
                        session.Delete($"TestObjs/{i}");
                    }

                    await session.SaveChangesAsync();
                }

                var backupStatus2 = await source.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                await backupStatus2.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                await Backup.GetBackupFilesAndAssertCountAsync(backupPath, 2, backupStatus2.Id, source.Database);
                
                await documentStore.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(), Directory.GetDirectories(backupPath).First());
            }

            await AssertClusterWaitForNotNull(nodes, documentStore.Database, async s =>
            {
                using var session = s.OpenAsyncSession();
                return await session.LoadAsync<TestObj>(notDelete);
            });
            var r = await WaitForSingleAsync(async () => await documentStore.Operations.SendAsync(new GetCompareExchangeValuesOperation<TestObj>("")));
            Assert.True(r.Count == 1, AddDebugInfoAsync(backupPath, r.Count).Result);
            Assert.EndsWith(notDelete, r.Single().Key, StringComparison.OrdinalIgnoreCase);
        }

        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [RavenData(1, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(2 * 1024, DatabaseMode = RavenDatabaseMode.All)]// DatabaseDestination.DatabaseCompareExchangeActions.BatchSize
        public async Task ClusterWideTransaction_WhenRestoreFromIncrementalBackupAfterStoreAndUpdate_ShouldCompleteImportWithNoException(Options options, int count)
        {
            const string modified = "Modified";
            var backupPath = NewDataPath(suffix: "BackupFolder");

            var (nodes, leader) = await CreateRaftCluster(3);
            options.Server = leader;
            options.ReplicationFactor = nodes.Count;
            using var documentStore = InternalGetDocumentStore(options);

            var notToModify = $"TestObjs/{count}";
            using (var source = InternalGetDocumentStore())
            {
                var config = new PeriodicBackupConfiguration { LocalSettings = new LocalSettings { FolderPath = backupPath }, IncrementalBackupFrequency = "0 0 */12 * *" };
                var backupTaskId = (await source.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                Backup.WaitForResponsibleNodeUpdate(Server.ServerStore, source.Database, backupTaskId);

                using (var session = source.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    for (int i = 0; i < count; i++)
                    {
                        await session.StoreAsync(new TestObj(), $"TestObjs/{i}");
                    }
                    await session.StoreAsync(new TestObj(), notToModify);
                    await session.SaveChangesAsync();
                }

                var backupStatus = await source.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                await backupStatus.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

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
                await backupStatus2.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                await Backup.GetBackupFilesAndAssertCountAsync(backupPath, 2, backupStatus2.Id, source.Database);
                
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

        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ClusterWideTransaction_WhenRestoreFromIncrementalBackupAfterStoreAndUpdateWithoutLoad_ShouldFail(Options options)
        {
            const string docId = "TestObjs/1";
            using var source = InternalGetDocumentStore(options);
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

        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ClusterWideTransaction_WhenLoadAndUpdateWhileDeleted_ShouldFailUpdate(Options options)
        {
            var (nodes, leader) = await CreateRaftCluster(3, shouldRunInMemory: false);
            options.Server = leader;
            options.ReplicationFactor = nodes.Count;
            using var documentStore = InternalGetDocumentStore(options);

            var entity = new TestObj();
            using (var session = documentStore.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(entity);
                await session.SaveChangesAsync();
            }

            await LoadAndUpdateWhileDeleted(nodes, documentStore.Database, entity.Id);
        }

        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ClusterWideTransaction_WhenImportThenLoadAndUpdateWhileDeleted_ShouldFailUpdate(Options options)
        {
            var (nodes, leader) = await CreateRaftCluster(3);
            options.Server = leader;
            options.ReplicationFactor = nodes.Count;
            using var documentStore = InternalGetDocumentStore(options);

            var entity = new TestObj();
            using (var source = InternalGetDocumentStore())
            {
                using var session = source.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide });
                await session.StoreAsync(entity);
                await session.SaveChangesAsync();

                var operation = await source.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), documentStore.Smuggler);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            }

            await LoadAndUpdateWhileDeleted(nodes, documentStore.Database, entity.Id);
        }

        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ClusterWideTransaction_WhenSetExpirationAndExport_ShouldDeleteTheCompareExchangeAsWell(Options options)
        {
            var customSettings = new Dictionary<string, string> { [RavenConfiguration.GetKey(x => x.Cluster.CompareExchangeExpiredCleanupInterval)] = "1" };
            using var server = GetNewServer(new ServerCreationOptions { CustomSettings = customSettings, });

            using var source = InternalGetDocumentStore(options);
            options.Server = server;
            using var dest = InternalGetDocumentStore(options);
            await dest.Maintenance.SendAsync(new ConfigureExpirationOperation(new ExpirationConfiguration
            {
                Disabled = false,
                DeleteFrequencyInSec = 1
            }));

            const string id = "testObjs/0";
            using (var session = source.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = new TestObj();
                await session.StoreAsync(entity, id);

                var expires = SystemTime.UtcNow.AddMinutes(-5);
                session.Advanced.GetMetadataFor(entity)[Constants.Documents.Metadata.Expires] = expires.GetDefaultRavenFormat(isUtc: true);
                await session.SaveChangesAsync();
            }

            var operation = await source.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), dest.Smuggler);
            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            await AssertWaitForNullAsync(async () =>
            {
                using var session = dest.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide });
                return await session.LoadAsync<TestObj>(id);
            });

            await AssertWaitForTrueAsync(async () =>
            {
                var compareExchangeValues = await dest.Operations.SendAsync(new GetCompareExchangeValuesOperation<object>(""));
                return compareExchangeValues.Any() == false;
            });
        }

        private static IEnumerable<object[]> GetMetadataStaticFields()
        {
            return typeof(Constants.Documents.Metadata)
                .GetFields(System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.Public)
                .Select(p => p.GetValue(null).ToString())
                .Distinct()
                .SelectMany(s =>
                {
                    var builder = new StringBuilder(s);
                    //Just replacing one char in the end
                    builder[^1] = builder[^1] == 'a' ? 'b' : 'a';
                    return new[] { new object[] { builder.ToString() } };
                });
        }

        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [RavenMemberData(nameof(GetMetadataStaticFields), DatabaseMode = RavenDatabaseMode.All)]
        public async Task StoreDocument_WheHasUserMetadataPropertyWithLengthEqualsToInternalRavenDbMetadataPropertyLength(Options options, string metadataPropNameToTest)
        {
            const string id = "id1";
            const string value = "Value";

            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var executor = store.GetRequestExecutor();
                    using var dis = executor.ContextPool.AllocateOperationContext(out var context);
                    var p = context.ReadObject(new DynamicJsonValue
                    {
                        [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                        {
                            [metadataPropNameToTest] = value

                        }
                    }, $"{nameof(metadataPropNameToTest)} {metadataPropNameToTest}");
                    await session.StoreAsync(p, null, id);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var entity = await session.LoadAsync<DynamicJsonValue>(id);
                    var metadata = session.Advanced.GetMetadataFor(entity);
                    Assert.Equal(value, metadata[metadataPropNameToTest]);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ClusterWideTransaction_WhenSetExpiration_ShouldDeleteTheCompareExchangeAsWell(Options options)
        {
            var customSettings = new Dictionary<string, string> { [RavenConfiguration.GetKey(x => x.Cluster.CompareExchangeExpiredCleanupInterval)] = "1" };
            using var server = GetNewServer(new ServerCreationOptions { CustomSettings = customSettings, });
            options.Server = server;
            using var store = InternalGetDocumentStore(options);
            await store.Maintenance.SendAsync(new ConfigureExpirationOperation(new ExpirationConfiguration
            {
                Disabled = false,
                DeleteFrequencyInSec = 10
            }));

            const string id = "testObjs/0";
            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = new TestObj();
                await session.StoreAsync(entity, id);

                var expires = SystemTime.UtcNow.AddMinutes(-5);
                session.Advanced.GetMetadataFor(entity)[Constants.Documents.Metadata.Expires] = expires.GetDefaultRavenFormat(isUtc: true);
                await session.SaveChangesAsync();
            }

            await AssertWaitForNullAsync(async () =>
            {
                using var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide });
                return await session.LoadAsync<TestObj>(id);
            });

            await AssertWaitForTrueAsync(async () =>
            {
                var compareExchangeValues = await store.Operations.SendAsync(new GetCompareExchangeValuesOperation<object>(""));
                return compareExchangeValues.Any() == false;
            });
        }

        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ClusterWideTransaction_WhenDocumentRemovedByExpiration_ShouldAllowToCreateNewDocumentEvenIfItsCompareExchangeWasntRemoved(Options options)
        {
            using var store = InternalGetDocumentStore(options);
            await store.Maintenance.SendAsync(new ConfigureExpirationOperation(new ExpirationConfiguration { Disabled = false, DeleteFrequencyInSec = 1 }));

            const string id = "testObjs/0";
            for (int i = 0; i < 5; i++)
            {
                await AssertWaitForNullAsync(async () =>
                {
                    using var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide });
                    return await session.LoadAsync<TestObj>(id);
                });

                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var entity = new TestObj();
                    await session.StoreAsync(entity, id);

                    var expires = SystemTime.UtcNow.AddMinutes(-5);
                    session.Advanced.GetMetadataFor(entity)[Constants.Documents.Metadata.Expires] = expires.GetDefaultRavenFormat(isUtc: true);

                    await session.SaveChangesAsync();
                }
            }
        }

        private static async Task<string> AddDebugInfoAsync(string backupPath, int count)
        {
            var sb = new StringBuilder();
            sb.AppendLine($"Expected to have a single compare exchange value after restore from incremental, but got {count}");

            var dir = Directory.GetDirectories(backupPath).First();
            var files = Directory.GetFiles(dir)
                .Where(BackupUtils.IsBackupFile)
                .OrderBackups();

            foreach (var file in files)
            {
                sb.AppendLine($"backup file {Path.GetFileName(file)} :");
                using (var inputStream = File.Open(file, FileMode.Open))
                using (var stream = await Raven.Server.Utils.BackupUtils.GetDecompressionStreamAsync(inputStream))
                {
                    var text = stream.ReadStringWithoutPrefix();
                    sb.AppendLine(JsonConvert.SerializeObject(text)).AppendLine();
                }
            }

            return sb.ToString();
        }
    }
}
