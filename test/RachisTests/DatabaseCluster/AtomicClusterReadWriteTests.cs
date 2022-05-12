using System.Threading.Tasks;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace RachisTests.DatabaseCluster
{
    public class AtomicClusterReadWriteTests : AtomicClusterReadWriteTestsBase
    {
        public AtomicClusterReadWriteTests(ITestOutputHelper output) : base(output)
        {
        }

        protected override IDocumentStore InternalGetDocumentStore(Options options = null, string caller = null)
        {
            return GetDocumentStore(options, caller);
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task ClusterWideTransaction_WhenStore_ShouldCreateCompareExchange()
        {
            await base.ClusterWideTransaction_WhenStore_ShouldCreateCompareExchange();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task ClusterWideTransaction_WhenDisableAndStore_ShouldNotCreateCompareExchange()
        {
            await base.ClusterWideTransaction_WhenDisableAndStore_ShouldNotCreateCompareExchange();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task ClusterWideTransaction_WhenLoadAndUpdateInParallel_ShouldSucceedOnlyInTheFirst()
        {
            await base.ClusterWideTransaction_WhenLoadAndUpdateInParallel_ShouldSucceedOnlyInTheFirst();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task ClusterWideTransaction_WhenLoadAndDeleteWhileUpdated_ShouldFailDeletion()
        {
            await base.ClusterWideTransaction_WhenLoadAndDeleteWhileUpdated_ShouldFailDeletion();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task ClusterWideTransaction_WhenImportThenLoadAndDeleteWhileUpdated_ShouldFailDeletion()
        {
            await base.ClusterWideTransaction_WhenImportThenLoadAndDeleteWhileUpdated_ShouldFailDeletion();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task CanRestoreAfterRecreation()
        {
            await base.CanRestoreAfterRecreation();
        }

        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [InlineData(1)]
        [InlineData(1, false)]
        [InlineData(2 * 1024)]// DatabaseDestination.DatabaseCompareExchangeActions.BatchSize
        public override async Task ClusterWideTransaction_WhenRestoreFromIncrementalBackupAfterStoreAndDelete_ShouldDeleteInTheDestination(int count, bool withLoad = true)
        {
            await base.ClusterWideTransaction_WhenRestoreFromIncrementalBackupAfterStoreAndDelete_ShouldDeleteInTheDestination(count, withLoad);
        }

        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [InlineData(1)]
        [InlineData(2 * 1024)]// DatabaseDestination.DatabaseCompareExchangeActions.BatchSize

        public override async Task ClusterWideTransaction_WhenRestoreFromIncrementalBackupAfterStoreAndUpdate_ShouldCompleteImportWithNoException(int count)
        {
            await base.ClusterWideTransaction_WhenRestoreFromIncrementalBackupAfterStoreAndUpdate_ShouldCompleteImportWithNoException(count);
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task ClusterWideTransaction_WhenRestoreFromIncrementalBackupAfterStoreAndUpdateWithoutLoad_ShouldFail()
        {
            await base.ClusterWideTransaction_WhenRestoreFromIncrementalBackupAfterStoreAndUpdateWithoutLoad_ShouldFail();
        }
        
        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task ClusterWideTransaction_WhenLoadAndUpdateWhileDeleted_ShouldFailUpdate()
        {
            await base.ClusterWideTransaction_WhenLoadAndUpdateWhileDeleted_ShouldFailUpdate();
        }
        
        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task ClusterWideTransaction_WhenImportThenLoadAndUpdateWhileDeleted_ShouldFailUpdate()
        {
            await base.ClusterWideTransaction_WhenImportThenLoadAndUpdateWhileDeleted_ShouldFailUpdate();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task ClusterWideTransaction_WhenSetExpirationAndExport_ShouldDeleteTheCompareExchangeAsWell()
        {
            await base.ClusterWideTransaction_WhenSetExpirationAndExport_ShouldDeleteTheCompareExchangeAsWell();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task ClusterWideTransaction_WhenSetExpiration_ShouldDeleteTheCompareExchangeAsWell()
        {
            await base.ClusterWideTransaction_WhenSetExpiration_ShouldDeleteTheCompareExchangeAsWell();
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public override async Task ClusterWideTransaction_WhenDocumentRemovedByExpiration_ShouldAllowToCreateNewDocumentEvenIfItsCompareExchangeWasntRemoved()
        {
            await base.ClusterWideTransaction_WhenDocumentRemovedByExpiration_ShouldAllowToCreateNewDocumentEvenIfItsCompareExchangeWasntRemoved();
        }
    }
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

        [Fact]
        public async Task ClusterWideTransaction_WhenSetExpirationAndExport_ShouldDeleteTheCompareExchangeAsWell()
        {
            var customSettings = new Dictionary<string, string> {[RavenConfiguration.GetKey(x => x.Cluster.CompareExchangeExpiredCleanupInterval)] = "1"};
            using var server = GetNewServer(new ServerCreationOptions {CustomSettings = customSettings,});

            using var source = GetDocumentStore();
            using var dest = GetDocumentStore(new Options {Server = server});
            await dest.Maintenance.SendAsync(new ConfigureExpirationOperation(new ExpirationConfiguration
            {
                Disabled = false,
                DeleteFrequencyInSec = 1
            }));
            
            const string id = "testObjs/0";
            using (var session = source.OpenAsyncSession(new SessionOptions {TransactionMode = TransactionMode.ClusterWide}))
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

        [Fact]
        public async Task ClusterWideTransaction_WhenSetExpiration_ShouldDeleteTheCompareExchangeAsWell()
        {
            var customSettings = new Dictionary<string, string> {[RavenConfiguration.GetKey(x => x.Cluster.CompareExchangeExpiredCleanupInterval)] = "1"};
            using var server = GetNewServer(new ServerCreationOptions {CustomSettings = customSettings,});
            using var store = GetDocumentStore(new Options{Server = server});
            await store.Maintenance.SendAsync(new ConfigureExpirationOperation(new ExpirationConfiguration
            {
                Disabled = false,
                DeleteFrequencyInSec = 10
            }));
            
            const string id = "testObjs/0";
            using (var session = store.OpenAsyncSession(new SessionOptions {TransactionMode = TransactionMode.ClusterWide}))
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

        [Fact]
        public async Task ClusterWideTransaction_WhenDocumentRemovedByExpiration_ShouldAllowToCreateNewDocumentEvenIfItsCompareExchangeWasntRemoved()
        {
            using var store = GetDocumentStore();
            await store.Maintenance.SendAsync(new ConfigureExpirationOperation(new ExpirationConfiguration {Disabled = false, DeleteFrequencyInSec = 1}));

            const string id = "testObjs/0";
            for (int i = 0; i < 5; i++)
            {
                await AssertWaitForNullAsync(async () =>
                {
                    using var session = store.OpenAsyncSession(new SessionOptions {TransactionMode = TransactionMode.ClusterWide});
                    return await session.LoadAsync<TestObj>(id);
                });
                
                using (var session = store.OpenAsyncSession(new SessionOptions {TransactionMode = TransactionMode.ClusterWide}))
                {
                    var entity = new TestObj();
                    await session.StoreAsync(entity, id);

                    var expires = SystemTime.UtcNow.AddMinutes(-5);
                    session.Advanced.GetMetadataFor(entity)[Constants.Documents.Metadata.Expires] = expires.GetDefaultRavenFormat(isUtc: true);
                
                    await session.SaveChangesAsync();
                }
            }
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
                var store = new DocumentStore { Urls = new[] { urls[i] }, Database = database, Conventions = new DocumentConventions { DisableTopologyUpdates = true } }.Initialize();
                stores[i] = store;
            }

            return disposable;
        }
    }
}
