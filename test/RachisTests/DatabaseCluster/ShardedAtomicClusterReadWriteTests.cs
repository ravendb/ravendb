using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace RachisTests.DatabaseCluster
{
    public class ShardedAtomicClusterReadWriteTests : AtomicClusterReadWriteTestsBase
    {
        public ShardedAtomicClusterReadWriteTests(ITestOutputHelper output) : base(output)
        {
        }

        protected override DocumentStore GetDocumentStore(Options options = null, [CallerMemberName] string caller = null)
        {
            options ??= new Options();
            var modifyDatabaseRecord = options.ModifyDatabaseRecord;
            options.ModifyDatabaseRecord = r =>
            {
                modifyDatabaseRecord?.Invoke(r);
                var databaseTopology = r.Topology ?? new DatabaseTopology();
                r.Topology = null;

                r.Shards = Enumerable.Range(0, 3)
                    .Select(_ => databaseTopology)
                    .ToArray();
            };
            return base.GetDocumentStore(options, caller);
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task ClusterWideTransaction_WhenStore_ShouldCreateCompareExchange()
        {
            await base.ClusterWideTransaction_WhenStore_ShouldCreateCompareExchange();
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task ClusterWideTransaction_WhenDisableAndStore_ShouldNotCreateCompareExchange()
        {
            await base.ClusterWideTransaction_WhenDisableAndStore_ShouldNotCreateCompareExchange();
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task ClusterWideTransaction_WhenLoadAndUpdateInParallel_ShouldSucceedOnlyInTheFirst()
        {
            await base.ClusterWideTransaction_WhenLoadAndUpdateInParallel_ShouldSucceedOnlyInTheFirst();
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task ClusterWideTransaction_WhenLoadAndDeleteWhileUpdated_ShouldFailDeletion()
        {
            await base.ClusterWideTransaction_WhenLoadAndDeleteWhileUpdated_ShouldFailDeletion();
        }

        [RavenFact(RavenTestCategory.Sharding, Skip = "Export")]
        public async Task ClusterWideTransaction_WhenImportThenLoadAndDeleteWhileUpdated_ShouldFailDeletion()
        {
            await base.ClusterWideTransaction_WhenImportThenLoadAndDeleteWhileUpdated_ShouldFailDeletion();
        }

        [RavenFact(RavenTestCategory.Sharding, Skip = "Unable to run request http://127.0.0.1:42023/databases/CanRestoreAfterRecreation_5/admin/periodic-backup?raft-request-id=c34c8e14-219b-4ced-a0f0-27c3e358adb9, the database is sharded, but no shared route is defined for this operation!")]
        public async Task CanRestoreAfterRecreation()
        {
            await base.CanRestoreAfterRecreation();
        }

        [RavenTheory(RavenTestCategory.Sharding, Skip = "Unable to run request http://127.0.0.1:42941/databases/ClusterWideTransaction_WhenRestoreFromIncrementalBackupAfterStoreAndDelete_ShouldDeleteInTheDestination_8/admin/periodic-backup?raft-request-id=10032972-255f-4846-b25c-7ac295236c47, the database is sharded, but no shared route is defined for this operation!")]
        [InlineData(1)]
        [InlineData(1, false)]
        [InlineData(2 * 1024)]// DatabaseDestination.DatabaseCompareExchangeActions.BatchSize
        public async Task ClusterWideTransaction_WhenRestoreFromIncrementalBackupAfterStoreAndDelete_ShouldDeleteInTheDestination(int count, bool withLoad = true)
        {
            await base.ClusterWideTransaction_WhenRestoreFromIncrementalBackupAfterStoreAndDelete_ShouldDeleteInTheDestination(count, withLoad);
        }

        [RavenTheory(RavenTestCategory.Sharding, Skip = "Unable to run request http://127.0.0.1:42941/databases/ClusterWideTransaction_WhenRestoreFromIncrementalBackupAfterStoreAndUpdate_ShouldCompleteImportWithNoException_4/admin/periodic-backup?raft-request-id=1de16895-d8c0-4ca3-a636-9e35ad7f87af, the database is sharded, but no shared route is defined for this operation!")]
        [InlineData(1)]
        [InlineData(2 * 1024)]// DatabaseDestination.DatabaseCompareExchangeActions.BatchSize
        public async Task ClusterWideTransaction_WhenRestoreFromIncrementalBackupAfterStoreAndUpdate_ShouldCompleteImportWithNoException(int count)
        {
            await base.ClusterWideTransaction_WhenRestoreFromIncrementalBackupAfterStoreAndUpdate_ShouldCompleteImportWithNoException(count);
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task ClusterWideTransaction_WhenRestoreFromIncrementalBackupAfterStoreAndUpdateWithoutLoad_ShouldFail()
        {
            await base.ClusterWideTransaction_WhenRestoreFromIncrementalBackupAfterStoreAndUpdateWithoutLoad_ShouldFail();
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task ClusterWideTransaction_WhenLoadAndUpdateWhileDeleted_ShouldFailUpdate()
        {
            await base.ClusterWideTransaction_WhenLoadAndUpdateWhileDeleted_ShouldFailUpdate();
        }

        [RavenFact(RavenTestCategory.Sharding, Skip = "Export")]
        public async Task ClusterWideTransaction_WhenImportThenLoadAndUpdateWhileDeleted_ShouldFailUpdate()
        {
            await base.ClusterWideTransaction_WhenImportThenLoadAndUpdateWhileDeleted_ShouldFailUpdate();
        }
        
        [RavenFact(RavenTestCategory.Sharding, Skip = "Unable to run request http://127.0.0.1:41141/databases/ClusterWideTransaction_WhenSetExpirationAndExport_ShouldDeleteTheCompareExchangeAsWell_2/admin/expiration/config?raft-request-id=2c54ba6e-818d-4fe9-9675-7a46f15e8f5f, the database is sharded, but no shared route is defined for this operation!")]
        public async Task ClusterWideTransaction_WhenSetExpirationAndExport_ShouldDeleteTheCompareExchangeAsWell()
        {
            await base.ClusterWideTransaction_WhenSetExpirationAndExport_ShouldDeleteTheCompareExchangeAsWell();
        }

        [RavenFact(RavenTestCategory.Sharding, Skip = "Unable to run request http://127.0.0.1:35179/databases/ClusterWideTransaction_WhenSetExpiration_ShouldDeleteTheCompareExchangeAsWell_5/admin/expiration/config?raft-request-id=a3b127e6-0c3e-45ee-99ac-f48ea52218fd, the database is sharded, but no shared route is defined for this operation!")]
        public async Task ClusterWideTransaction_WhenSetExpiration_ShouldDeleteTheCompareExchangeAsWell()
        {
            await base.ClusterWideTransaction_WhenSetExpiration_ShouldDeleteTheCompareExchangeAsWell();
        }

        [RavenFact(RavenTestCategory.Sharding, Skip = "Unable to run request http://127.0.0.1:36357/databases/ClusterWideTransaction_WhenDocumentRemovedByExpiration_ShouldAllowToCreateNewDocumentEvenIfItsCompareExchangeWasntRemoved_3/admin/expiration/config?raft-request-id=b90b718f-15b1-48f8-b110-66c4c6a85790, the database is sharded, but no shared route is defined for this operation!")]
        public async Task ClusterWideTransaction_WhenDocumentRemovedByExpiration_ShouldAllowToCreateNewDocumentEvenIfItsCompareExchangeWasntRemoved()
        {
            await base.ClusterWideTransaction_WhenDocumentRemovedByExpiration_ShouldAllowToCreateNewDocumentEvenIfItsCompareExchangeWasntRemoved();
        }
    }
}
