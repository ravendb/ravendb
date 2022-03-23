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
