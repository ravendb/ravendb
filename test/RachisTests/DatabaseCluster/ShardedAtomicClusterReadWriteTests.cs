using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
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

        [Fact(Skip = "Should complete shard implementation")]
        public async Task ClusterWideTransaction_WhenStore_ShouldCreateCompareExchange()
        {
            await base.ClusterWideTransaction_WhenStore_ShouldCreateCompareExchange();
        }

        [Fact(Skip = "Should complete shard implementation")]
        public async Task ClusterWideTransaction_WhenDisableAndStore_ShouldNotCreateCompareExchange()
        {
            await base.ClusterWideTransaction_WhenDisableAndStore_ShouldNotCreateCompareExchange();
        }

        [Fact]
        public async Task ClusterWideTransaction_WhenLoadAndUpdateInParallel_ShouldSucceedOnlyInTheFirst()
        {
            await base.ClusterWideTransaction_WhenLoadAndUpdateInParallel_ShouldSucceedOnlyInTheFirst();
        }

        [Fact]
        public async Task ClusterWideTransaction_WhenLoadAndDeleteWhileUpdated_ShouldFailDeletion()
        {
            await base.ClusterWideTransaction_WhenLoadAndDeleteWhileUpdated_ShouldFailDeletion();
        }

        [Fact(Skip = "Should complete shard implementation")]
        public async Task ClusterWideTransaction_WhenImportThenLoadAndDeleteWhileUpdated_ShouldFailDeletion()
        {
            await base.ClusterWideTransaction_WhenImportThenLoadAndDeleteWhileUpdated_ShouldFailDeletion();
        }

        [Fact(Skip = "Should complete shard implementation")]
        public async Task CanRestoreAfterRecreation()
        {
            await base.CanRestoreAfterRecreation();
        }

        [Theory(Skip = "Should complete shard implementation")]
        [InlineData(1)]
        [InlineData(1, false)]
        [InlineData(2 * 1024)]// DatabaseDestination.DatabaseCompareExchangeActions.BatchSize
        public async Task ClusterWideTransaction_WhenRestoreFromIncrementalBackupAfterStoreAndDelete_ShouldDeleteInTheDestination(int count, bool withLoad = true)
        {
            await base.ClusterWideTransaction_WhenRestoreFromIncrementalBackupAfterStoreAndDelete_ShouldDeleteInTheDestination(count, withLoad);
        }

        [Theory(Skip = "Should complete shard implementation")]
        [InlineData(1)]
        [InlineData(2 * 1024)]// DatabaseDestination.DatabaseCompareExchangeActions.BatchSize
        public async Task ClusterWideTransaction_WhenRestoreFromIncrementalBackupAfterStoreAndUpdate_ShouldCompleteImportWithNoException(int count)
        {
            await base.ClusterWideTransaction_WhenRestoreFromIncrementalBackupAfterStoreAndUpdate_ShouldCompleteImportWithNoException(count);
        }

        [Theory(Skip = "Should complete shard implementation")]
        public async Task ClusterWideTransaction_WhenRestoreFromIncrementalBackupAfterStoreAndUpdateWithoutLoad_ShouldFail()
        {
            await base.ClusterWideTransaction_WhenRestoreFromIncrementalBackupAfterStoreAndUpdateWithoutLoad_ShouldFail();
        }

        [Fact]
        public async Task ClusterWideTransaction_WhenLoadAndUpdateWhileDeleted_ShouldFailUpdate()
        {
            await base.ClusterWideTransaction_WhenLoadAndUpdateWhileDeleted_ShouldFailUpdate();
        }

        [Fact(Skip = "Should complete shard implementation")]
        public async Task ClusterWideTransaction_WhenImportThenLoadAndUpdateWhileDeleted_ShouldFailUpdate()
        {
            await base.ClusterWideTransaction_WhenImportThenLoadAndUpdateWhileDeleted_ShouldFailUpdate();
        }
    }
}
