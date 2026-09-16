using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;
using static InterversionTests.Revisions.RevisionsInterversionHelpers;

namespace InterversionTests.Revisions
{
    // Cross-version entity-preservation suite: pre-PR data must remain readable on current bits across
    // every supported old-to-new transition mechanism -- restore-from-backup, restore-from-snapshot, and
    // in-place binary upgrade. Each test seeds the same SeedStandardAsync cascade on the old node and
    // asserts via EntityAssertions on the current-bits side.
    public class OldToNewEntityPreservationTests : OldDataFixture
    {
        public OldToNewEntityPreservationTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.BackupExportImport | RavenTestCategory.Interversion, RavenPlatform.Windows | RavenPlatform.Linux)]
        public async Task BackupRestore_PreservesAllFourEntities()
        {
            var seedDb = GetDatabaseName() + "-seed";
            var state = await SeedStandardAsync(Versions.PrePRv62, seedDb, docCount: 2, produceBackup: true);
            Assert.NotNull(state.BackupFolderPath);

            KillSlavedServerProcess(state.Node.Process);

            using var hostStore = GetCurrentBitsStore();
            var restoreDb = GetDatabaseName() + "-restored";
            using var _ = Databases.EnsureDatabaseDeletion(restoreDb, hostStore);
            using var restoredStore = await RestoreAsync(hostStore, restoreDb, state.BackupFolderPath);

            await EntityAssertions.AssertClientSurvivingRevisionsAsync(
                restoredStore, state, expectedSurvivingRevisions: 2, label: "post-restore client");
            await EntityAssertions.AssertStrictTombstonesAsync(
                Server, restoreDb, state, label: "post-restore strict");
        }

        // Snapshot restore is byte-for-byte at the Voron page level; legacy L-shape rows land as-is and
        // the schema upgrade runs at DB open. Also pins the HashedRevisionPk feature token stays false
        // on the restored DB so the raw-form fallback remains active for legacy rows.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.BackupExportImport | RavenTestCategory.Interversion, RavenPlatform.Windows | RavenPlatform.Linux)]
        public async Task SnapshotRestore_PreservesEntitiesAndLeavesHashedRevisionPkFalse()
        {
            var seedDb = GetDatabaseName() + "-seed";
            OldDatabaseState state = await SeedStandardAsync(Versions.PrePRv62, seedDb, docCount: 2, produceSnapshot: true);

            Assert.NotNull(state.SnapshotFilePath);
            Assert.True(File.Exists(state.SnapshotFilePath), $"Snapshot file '{state.SnapshotFilePath}' must exist after seed.");

            KillSlavedServerProcess(state.Node.Process);

            using var hostStore = GetCurrentBitsStore();
            var restoreDb = GetDatabaseName() + "-restored";
            string snapshotFolder = Path.GetDirectoryName(state.SnapshotFilePath);

            using var _ = Databases.EnsureDatabaseDeletion(restoreDb, hostStore);
            using var restoredStore = await RestoreAsync(hostStore, restoreDb, snapshotFolder);

            Raven.Server.Documents.DocumentDatabase restoredDatabase =
                await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(restoreDb);

            Assert.False(restoredDatabase.SupportedFeatures.SupportedFeatureTypes.HashedRevisionPk,
                "v6.2.8 snapshot must NOT carry the HashedRevisionPk token; detector must stay false on restored DB.");

            await EntityAssertions.AssertClientSurvivingRevisionsAsync(
                restoredStore, state, expectedSurvivingRevisions: 2, label: "post-restore client");
            await EntityAssertions.AssertStrictTombstonesAsync(
                Server, restoreDb, state, label: "post-restore strict");
        }

        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Interversion, RavenPlatform.Windows | RavenPlatform.Linux)]
        public async Task InPlaceUpgrade_PreservesAllFourEntities()
        {
            var dbName = GetDatabaseName();

            var state = await SeedStandardAsync(Versions.PrePRv62, dbName, docCount: 2);

            await UpgradeServerAsync(toVersion: "current", state.Node);

            using var store = new DocumentStore
            {
                Urls = new[] { state.Node.Url },
                Database = dbName
            };
            store.Initialize();

            await EntityAssertions.AssertClientSurvivingRevisionsAsync(
                store, state, expectedSurvivingRevisions: 2, label: "post-upgrade client");

            var server = Servers.Single(s => s.WebUrl == state.Node.Url);
            await EntityAssertions.AssertStrictTombstonesAsync(
                server, dbName, state, label: "post-upgrade strict");
        }
    }
}
