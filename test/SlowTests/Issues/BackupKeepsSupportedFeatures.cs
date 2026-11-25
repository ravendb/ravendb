using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Exceptions;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Server.Replication;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    // Each database feature after a real backup and restore. ThrowRevisionKeyTooBigFix has no test: nothing in this build reads it.
    public class BackupKeepsSupportedFeatures : RavenTestBase
    {
        private const string IdWithControlCharacter = "users/1\u0001";

        public BackupKeepsSupportedFeatures(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.BackupExportImport)]
        [RavenData(BackupType.Backup, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(BackupType.Snapshot, DatabaseMode = RavenDatabaseMode.Single)] // sharded databases have no snapshot backups
        public async Task ThrowControlCharactersInIdentifier_RestoredDatabaseRejectsControlCharacterIds(Options options, BackupType backupType)
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using (var store = GetDocumentStore(options))
            {
                // A new database rejects control characters in document IDs.
                await Assert.ThrowsAnyAsync<RavenException>(() => StoreUserAsync(store, store.Database, IdWithControlCharacter));

                await StoreUserAsync(store, store.Database, "users/1");
                var config = Backup.CreateBackupConfiguration(backupPath, backupType);
                var taskId = await Backup.RunBackupForDatabaseModeAsync(Server, config, store, options.DatabaseMode);

                // Followed by an incremental file: for a snapshot, that file's record is the one restore keeps.
                await StoreUserAsync(store, store.Database, "users/2");
                var restoreConfig = await Backup.RunBackupAndCreateRestoreConfigurationAsync(store, options.DatabaseMode, taskId, backupPath);

                using (Backup.RestoreDatabase(store, restoreConfig))
                {
                    await Assert.ThrowsAnyAsync<RavenException>(() => StoreUserAsync(store, restoreConfig.DatabaseName, IdWithControlCharacter));
                }
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Replication)]
        public async Task PullReplicationCompositeChangeVectors_RestoredHubSendsCompositeChangeVectors()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var storeOptions = new Options
            {
                ClientCertificate = certificates.ServerCertificateForCommunication.Value,
                AdminCertificate = certificates.ServerCertificateForCommunication.Value
            };
            using var hub = GetDocumentStore(storeOptions);
            using var sink = GetDocumentStore(storeOptions);
            using var pullCert = FilteredReplicationTestsPullReplicationCompositeChangeVectors.ExportCertificate(certificates.ClientCertificate1.Value);

            var backupPath = NewDataPath(suffix: "BackupFolder");
            var config = Backup.CreateBackupConfiguration(backupPath);
            var taskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, hub);
            var restoreConfig = await Backup.RunBackupAndCreateRestoreConfigurationAsync(hub, RavenDatabaseMode.Single, taskId, backupPath);

            using (Backup.RestoreDatabase(hub, restoreConfig))
            using (var restoredHub = GetDocumentStore(new Options
                   {
                       CreateDatabase = false,
                       ModifyDatabaseName = _ => restoreConfig.DatabaseName,
                       ClientCertificate = certificates.ServerCertificateForCommunication.Value,
                       AdminCertificate = certificates.ServerCertificateForCommunication.Value
                   }))
            {
                await FilteredReplicationTestsPullReplicationCompositeChangeVectors.SetupFilteredHubToSinkPullReplicationAsync(restoredHub, sink, pullCert);

                await StoreUserAsync(restoredHub, restoredHub.Database, "items/include/1");
                Assert.True(WaitForDocument<User>(sink, "items/include/1", predicate: null, timeout: 30_000));

                // The legacy form carries only the version part; the composite form also carries the order part after a '|'.
                using (var session = sink.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("items/include/1");
                    Assert.Contains("|", session.Advanced.GetChangeVectorFor(user));
                }
            }
        }

        [RavenTheory(RavenTestCategory.BackupExportImport | RavenTestCategory.Revisions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task HashedRevisionPk_RegularRestore_StartsNoRevisionsKeyCleanup(Options options)
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using (var store = GetDocumentStore(options))
            {
                await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration()
                }));
                await StoreUserAsync(store, store.Database, "users/1", "first");
                await StoreUserAsync(store, store.Database, "users/1", "second");

                var config = Backup.CreateBackupConfiguration(backupPath);
                var taskId = await Backup.RunBackupForDatabaseModeAsync(Server, config, store, options.DatabaseMode);
                var restoreConfig = await Backup.RunBackupAndCreateRestoreConfigurationAsync(store, options.DatabaseMode, taskId, backupPath);

                using (Backup.RestoreDatabase(store, restoreConfig))
                {
                    // The cleanup sets the flag only in its last batch and leaves its state behind, so one of the two checks fails once it has started.
                    foreach (var database in await GetDatabasesAsync(restoreConfig.DatabaseName, options.DatabaseMode))
                    {
                        Assert.True(database.DocumentsStorage.RevisionsStorage.HashedRevisionPk, $"{database.Name}: a restored storage holds only hashed revision rows");

                        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            Assert.False(CleanupState.TryRead(context.Transaction.InnerTransaction, out _, out _), $"{database.Name}: the revisions key cleanup never ran");
                        }
                    }

                    using (var session = store.OpenAsyncSession(restoreConfig.DatabaseName))
                    {
                        Assert.Equal(2, await session.Advanced.Revisions.GetCountForAsync("users/1"));
                    }
                }
            }
        }

        private async Task<List<DocumentDatabase>> GetDatabasesAsync(string database, RavenDatabaseMode mode)
        {
            if (mode == RavenDatabaseMode.Single)
                return [await GetDatabase(database)];

            var shards = new List<DocumentDatabase>();
            await foreach (var shard in Sharding.GetShardsDocumentDatabaseInstancesFor(database))
                shards.Add(shard);
            return shards;
        }

        private static async Task StoreUserAsync(IDocumentStore store, string database, string id, string name = "Name")
        {
            using (var session = store.OpenAsyncSession(database))
            {
                await session.StoreAsync(new User { Name = name }, id);
                await session.SaveChangesAsync();
            }
        }
    }
}
