using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using static InterversionTests.Revisions.RevisionsInterversionHelpers;

namespace InterversionTests.Revisions
{
    // RevisionCVs: every CV created during the cascade. EvictedRevisionCVs: CVs evicted by EnforceConfiguration (and thus tombstoned).
    public sealed class EntitySnapshot
    {
        public string DocId;
        public List<string> RevisionCVs = new();
        public List<string> EvictedRevisionCVs = new();
        public List<AttachmentInfo> RevisionAttachments = new();
        public List<AttachmentInfo> RevisionAttachmentTombstones = new();
    }

    public sealed class AttachmentInfo
    {
        public string ParentRevisionCV;
        public string Name;
        public string ContentType;
    }

    // Hand-off from SeedAsync; the slaved process is alive on return for the caller to upgrade or kill.
    public sealed class OldDatabaseState
    {
        public InterversionTestBase.ProcessNode Node;
        public string Database;
        public string DataDirectory => Node.DataDir;
        public string BackupFolderPath;
        public string SnapshotFilePath;
        public Dictionary<string, EntitySnapshot> CapturedCVs = new();
    }

    // RunInMemory=false so legacy rows survive into the data dir the upgrade consumes.
    public abstract class OldDataFixture : InterversionTestBase
    {
        protected OldDataFixture(ITestOutputHelper output) : base(output)
        {
        }

        protected async Task<OldDatabaseState> SeedAsync(
            string oldVersion,
            string database,
            Func<IDocumentStore, OldDatabaseState, Task> seedCallback,
            bool produceBackup = false,
            bool produceSnapshot = false)
        {
            var customSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false",
                [RavenConfiguration.GetKey(x => x.Licensing.EulaAccepted)] = "true",
            };

            var node = await GetServerAsync(oldVersion, customSettings: customSettings, database: database);
            var state = new OldDatabaseState { Node = node, Database = database };

            var store = new DocumentStore
            {
                Urls = new[] { node.Url },
                Database = database
            };
            store.Initialize();

            try
            {
                var doc = new DatabaseRecord(database)
                {
                    Settings =
                    {
                        [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false",
                    }
                };
                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc));

                await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        MinimumRevisionsToKeep = 100,
                        PurgeOnDelete = false
                    }
                }));

                await seedCallback(store, state);

                if (produceBackup || produceSnapshot)
                {
                    var backupPath = NewDataPath(suffix: produceSnapshot ? "snapshot" : "backup");
                    var backupConfig = new PeriodicBackupConfiguration
                    {
                        Name = "OldDataFixtureBackup",
                        LocalSettings = new LocalSettings { FolderPath = backupPath },
                        BackupType = produceSnapshot ? BackupType.Snapshot : BackupType.Backup,
                        FullBackupFrequency = "0 0 1 1 *",
                        IncrementalBackupFrequency = null
                    };

                    var backupOp = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(backupConfig));
                    await store.Maintenance.SendAsync(new StartBackupOperation(isFullBackup: true, backupOp.TaskId));

                    // Wait on status, not file presence -- the latter is racy (subdir exists before the
                    // backup is flushed; file-poll can return mid-write and the caller may then kill the
                    // source server before the backup is fully on disk).
                    await WaitForFullBackupAsync(store, backupOp.TaskId);

                    if (produceSnapshot)
                        state.SnapshotFilePath = FindFirstSnapshotFile(backupPath);
                    if (produceBackup)
                    {
                        // Restore API expects the date-based subdir, not the parent.
                        var subdir = Directory.GetDirectories(backupPath);
                        state.BackupFolderPath = subdir.Length > 0 ? subdir[0] : backupPath;
                    }
                }
            }
            finally
            {
                // Intentionally not disposing the store -- AfterDispose would kill the slaved process before upgrade.
            }

            return state;
        }

        // Cascades put/update/attach/detach/delete (cap=100), then drops cap to 2 and enforces to produce evicted-revision tombstones.
        protected Task<OldDatabaseState> SeedStandardAsync(
            string oldVersion,
            string database,
            int docCount = 1,
            bool produceBackup = false,
            bool produceSnapshot = false)
        {
            return SeedAsync(oldVersion, database, async (store, state) =>
            {
                for (int i = 0; i < docCount; i++)
                {
                    var docId = $"users/{i + 1}";
                    var snapshot = new EntitySnapshot { DocId = docId };

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "v0" }, docId);
                        await session.SaveChangesAsync();
                    }
                    snapshot.RevisionCVs.Add(await GetLatestRevisionCvAsync(store, docId));

                    for (int v = 1; v <= 2; v++)
                    {
                        using var session = store.OpenAsyncSession();
                        var u = await session.LoadAsync<User>(docId);
                        u.Name = "v" + v;
                        await session.SaveChangesAsync();
                        snapshot.RevisionCVs.Add(await GetLatestRevisionCvAsync(store, docId));
                    }

                    using (var session = store.OpenAsyncSession())
                    {
                        var u = await session.LoadAsync<User>(docId);
                        session.Advanced.Attachments.Store(u, "att-1",
                            new MemoryStream(new byte[] { 1, 2, 3, 4 }), "application/octet-stream");
                        await session.SaveChangesAsync();
                        snapshot.RevisionAttachments.Add(new AttachmentInfo
                        {
                            ParentRevisionCV = await GetLatestRevisionCvAsync(store, docId),
                            Name = "att-1",
                            ContentType = "application/octet-stream"
                        });
                    }
                    snapshot.RevisionCVs.Add(await GetLatestRevisionCvAsync(store, docId));

                    using (var session = store.OpenAsyncSession())
                    {
                        var u = await session.LoadAsync<User>(docId);
                        u.Name = "v3";
                        await session.SaveChangesAsync();
                    }
                    snapshot.RevisionCVs.Add(await GetLatestRevisionCvAsync(store, docId));

                    using (var session = store.OpenAsyncSession())
                    {
                        var u = await session.LoadAsync<User>(docId);
                        session.Advanced.Attachments.Delete(u, "att-1");
                        await session.SaveChangesAsync();
                        snapshot.RevisionAttachmentTombstones.Add(new AttachmentInfo
                        {
                            ParentRevisionCV = await GetLatestRevisionCvAsync(store, docId),
                            Name = "att-1",
                            ContentType = "application/octet-stream"
                        });
                    }
                    snapshot.RevisionCVs.Add(await GetLatestRevisionCvAsync(store, docId));

                    using (var session = store.OpenAsyncSession())
                    {
                        session.Delete(docId);
                        await session.SaveChangesAsync();
                    }
                    snapshot.RevisionCVs.Add(await GetLatestRevisionCvAsync(store, docId));

                    state.CapturedCVs[docId] = snapshot;
                }

                await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        MinimumRevisionsToKeep = 2,
                        PurgeOnDelete = false
                    }
                }));

                var enforceOp = await store.Operations.SendAsync(new EnforceRevisionsConfigurationOperation());
                await enforceOp.WaitForCompletionAsync(TimeSpan.FromMinutes(2));

                foreach (var snapshot in state.CapturedCVs.Values)
                {
                    if (snapshot.RevisionCVs.Count > 2)
                    {
                        int evictedCount = snapshot.RevisionCVs.Count - 2;
                        snapshot.EvictedRevisionCVs.AddRange(snapshot.RevisionCVs.GetRange(0, evictedCount));
                    }
                }
            }, produceBackup, produceSnapshot);
        }

        // Standard host store used to drive RestoreBackupOperation on a fresh current-bits data path.
        protected DocumentStore GetCurrentBitsStore()
        {
            return GetDocumentStore(new Options
            {
                Path = NewDataPath(suffix: "current"),
                RunInMemory = false,
                CreateDatabase = false
            });
        }

        // Drives RestoreBackupOperation and returns an initialised client store pointed at the restored DB.
        protected async Task<DocumentStore> RestoreAsync(DocumentStore hostStore, string restoreDb, string backupLocation)
        {
            var op = await hostStore.Maintenance.Server.SendAsync(new RestoreBackupOperation(new RestoreBackupConfiguration
            {
                DatabaseName = restoreDb,
                BackupLocation = backupLocation,
                DataDirectory = NewDataPath(suffix: "restored-data")
            }));
            await op.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

            var restoredStore = new DocumentStore { Urls = hostStore.Urls, Database = restoreDb };
            restoredStore.Initialize();
            return restoredStore;
        }

        private static string FindFirstSnapshotFile(string folder)
        {
            foreach (var file in Directory.EnumerateFiles(folder, "*.ravendb-snapshot", SearchOption.AllDirectories))
                return file;
            throw new InvalidOperationException($"No .ravendb-snapshot file found under '{folder}'.");
        }
    }
}
