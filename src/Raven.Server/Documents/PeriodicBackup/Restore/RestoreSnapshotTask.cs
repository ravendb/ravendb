using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Web.System;
using Sparrow.Json;
using Voron.Data.Tables;
using Voron.Impl.Backup;
using Voron.Util.Settings;
using Index = Raven.Server.Documents.Indexes.Index;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    internal class RestoreSnapshotTask : AbstractRestoreBackupTask
    {
        private Stopwatch _sw;
        private readonly string _firstFile, _extension;

        public RestoreSnapshotTask(ServerStore serverStore, RestoreBackupConfigurationBase restoreConfiguration, IRestoreSource restoreSource, 
            string firstFile, string extension, List<string> filesToRestore, OperationCancelToken operationCancelToken) : base(serverStore, restoreConfiguration, restoreSource, filesToRestore, operationCancelToken)
        {
            _firstFile = firstFile;
            _extension = extension;
        }

        protected override async Task RestoreAsync()
        {
            await RestoreFromSmugglerFileAsync(Progress, Database, _firstFile, Context);
            await SmugglerRestoreAsync(Database, Context);

            Result.SnapshotRestore.Processed = true;

            var summary = Database.GetDatabaseSummary();
            Result.Documents.ReadCount += summary.DocumentsCount;
            Result.Documents.Attachments.ReadCount += summary.AttachmentsCount;
            Result.Counters.ReadCount += summary.CounterEntriesCount;
            Result.RevisionDocuments.ReadCount += summary.RevisionsCount;
            Result.Conflicts.ReadCount += summary.ConflictsCount;
            Result.Indexes.ReadCount += RestoreSettings.DatabaseRecord.GetIndexesCount();
            Result.CompareExchange.ReadCount += summary.CompareExchangeCount;
            Result.CompareExchangeTombstones.ReadCount += summary.CompareExchangeTombstonesCount;
            Result.Identities.ReadCount += summary.IdentitiesCount;
            Result.TimeSeries.ReadCount += summary.TimeSeriesSegmentsCount;

            Result.AddInfo($"Successfully restored {Result.SnapshotRestore.ReadCount} files during snapshot restore, took: {_sw.ElapsedMilliseconds:#,#;;0}ms");
            Progress.Invoke(Result.Progress);
        }

        protected override async Task InitializeAsync()
        {
            await base.InitializeAsync();
            
            Options |= InitializeOptions.GenerateNewDatabaseId;

            Progress.Invoke(Result.Progress);

            _sw = Stopwatch.StartNew();

            if (_extension == Constants.Documents.PeriodicBackup.EncryptedSnapshotExtension)
            {
                HasEncryptionKey = RestoreConfiguration.EncryptionKey != null ||
                                    RestoreConfiguration.BackupEncryptionSettings?.Key != null;
            }

            // restore the snapshot
            RestoreSettings = await RestoreSnapshotAsync(Context, _firstFile, Progress, Result);

            Debug.Assert(RestoreSettings != null);

            if (RestoreConfiguration.SkipIndexes)
            {
                // remove all indexes from the database record
                RestoreSettings.DatabaseRecord.AutoIndexes = null;
                RestoreSettings.DatabaseRecord.Indexes = null;
            }

            // removing the snapshot from the list of files
            FilesToRestore.RemoveAt(0);
        }

        protected override void OnAfterRestore()
        {
            base.OnAfterRestore();
            RegenerateDatabaseIdInIndexes(Database);
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                var changeVector = Database.DocumentsStorage.GetNewChangeVector(context);
                Database.DocumentsStorage.SetDatabaseChangeVector(context, changeVector.ChangeVector);
                tx.Commit();
            }
        }

        private async Task<RestoreSettings> RestoreSnapshotAsync(JsonOperationContext context, string backupPath,
            Action<IOperationProgress> onProgress, RestoreResult restoreResult)
        {
            Debug.Assert(onProgress != null);

            RestoreSettings restoreSettings = null;

            var fullBackupPath = RestoreSource.GetBackupPath(backupPath);
            using (var zip = await RestoreSource.GetZipArchiveForSnapshot(fullBackupPath))
            {
                var restorePath = new VoronPathSetting(RestoreConfiguration.DataDirectory);
                if (Directory.Exists(restorePath.FullPath) == false)
                    Directory.CreateDirectory(restorePath.FullPath);

                // validate free space
                var snapshotSize = zip.Entries.Sum(entry => entry.Length);
                BackupHelper.AssertFreeSpaceForSnapshot(restorePath.FullPath, snapshotSize, "restore a backup", Logger);

                foreach (var zipEntries in zip.Entries.GroupBy(x => x.FullName.Substring(0, x.FullName.Length - x.Name.Length)))
                {
                    var directory = zipEntries.Key;

                    if (string.IsNullOrWhiteSpace(directory))
                    {
                        foreach (var zipEntry in zipEntries)
                        {
                            if (string.Equals(zipEntry.Name, RestoreSettings.SettingsFileName, StringComparison.OrdinalIgnoreCase))
                            {
                                await using (var entryStream = zipEntry.Open())
                                {
                                    var snapshotEncryptionKey = RestoreConfiguration.EncryptionKey != null
                                        ? Convert.FromBase64String(RestoreConfiguration.EncryptionKey)
                                        : null;

                                    await using (var stream = GetInputStream(entryStream, snapshotEncryptionKey))
                                    {
                                        var json = await context.ReadForMemoryAsync(stream, "read database settings for restore");
                                        json.BlittableValidation();

                                        restoreSettings = JsonDeserializationServer.RestoreSettings(json);

                                        restoreSettings.DatabaseRecord.DatabaseName = RestoreConfiguration.DatabaseName;
                                        DatabaseHelper.Validate(RestoreConfiguration.DatabaseName, restoreSettings.DatabaseRecord, ServerStore.Configuration);

                                        if (restoreSettings.DatabaseRecord.Encrypted && HasEncryptionKey == false)
                                            throw new ArgumentException("Database snapshot is encrypted but the encryption key is missing!");

                                        if (restoreSettings.DatabaseRecord.Encrypted == false && HasEncryptionKey)
                                            throw new ArgumentException("Cannot encrypt a non encrypted snapshot backup during restore!");
                                    }
                                }
                            }
                        }

                        continue;
                    }

                    var restoreDirectory = directory.StartsWith(Constants.Documents.PeriodicBackup.Folders.Documents, StringComparison.OrdinalIgnoreCase)
                        ? restorePath
                        : restorePath.Combine(directory);

                    var isSubDirectory = PathUtil.IsSubDirectory(restoreDirectory.FullPath, restorePath.FullPath);
                    if (isSubDirectory == false)
                    {
                        var extensions = zipEntries
                            .Select(x => Path.GetExtension(x.Name))
                            .Distinct()
                            .ToArray();

                        if (extensions.Length != 1 || string.Equals(extensions[0], TableValueCompressor.CompressionRecoveryExtension, StringComparison.OrdinalIgnoreCase) == false)
                            throw new InvalidOperationException($"Encountered invalid directory '{directory}' in snapshot file with following file extensions: {string.Join(", ", extensions)}");

                        // this enables backward compatibility of snapshot backups with compression recovery files before fix was made in RavenDB-17173
                        // the underlying issue was that we were putting full path when compression recovery files were backed up using snapshot
                        // because of that the end restore directory was not a sub-directory of a restore path
                        // which could result in a file already exists exception
                        // since restoring of compression recovery files is not mandatory then it is safe to skip them
                        continue;
                    }

                    BackupMethods.Full.Restore(
                        zipEntries,
                        restoreDirectory,
                        journalDir: null,
                        onProgress: message =>
                        {
                            restoreResult.AddInfo(message);
                            restoreResult.SnapshotRestore.ReadCount++;
                            onProgress.Invoke(restoreResult.Progress);
                        },
                        cancellationToken: OperationCancelToken.Token);
                }
            }

            if (restoreSettings == null)
                throw new InvalidDataException("Cannot restore the snapshot without the settings file!");

            return restoreSettings;
        }

        private async Task RestoreFromSmugglerFileAsync(Action<IOperationProgress> onProgress, DocumentDatabase database, string smugglerFile, JsonOperationContext context)
        {
            var destination = new DatabaseDestination(database);

            var smugglerOptions = new DatabaseSmugglerOptionsServerSide
            {
                AuthorizationStatus = AuthorizationStatus.DatabaseAdmin,
                OperateOnTypes = DatabaseItemType.CompareExchange | DatabaseItemType.Identities | DatabaseItemType.Subscriptions,
                SkipRevisionCreation = true
            };

            var lastPath = RestoreSource.GetSmugglerBackupPath(smugglerFile);

            using (var zip = await RestoreSource.GetZipArchiveForSnapshot(lastPath))
            {
                foreach (var entry in zip.Entries)
                {
                    if (entry.Name == RestoreSettings.SmugglerValuesFileName)
                    {
                        await using (var input = entry.Open())
                        await using (var inputStream = GetSnapshotInputStream(input, database.Name))
                        await using (var uncompressed = new GZipStream(inputStream, CompressionMode.Decompress))
                        {
                            var source = new StreamSource(uncompressed, context, database.Name);
                            var smuggler = new Smuggler.Documents.DatabaseSmuggler(database, source, destination,
                                database.Time, context, smugglerOptions, onProgress: onProgress, token: OperationCancelToken.Token);

                            await smuggler.ExecuteAsync(ensureStepsProcessed: true, isLastFile: true);
                        }
                        break;
                    }
                }
            }
        }

        private Stream GetSnapshotInputStream(Stream fileStream, string database)
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var key = ServerStore.GetSecretKey(ctx, database);
                if (key != null)
                {
                    return new DecryptingXChaCha20Oly1305Stream(fileStream, key);
                }
            }

            return fileStream;
        }


        private void RegenerateDatabaseIdInIndexes(DocumentDatabase database)
        {
            // this code will generate new DatabaseId for each index.
            // This is something that we need to do when snapshot restore is executed to match the newly generated database id

            var indexesPath = database.Configuration.Indexing.StoragePath.FullPath;
            if (Directory.Exists(indexesPath) == false)
                return;

            foreach (var indexPath in Directory.GetDirectories(indexesPath))
            {
                Index index = null;
                try
                {
                    index = Index.Open(indexPath, database, generateNewDatabaseId: true);
                }
                catch (Exception e)
                {
                    Result.AddError($"Could not open index from path '{indexPath}'. Error: {e.Message}");
                }
                finally
                {
                    index?.Dispose();
                }
            }
        }

    }
}
