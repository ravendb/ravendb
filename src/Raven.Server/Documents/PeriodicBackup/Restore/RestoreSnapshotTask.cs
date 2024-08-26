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
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Web.System;
using Sparrow.Json;
using Voron.Data.Tables;
using Voron.Impl.Backup;
using Voron.Util.Settings;
using Index = Raven.Server.Documents.Indexes.Index;
using RavenServerBackupUtils = Raven.Server.Utils.BackupUtils;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    internal sealed class RestoreSnapshotTask : AbstractRestoreBackupTask
    {
        private Stopwatch _sw;
        private readonly string _firstFile, _extension;
        private ZipArchive _zipArchive;

        public RestoreSnapshotTask(ServerStore serverStore, RestoreBackupConfigurationBase restoreConfiguration, IRestoreSource restoreSource,
            string firstFile, string extension, List<string> filesToRestore, OperationCancelToken operationCancelToken) : base(serverStore, restoreConfiguration, restoreSource, filesToRestore, operationCancelToken)
        {
            _firstFile = firstFile;
            _extension = extension;
        }

        protected override async Task RestoreAsync()
        {
            await RestoreFromSmugglerFileAsync(Progress, Database, _firstFile, Context);
            await HandleSubscriptionFromSnapshot(FilesToRestore, RestoreSettings.Subscriptions, DatabaseName, Database);
            await SmugglerRestoreAsync(Database, Context, Database.Smuggler.CreateDestinationForSnapshotRestore(RestoreSettings.Subscriptions));

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

        private static void RemoveSubscriptionFromDatabaseValues(RestoreSettings restoreSettings)
        {
            foreach (var keyValue in restoreSettings.DatabaseValues)
            {
                if (keyValue.Key.StartsWith(SubscriptionState.Prefix, StringComparison.OrdinalIgnoreCase))
                {
                    var subscriptionState = JsonDeserializationClient.SubscriptionState(keyValue.Value);
                    restoreSettings.Subscriptions.Add(keyValue.Key, subscriptionState);
                }
            }

            foreach (var keyValue in restoreSettings.Subscriptions)
            {
                restoreSettings.DatabaseValues.Remove(keyValue.Key);
            }
        }

        private static async Task HandleSubscriptionFromSnapshot(List<string> filesToRestore, Dictionary<string, SubscriptionState> subscription,
            string databaseName, DocumentDatabase database)
        {
            //When dealing with multiple files, we will manage subscriptions using the smuggler.
            if (filesToRestore.Count > 0)
                return;

            foreach (var (name, state) in subscription)
            {
                var command = new PutSubscriptionCommand(databaseName, state.Query, state.MentorNode, RaftIdGenerator.DontCareId)
                {
                    Disabled = state.Disabled,
                    InitialChangeVector = state.ChangeVectorForNextBatchStartingPoint,
                };
                //There's no need to wait for the execution of this command at this point since we will wait for subsequent commands later.
                await database.ServerStore.SendToLeaderAsync(command);
            }
        }

        protected override async Task InitializeAsync()
        {
            await base.InitializeAsync();

            Result.Files.FileCount = FilesToRestore.Count + 1;

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

        protected override async Task OnAfterRestoreBeforeReturnAsync()
        {
            Result.AddInfo($"Loading the database after restore");

            try
            {
                await ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(DatabaseName, addToInitLog: message => Result.AddInfo(message));
            }
            catch (Exception e)
            {
                // we failed to load the database after restore, we don't want to fail the entire restore process since it will delete the database if we throw here
                Result.AddError($"Failed to load the database after restore, {e}");

                if (Logger.IsErrorEnabled)
                    Logger.Error($"Failed to load the database '{DatabaseName}' after restore", e);
            }
        }

        protected override async Task OnAfterRestoreAsync()
        {
            await base.OnAfterRestoreAsync();
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
            _zipArchive = await RestoreSource.GetZipArchiveForSnapshot(fullBackupPath, onProgress: message =>
            {
                restoreResult.AddInfo(message);
                onProgress.Invoke(restoreResult.Progress);
            });

            var restorePath = new VoronPathSetting(RestoreConfiguration.DataDirectory);
            if (Directory.Exists(restorePath.FullPath) == false)
                Directory.CreateDirectory(restorePath.FullPath);

            // validate free space
            var snapshotSize = _zipArchive.Entries.Sum(entry => entry.Length);
            BackupHelper.AssertFreeSpaceForSnapshot(restorePath.FullPath, snapshotSize, "restore a backup", Logger);

            foreach (var zipEntries in _zipArchive.Entries.GroupBy(x => x.FullName.Substring(0, x.FullName.Length - x.Name.Length)))
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

                                await using (var decompressionStream = FullBackup.GetDecompressionStream(entryStream))
                                await using (var stream = await GetInputStreamAsync(decompressionStream, snapshotEncryptionKey))
                                {
                                    var json = await context.ReadForMemoryAsync(stream, "read database settings for restore");
                                    json.BlittableValidation();

                                    restoreSettings = JsonDeserializationServer.RestoreSettings(json);
                                    RemoveSubscriptionFromDatabaseValues(restoreSettings);
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

                    if (extensions.Length != 1 || string.Equals(extensions[0], TableValueCompressor.CompressionRecoveryExtension, StringComparison.OrdinalIgnoreCase) ==
                        false)
                        throw new InvalidOperationException(
                            $"Encountered invalid directory '{directory}' in snapshot file with following file extensions: {string.Join(", ", extensions)}");

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

            if (restoreSettings == null)
                throw new InvalidDataException("Cannot restore the snapshot without the settings file!");

            return restoreSettings;
        }

        private async Task RestoreFromSmugglerFileAsync(Action<IOperationProgress> onProgress, DocumentDatabase database, string smugglerFile, JsonOperationContext context)
        {
            var destination = database.Smuggler.CreateDestination();

            var smugglerOptions = new DatabaseSmugglerOptionsServerSide(AuthorizationStatus.DatabaseAdmin)
            {
                OperateOnTypes = DatabaseItemType.CompareExchange | DatabaseItemType.Identities | DatabaseItemType.Subscriptions,
                SkipRevisionCreation = true
            };

            Result.Files.CurrentFileName = smugglerFile; 
            Result.Files.CurrentFile++;

            onProgress.Invoke(Result.Progress);

            if (_zipArchive == null)
                throw new InvalidOperationException($"Restoring of smuggler values failed because {nameof(_zipArchive)} is null");

            var entry = _zipArchive.GetEntry(RestoreSettings.SmugglerValuesFileName);
            if (entry != null)
            {
                await using (var input = entry.Open())
                await using (var inputStream = await GetSnapshotInputStreamAsync(input, database.Name))
                await using (var uncompressed = await RavenServerBackupUtils.GetDecompressionStreamAsync(inputStream))
                {
                    var source = new StreamSource(uncompressed, context, database.Name, smugglerOptions);

                    var smuggler = database.Smuggler.CreateForRestore(databaseRecord: null, source, destination, context, smugglerOptions, result: null, onProgress,
                        OperationCancelToken.Token);
                    smuggler.BackupKind = BackupKind.Incremental;

                    await smuggler.ExecuteAsync(ensureStepsProcessed: true, isLastFile: true);
                }
            }
        }

        private async Task<Stream> GetSnapshotInputStreamAsync(Stream fileStream, string database)
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var key = ServerStore.GetSecretKey(ctx, database);
                if (key != null)
                {
                    var decryptingStream = new DecryptingXChaCha20Oly1305Stream(fileStream, key);

                    await decryptingStream.InitializeAsync();

                    return decryptingStream;
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
                    index = Index.Open(indexPath, database, generateNewDatabaseId: true, out _);
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

        public override void Dispose()
        {
            using (_zipArchive)
                base.Dispose();
        }
    }
}
