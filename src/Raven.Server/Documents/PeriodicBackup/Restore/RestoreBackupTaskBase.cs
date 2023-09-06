using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Utils;
using Raven.Server.Web.System;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server.Exceptions;
using Sparrow.Server.Utils;
using Sparrow.Utils;
using Voron.Data.Tables;
using Voron.Impl.Backup;
using Voron.Util.Settings;
using BackupUtils = Raven.Client.Documents.Smuggler.BackupUtils;
using Index = Raven.Server.Documents.Indexes.Index;
using RavenServerBackupUtils = Raven.Server.Utils.BackupUtils;
using System.Threading;
using Size = Sparrow.Size;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public abstract class RestoreBackupTaskBase
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<RestoreBackupTaskBase>("Server");

        private readonly ServerStore _serverStore;
        private readonly string _nodeTag;
        private readonly OperationCancelToken _operationCancelToken;
        private bool _hasEncryptionKey;
        private readonly bool _restoringToDefaultDataDirectory;
        private ZipArchive _zipArchiveForSnapshot;

        public RestoreBackupConfigurationBase RestoreFromConfiguration { get; }

        protected RestoreBackupTaskBase(ServerStore serverStore,
            RestoreBackupConfigurationBase restoreFromConfiguration,
            string nodeTag,
            OperationCancelToken operationCancelToken)
        {
            _serverStore = serverStore;
            RestoreFromConfiguration = restoreFromConfiguration;
            _nodeTag = nodeTag;
            _operationCancelToken = operationCancelToken;

            var dataDirectoryThatWillBeUsed = string.IsNullOrWhiteSpace(RestoreFromConfiguration.DataDirectory) ?
                                       _serverStore.Configuration.Core.DataDirectory.FullPath :
                                       new PathSetting(RestoreFromConfiguration.DataDirectory, _serverStore.Configuration.Core.DataDirectory.FullPath).FullPath;

            if (ResourceNameValidator.IsValidResourceName(RestoreFromConfiguration.DatabaseName, dataDirectoryThatWillBeUsed, out string errorMessage) == false)
                throw new InvalidOperationException(errorMessage);

            _serverStore.EnsureNotPassiveAsync().Wait(_operationCancelToken.Token);

            ClusterTopology clusterTopology;
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                if (_serverStore.Cluster.DatabaseExists(context, RestoreFromConfiguration.DatabaseName))
                    throw new ArgumentException($"Cannot restore data to an existing database named {RestoreFromConfiguration.DatabaseName}");

                clusterTopology = _serverStore.GetClusterTopology(context);
            }

            _hasEncryptionKey = string.IsNullOrWhiteSpace(RestoreFromConfiguration.EncryptionKey) == false;
            if (_hasEncryptionKey)
            {
                var key = Convert.FromBase64String(RestoreFromConfiguration.EncryptionKey);
                if (key.Length != 256 / 8)
                    throw new InvalidOperationException($"The size of the key must be 256 bits, but was {key.Length * 8} bits.");

                if (AdminDatabasesHandler.NotUsingHttps(clusterTopology.GetUrlFromTag(_serverStore.NodeTag)))
                    throw new InvalidOperationException("Cannot restore an encrypted database to a node which doesn't support SSL!");
            }

            var backupEncryptionSettings = RestoreFromConfiguration.BackupEncryptionSettings;
            if (backupEncryptionSettings != null)
            {
                if (backupEncryptionSettings.EncryptionMode == EncryptionMode.UseProvidedKey &&
                    backupEncryptionSettings.Key == null)
                {
                    throw new InvalidOperationException($"{nameof(BackupEncryptionSettings.EncryptionMode)} is set to {nameof(EncryptionMode.UseProvidedKey)} but an encryption key wasn't provided");
                }

                if (backupEncryptionSettings.EncryptionMode != EncryptionMode.UseProvidedKey &&
                    backupEncryptionSettings.Key != null)
                {
                    throw new InvalidOperationException($"{nameof(BackupEncryptionSettings.EncryptionMode)} is set to {backupEncryptionSettings.EncryptionMode} but an encryption key was provided");
                }
            }

            var hasRestoreDataDirectory = string.IsNullOrWhiteSpace(RestoreFromConfiguration.DataDirectory) == false;
            if (hasRestoreDataDirectory &&
                HasFilesOrDirectories(dataDirectoryThatWillBeUsed))
                throw new ArgumentException("New data directory must be empty of any files or folders, " +
                                            $"path: {dataDirectoryThatWillBeUsed}");

            if (hasRestoreDataDirectory == false)
                RestoreFromConfiguration.DataDirectory = GetDataDirectory();

            _restoringToDefaultDataDirectory = IsDefaultDataDirectory(RestoreFromConfiguration.DataDirectory, RestoreFromConfiguration.DatabaseName);
        }

        protected async Task<Stream> CopyRemoteStreamLocallyAsync(Stream stream, Size size, Action<string> onProgress)
        {
            return await CopyRemoteStreamLocallyAsync(stream, size, _serverStore.Configuration, onProgress, _operationCancelToken.Token);
        }

        public static async Task<Stream> CopyRemoteStreamLocallyAsync(Stream stream, Size size, RavenConfiguration configuration, Action<string> onProgress, CancellationToken cancellationToken)
        {
            if (stream.CanSeek)
                return stream;

            // This is meant to be used by ZipArchive, which will copy the data locally because is *must* be seekable.
            // To avoid reading everything to memory, we copy to a local file instead. Note that this also ensure that we
            // can process files > 2GB in size. https://github.com/dotnet/runtime/issues/59027

            var filePath = RavenServerBackupUtils.GetBackupTempPath(configuration, $"{Guid.NewGuid()}.snapshot-restore", out PathSetting basePath).FullPath;
            IOExtensions.CreateDirectory(basePath.FullPath);
            var file = SafeFileStream.Create(filePath, FileMode.Create, FileAccess.ReadWrite, FileShare.Read,
                32 * 1024, FileOptions.DeleteOnClose);

            try
            {
                AssertFreeSpace(size, basePath.FullPath);

                var sw = Stopwatch.StartNew();
                var swForProgress = Stopwatch.StartNew();
                long totalRead = 0;

                onProgress?.Invoke($"Copying ZipArchive locally, size: {size}");

                await stream.CopyToAsync(file, readCount =>
                {
                    totalRead += readCount;
                    if (swForProgress.ElapsedMilliseconds > 5000)
                    {
                        swForProgress.Restart();
                        onProgress?.Invoke($"Copied: {new Size(totalRead, SizeUnit.Bytes)}/{size}");
                    }
                }, cancellationToken);

                onProgress?.Invoke($"Copied ZipArchive locally, took: {sw.Elapsed}");

                file.Seek(0, SeekOrigin.Begin);

                return file;
            }
            catch
            {
                try
                {
                    await file.DisposeAsync();
                }
                catch
                {
                    // nothing we can do
                }
                finally
                {
                    PosixFile.DeleteOnClose(filePath);
                }

                throw;
            }
        }

        private static void AssertFreeSpace(Size size, string basePath)
        {
            var spaceInfo = DiskUtils.GetDiskSpaceInfo(basePath);
            if (spaceInfo == null)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to get space info for '{basePath}'");

                return;
            }

            // + we need to download the snapshot
            // + we need to unzip the snapshot
            // + leave 1GB of free space
            var freeSpaceNeeded = 2 * size + new Size(1, SizeUnit.Gigabytes);

            if (freeSpaceNeeded > spaceInfo.TotalFreeSpace)
                throw new DiskFullException($"There is not enough space on '{basePath}', we need at least {freeSpaceNeeded} in order to successfully copy the snapshot backup file locally. " +
                                            $"Currently available space is {spaceInfo.TotalFreeSpace}.");
        }

        protected abstract Task<Stream> GetStream(string path);

        protected abstract Task<ZipArchive> GetZipArchiveForSnapshot(string path, Action<string> onProgress);

        protected abstract Task<List<string>> GetFilesForRestore();

        protected abstract string GetBackupPath(string smugglerFile);

        protected abstract string GetBackupLocation();

        public async Task<IOperationResult> Execute(Action<IOperationProgress> onProgress)
        {
            var databaseName = RestoreFromConfiguration.DatabaseName;
            var result = new RestoreResult
            {
                DataDirectory = RestoreFromConfiguration.DataDirectory
            };

            try
            {
                var filesToRestore = await GetOrderedFilesToRestore();

                using (_serverStore.ContextPool.AllocateOperationContext(out JsonOperationContext serverContext))
                {
                    if (onProgress == null)
                        onProgress = _ => { };

                    Stopwatch sw = null;
                    RestoreSettings restoreSettings = null;
                    var firstFile = filesToRestore[0];

                    var extension = Path.GetExtension(firstFile);
                    var snapshotRestore = false;

                    if ((extension == Constants.Documents.PeriodicBackup.SnapshotExtension) ||
                        (extension == Constants.Documents.PeriodicBackup.EncryptedSnapshotExtension))
                    {
                        onProgress.Invoke(result.Progress);

                        snapshotRestore = true;
                        sw = Stopwatch.StartNew();
                        if (extension == Constants.Documents.PeriodicBackup.EncryptedSnapshotExtension)
                        {
                            _hasEncryptionKey = RestoreFromConfiguration.EncryptionKey != null ||
                                                RestoreFromConfiguration.BackupEncryptionSettings?.Key != null;
                        }
                        // restore the snapshot
                        restoreSettings = await SnapshotRestore(serverContext, firstFile, onProgress, result);

                        if (restoreSettings != null && RestoreFromConfiguration.SkipIndexes)
                        {
                            // remove all indexes from the database record
                            restoreSettings.DatabaseRecord.AutoIndexes = null;
                            restoreSettings.DatabaseRecord.Indexes = null;
                        }
                        // removing the snapshot from the list of files
                        filesToRestore.RemoveAt(0);
                    }
                    else
                    {
                        result.SnapshotRestore.Skipped = true;
                        result.SnapshotRestore.Processed = true;

                        onProgress.Invoke(result.Progress);
                    }

                    if (restoreSettings == null)
                    {
                        restoreSettings = new RestoreSettings
                        {
                            DatabaseRecord = new DatabaseRecord(databaseName)
                            {
                                // we only have a smuggler restore
                                // use the encryption key to encrypt the database
                                Encrypted = _hasEncryptionKey
                            }
                        };

                        DatabaseHelper.Validate(databaseName, restoreSettings.DatabaseRecord, _serverStore.Configuration);
                    }

                    var databaseRecord = restoreSettings.DatabaseRecord;
                    databaseRecord.Settings ??= new Dictionary<string, string>();

                    var runInMemoryConfigurationKey = RavenConfiguration.GetKey(x => x.Core.RunInMemory);
                    databaseRecord.Settings.Remove(runInMemoryConfigurationKey);
                    if (_serverStore.Configuration.Core.RunInMemory)
                        databaseRecord.Settings[runInMemoryConfigurationKey] = "false";

                    var dataDirectoryConfigurationKey = RavenConfiguration.GetKey(x => x.Core.DataDirectory);
                    databaseRecord.Settings.Remove(dataDirectoryConfigurationKey); // removing because we want to restore to given location, not to serialized in backup one
                    if (_restoringToDefaultDataDirectory == false)
                        databaseRecord.Settings[dataDirectoryConfigurationKey] = RestoreFromConfiguration.DataDirectory;

                    if (_hasEncryptionKey)
                    {
                        // save the encryption key so we'll be able to access the database
                        _serverStore.PutSecretKey(RestoreFromConfiguration.EncryptionKey,
                            databaseName, overwrite: false);
                    }

                    var addToInitLog = new Action<LogMode, string>((logMode, txt) => // init log is not save in mem during RestoreBackup
                    {
                        var msg = $"[RestoreBackup] {DateTime.UtcNow} :: Database '{databaseName}' : {txt}";

                        switch (logMode)
                        {
                            case LogMode.Operations when Logger.IsOperationsEnabled:
                                Logger.Operations(msg);
                                break;

                            case LogMode.Information when Logger.IsInfoEnabled:
                                Logger.Info(msg);
                                break;
                        }
                    });

                    var configuration = _serverStore
                        .DatabasesLandlord
                        .CreateDatabaseConfiguration(databaseName, ignoreDisabledDatabase: true, ignoreBeenDeleted: true, ignoreNotRelevant: true, databaseRecord);

                    using (var database = new DocumentDatabase(databaseName, configuration, _serverStore, addToInitLog))
                    {
                        // smuggler needs an existing document database to operate
                        var options = InitializeOptions.SkipLoadingDatabaseRecord;
                        if (snapshotRestore)
                            options |= InitializeOptions.GenerateNewDatabaseId;

                        database.Initialize(options);
                        databaseRecord.Topology = new DatabaseTopology();

                        // restoring to the current node only
                        databaseRecord.Topology.Members.Add(_nodeTag);
                        // we are currently restoring, shouldn't try to access it
                        databaseRecord.DatabaseState = DatabaseStateStatus.RestoreInProgress;

                        await SaveDatabaseRecordAsync(databaseName, databaseRecord, restoreSettings.DatabaseValues, result, onProgress);
                        _serverStore.ForTestingPurposes?.RestoreDatabaseAfterSavingDatabaseRecord?.Invoke();
                   
                        database.SupportedFeatures = new SupportedFeature(databaseRecord);
                        database.ClusterTransactionId = databaseRecord.Topology.ClusterTransactionIdBase64;
                        database.DatabaseGroupId = databaseRecord.Topology.DatabaseTopologyIdBase64;

                        database.TxMerger.Start();

                        result.Files.FileCount = filesToRestore.Count + (snapshotRestore ? 1 : 0);
                        
                        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        {
                            if (snapshotRestore)
                            {
                                await RestoreFromSmugglerFile(onProgress, database, firstFile, context, result);
                                await HandleSubscriptionFromSnapshot(filesToRestore, restoreSettings.Subscriptions, databaseName, database);
                                await SmugglerRestore(database, filesToRestore, context, databaseRecord, onProgress, result, new SnapshotDatabaseDestination(database, restoreSettings.Subscriptions));

                                result.SnapshotRestore.Processed = true;

                                var summary = database.GetDatabaseSummary();
                                result.Documents.ReadCount += summary.DocumentsCount;
                                result.Documents.Attachments.ReadCount += summary.AttachmentsCount;
                                result.Counters.ReadCount += summary.CounterEntriesCount;
                                result.RevisionDocuments.ReadCount += summary.RevisionsCount;
                                result.Conflicts.ReadCount += summary.ConflictsCount;
                                result.Indexes.ReadCount += databaseRecord.GetIndexesCount();
                                result.CompareExchange.ReadCount += summary.CompareExchangeCount;
                                result.CompareExchangeTombstones.ReadCount += summary.CompareExchangeTombstonesCount;
                                result.Identities.ReadCount += summary.IdentitiesCount;
                                result.TimeSeries.ReadCount += summary.TimeSeriesSegmentsCount;

                                result.AddInfo($"Successfully restored {result.SnapshotRestore.ReadCount} files during snapshot restore, took: {sw.ElapsedMilliseconds:#,#;;0}ms");
                                onProgress.Invoke(result.Progress);

                                using (var tx = context.OpenWriteTransaction())
                                {
                                    var changeVector = database.DocumentsStorage.GetNewChangeVector(context);
                                    database.DocumentsStorage.SetDatabaseChangeVector(context, changeVector.ChangeVector);
                                    tx.Commit();
                                }
                            }
                            else
                            {
                                await SmugglerRestore(database, filesToRestore, context, databaseRecord, onProgress, result, new DatabaseDestination(database));
                            }

                            DisableOngoingTasksIfNeeded(databaseRecord);

                            Raven.Server.Smuggler.Documents.DatabaseSmuggler.EnsureProcessed(result, skipped: false, indexesSkipped: result.Indexes.Skipped);

                            onProgress.Invoke(result.Progress);
                        }

                        if (snapshotRestore)
                            RegenerateDatabaseIdInIndexes(configuration, database);
                    }

                    // after the db for restore is done, we can safely set the db state to normal and write the DatabaseRecord
                    databaseRecord.DatabaseState = DatabaseStateStatus.Normal;
                    await SaveDatabaseRecordAsync(databaseName, databaseRecord, null, result, onProgress);

                    result.AddInfo($"Loading the database after restore");

                    try
                    {
                        await _serverStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName, addToInitLog: (_, message) => result.AddInfo(message));
                    }
                    catch (Exception e)
                    {
                        // we failed to load the database after restore, we don't want to fail the entire restore process since it will delete the database if we throw here
                        result.AddError($"Failed to load the database after restore, {e}");

                        if (Logger.IsOperationsEnabled)
                            Logger.Operations($"Failed to load the database '{databaseName}' after restore", e);
                    }

                    return result;
                }
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations("Failed to restore database", e);

                var alert = AlertRaised.Create(
                    RestoreFromConfiguration.DatabaseName,
                    "Failed to restore database",
                    $"Could not restore database named {RestoreFromConfiguration.DatabaseName}",
                    AlertType.RestoreError,
                    NotificationSeverity.Error,
                    details: new ExceptionDetails(e));
                _serverStore.NotificationCenter.Add(alert);

                using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    bool databaseExists;
                    using (context.OpenReadTransaction())
                    {
                        databaseExists = _serverStore.Cluster.DatabaseExists(context, RestoreFromConfiguration.DatabaseName);
                    }

                    if (databaseExists == false)
                    {
                        // delete any files that we already created during the restore
                        IOExtensions.DeleteDirectory(RestoreFromConfiguration.DataDirectory);
                    }
                    else
                    {
                        try
                        {
                            var deleteResult = await _serverStore.DeleteDatabaseAsync(RestoreFromConfiguration.DatabaseName, true, new[] { _serverStore.NodeTag },
                                RaftIdGenerator.DontCareId);
                            await _serverStore.Cluster.WaitForIndexNotification(deleteResult.Index, TimeSpan.FromSeconds(60));
                        }
                        catch (TimeoutException te)
                        {
                            result.AddError($"Failed to delete the database {databaseName} after a failed restore. " +
                                            $"In order to restart the restore process this database needs to be deleted manually. Exception: {te}.");
                            onProgress.Invoke(result.Progress);
                        }
                    }
                }

                result.AddError($"Error occurred during restore of database {databaseName}. Exception: {e}");
                onProgress.Invoke(result.Progress);
                throw;
            }
            finally
            {
                Dispose();
            }

            void RegenerateDatabaseIdInIndexes(RavenConfiguration configuration, DocumentDatabase database)
            {
                // this code will generate new DatabaseId for each index.
                // This is something that we need to do when snapshot restore is executed to match the newly generated database id

                var indexesPath = configuration.Indexing.StoragePath.FullPath;
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
                        result.AddError($"Could not open index from path '{indexPath}'. Error: {e.Message}");
                    }
                    finally
                    {
                        index?.Dispose();
                    }
                }
            }
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

        private async Task SaveDatabaseRecordAsync(string databaseName, DatabaseRecord databaseRecord, Dictionary<string,
            BlittableJsonReaderObject> databaseValues, SmugglerResult restoreResult, Action<IOperationProgress> onProgress)
        {
            // at this point we restored a large portion of the database or all of it	
            // we'll retry saving the database record since a failure here will cause us to abort the entire restore operation	

            var index = await BackupHelper.RunWithRetriesAsync(maxRetries: 10, async () =>
                {
                    var result = await _serverStore.WriteDatabaseRecordAsync(
                        databaseName, databaseRecord, null, RaftIdGenerator.NewId(), databaseValues, isRestore: true);
                    return result.Index;
                },
                infoMessage: "Saving the database record",
                errorMessage: "Failed to save the database record, the restore is aborted",
                restoreResult, onProgress, _operationCancelToken);

            await BackupHelper.RunWithRetriesAsync(maxRetries: 10, async () =>
                {
                    await _serverStore.Cluster.WaitForIndexNotification(index, TimeSpan.FromSeconds(30));
                    return index;
                },
                infoMessage: $"Verifying that the change to the database record propagated to node {_serverStore.NodeTag}",
                errorMessage: $"Failed to verify that the change to the database record was propagated to node {_serverStore.NodeTag}, the restore is aborted",
                restoreResult, onProgress, _operationCancelToken);
        }

        private async Task<List<string>> GetOrderedFilesToRestore()
        {
            var files = await GetFilesForRestore();

            var orderedFiles = files
                .Where(RestorePointsBase.IsBackupOrSnapshot)
                .OrderBackups()
                .ToList();

            if (orderedFiles.Any() == false)
                throw new ArgumentException($"No files to restore from the backup location, path: {GetBackupLocation()}");

            if (string.IsNullOrWhiteSpace(RestoreFromConfiguration.LastFileNameToRestore))
                return orderedFiles;

            var filesToRestore = new List<string>();

            foreach (var file in orderedFiles)
            {
                filesToRestore.Add(file);
                if (file.Equals(RestoreFromConfiguration.LastFileNameToRestore, StringComparison.OrdinalIgnoreCase))
                    break;
            }

            return filesToRestore;
        }

        protected async Task<RestoreSettings> SnapshotRestore(JsonOperationContext context, string backupPath,
            Action<IOperationProgress> onProgress, RestoreResult restoreResult)
        {
            Debug.Assert(onProgress != null);

            RestoreSettings restoreSettings = null;

            var fullBackupPath = GetBackupPath(backupPath);
            _zipArchiveForSnapshot = await GetZipArchiveForSnapshot(fullBackupPath, onProgress: message =>
            {
                restoreResult.AddInfo(message);
                onProgress.Invoke(restoreResult.Progress);
            });

            var restorePath = new VoronPathSetting(RestoreFromConfiguration.DataDirectory);
            if (Directory.Exists(restorePath.FullPath) == false)
                Directory.CreateDirectory(restorePath.FullPath);

            // validate free space
            var snapshotSize = _zipArchiveForSnapshot.Entries.Sum(entry => entry.Length);
            BackupHelper.AssertFreeSpaceForSnapshot(restorePath.FullPath, snapshotSize, "restore a backup", Logger);

            foreach (var zipEntries in _zipArchiveForSnapshot.Entries.GroupBy(x => x.FullName.Substring(0, x.FullName.Length - x.Name.Length)))
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
                                var snapshotEncryptionKey = RestoreFromConfiguration.EncryptionKey != null
                                    ? Convert.FromBase64String(RestoreFromConfiguration.EncryptionKey)
                                    : null;

                                await using (var decompressionStream = FullBackup.GetDecompressionStream(entryStream))
                                await using (var stream = await GetInputStreamAsync(decompressionStream, snapshotEncryptionKey))
                                {
                                    var json = await context.ReadForMemoryAsync(stream, "read database settings for restore");
                                    json.BlittableValidation();

                                    restoreSettings = JsonDeserializationServer.RestoreSettings(json);
                                    // It's necessary to modify the subscriptionId to prevent collisions with the current database index.
                                    //we will handle subscription with smuggler
                                    RemoveSubscriptionFromDatabaseValues(restoreSettings);
                                    restoreSettings.DatabaseRecord.DatabaseName = RestoreFromConfiguration.DatabaseName;
                                    DatabaseHelper.Validate(RestoreFromConfiguration.DatabaseName, restoreSettings.DatabaseRecord, _serverStore.Configuration);

                                    if (restoreSettings.DatabaseRecord.Encrypted && _hasEncryptionKey == false)
                                        throw new ArgumentException("Database snapshot is encrypted but the encryption key is missing!");

                                    if (restoreSettings.DatabaseRecord.Encrypted == false && _hasEncryptionKey)
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
                    cancellationToken: _operationCancelToken.Token);
            }

            if (restoreSettings == null)
                throw new InvalidDataException("Cannot restore the snapshot without the settings file!");

            return restoreSettings;
        }

        protected async Task SmugglerRestore(DocumentDatabase database, List<string> filesToRestore, DocumentsOperationContext context,
            DatabaseRecord databaseRecord, Action<IOperationProgress> onProgress, RestoreResult result, DatabaseDestination lastFileDestination)
        {
            Debug.Assert(onProgress != null);

            // the files are already ordered by name
            // take only the files that are relevant for smuggler restore

            if (filesToRestore.Count == 0)
                return;

            // we do have at least one smuggler backup, we'll take the indexes from the last file
            databaseRecord.AutoIndexes = new Dictionary<string, AutoIndexDefinition>();
            databaseRecord.Indexes = new Dictionary<string, IndexDefinition>();

            // restore the smuggler backup
            var options = new DatabaseSmugglerOptionsServerSide
            {
                AuthorizationStatus = AuthorizationStatus.DatabaseAdmin,
                SkipRevisionCreation = true
            };

            options.OperateOnTypes |= DatabaseItemType.LegacyDocumentDeletions;
            options.OperateOnTypes |= DatabaseItemType.LegacyAttachments;
            options.OperateOnTypes |= DatabaseItemType.LegacyAttachmentDeletions;
#pragma warning disable 618
            options.OperateOnTypes |= DatabaseItemType.Counters;
#pragma warning restore 618

            var oldOperateOnTypes = Raven.Client.Documents.Smuggler.DatabaseSmuggler.ConfigureOptionsForIncrementalImport(options);
            var destination = new DatabaseDestination(database);

            for (var i = 0; i < filesToRestore.Count - 1; i++)
            {
                result.AddInfo($"Restoring file {(i + 1):#,#;;0}/{filesToRestore.Count:#,#;;0}");

                var fileName = filesToRestore[i];
                result.Files.CurrentFileName = fileName;
                result.Files.CurrentFile++;

                onProgress.Invoke(result.Progress);

                var filePath = GetBackupPath(fileName);
                await ImportSingleBackupFile(database, onProgress, result, filePath, context, destination, options, isLastFile: false,
                    onDatabaseRecordAction: smugglerDatabaseRecord =>
                    {
                        // need to enable revisions before import
                        database.DocumentsStorage.RevisionsStorage.InitializeFromDatabaseRecord(smugglerDatabaseRecord);
                    });
            }

            options.OperateOnTypes = oldOperateOnTypes;

            result.AddInfo($"Restoring file {filesToRestore.Count:#,#;;0}/{filesToRestore.Count:#,#;;0}");

            var lastFileName = filesToRestore.Last();
            result.Files.CurrentFileName = lastFileName;
            result.Files.CurrentFile++;

            result.Indexes.Skipped = RestoreFromConfiguration.SkipIndexes;

            onProgress.Invoke(result.Progress);

            var lastFilePath = GetBackupPath(lastFileName);

            await ImportSingleBackupFile(database, onProgress, result, lastFilePath, context, lastFileDestination, options, isLastFile: true,
                onIndexAction: indexAndType =>
                {
                    if (this.RestoreFromConfiguration.SkipIndexes)
                        return;

                    switch (indexAndType.Type)
                    {
                        case IndexType.AutoMap:
                        case IndexType.AutoMapReduce:
                            var autoIndexDefinition = (AutoIndexDefinitionBaseServerSide)indexAndType.IndexDefinition;
                            databaseRecord.AutoIndexes[autoIndexDefinition.Name] =
                                PutAutoIndexCommand.GetAutoIndexDefinition(autoIndexDefinition, indexAndType.Type);
                            break;
                        case IndexType.Map:
                        case IndexType.MapReduce:
                        case IndexType.JavaScriptMap:
                        case IndexType.JavaScriptMapReduce:
                            var indexDefinition = (IndexDefinition)indexAndType.IndexDefinition;
                            databaseRecord.Indexes[indexDefinition.Name] = indexDefinition;
                            break;
                        case IndexType.None:
                        case IndexType.Faulty:
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                },
                onDatabaseRecordAction: smugglerDatabaseRecord =>
                {
                    databaseRecord.ConflictSolverConfig = smugglerDatabaseRecord.ConflictSolverConfig;
                    foreach (var setting in smugglerDatabaseRecord.Settings)
                    {
                        databaseRecord.Settings[setting.Key] = setting.Value;
                    }
                    databaseRecord.SqlEtls = smugglerDatabaseRecord.SqlEtls;
                    databaseRecord.RavenEtls = smugglerDatabaseRecord.RavenEtls;
                    databaseRecord.PeriodicBackups = smugglerDatabaseRecord.PeriodicBackups;
                    databaseRecord.ExternalReplications = smugglerDatabaseRecord.ExternalReplications;
                    databaseRecord.Sorters = smugglerDatabaseRecord.Sorters;
                    databaseRecord.Analyzers = smugglerDatabaseRecord.Analyzers;
                    databaseRecord.SinkPullReplications = smugglerDatabaseRecord.SinkPullReplications;
                    databaseRecord.HubPullReplications = smugglerDatabaseRecord.HubPullReplications;
                    databaseRecord.Revisions = smugglerDatabaseRecord.Revisions;
                    databaseRecord.Expiration = smugglerDatabaseRecord.Expiration;
                    databaseRecord.RavenConnectionStrings = smugglerDatabaseRecord.RavenConnectionStrings;
                    databaseRecord.SqlConnectionStrings = smugglerDatabaseRecord.SqlConnectionStrings;
                    databaseRecord.Client = smugglerDatabaseRecord.Client;
                    databaseRecord.TimeSeries = smugglerDatabaseRecord.TimeSeries;
                    databaseRecord.DocumentsCompression = smugglerDatabaseRecord.DocumentsCompression;
                    databaseRecord.LockMode = smugglerDatabaseRecord.LockMode;
                    databaseRecord.OlapConnectionStrings = smugglerDatabaseRecord.OlapConnectionStrings;
                    databaseRecord.OlapEtls = smugglerDatabaseRecord.OlapEtls;
                    databaseRecord.ElasticSearchEtls = smugglerDatabaseRecord.ElasticSearchEtls;
                    databaseRecord.ElasticSearchConnectionStrings = smugglerDatabaseRecord.ElasticSearchConnectionStrings;
                    databaseRecord.QueueEtls = smugglerDatabaseRecord.QueueEtls;
                    databaseRecord.QueueConnectionStrings = smugglerDatabaseRecord.QueueConnectionStrings;
                    databaseRecord.IndexesHistory = smugglerDatabaseRecord.IndexesHistory;
                    databaseRecord.Refresh = smugglerDatabaseRecord.Refresh;
                    databaseRecord.Integrations = smugglerDatabaseRecord.Integrations;
                    databaseRecord.Studio = smugglerDatabaseRecord.Studio;
                    databaseRecord.RevisionsForConflicts = smugglerDatabaseRecord.RevisionsForConflicts;
                    databaseRecord.SupportedFeatures = smugglerDatabaseRecord.SupportedFeatures;

                    // need to enable revisions before import
                    database.DocumentsStorage.RevisionsStorage.InitializeFromDatabaseRecord(smugglerDatabaseRecord);
                    database.SupportedFeatures = new SupportedFeature(databaseRecord);
                });

            result.Files.CurrentFileName = null;
        }

        private bool IsDefaultDataDirectory(string dataDirectory, string databaseName)
        {
            var defaultDataDirectory = RavenConfiguration.GetDataDirectoryPath(
                _serverStore.Configuration.Core,
                databaseName,
                ResourceType.Database);

            return PlatformDetails.RunningOnPosix == false
                ? string.Equals(defaultDataDirectory, dataDirectory, StringComparison.OrdinalIgnoreCase)
                : string.Equals(defaultDataDirectory, dataDirectory, StringComparison.Ordinal);
        }

        private string GetDataDirectory()
        {
            var dataDirectory =
                RavenConfiguration.GetDataDirectoryPath(
                    _serverStore.Configuration.Core,
                    RestoreFromConfiguration.DatabaseName,
                    ResourceType.Database);

            var i = 0;
            while (HasFilesOrDirectories(dataDirectory))
                dataDirectory += $"-{++i}";

            return dataDirectory;
        }

        private void DisableOngoingTasksIfNeeded(DatabaseRecord databaseRecord)
        {
            if (RestoreFromConfiguration.DisableOngoingTasks == false)
                return;

            if (databaseRecord.RavenEtls != null)
            {
                foreach (var task in databaseRecord.RavenEtls)
                {
                    task.Disabled = true;
                }
            }

            if (databaseRecord.SqlEtls != null)
            {
                foreach (var task in databaseRecord.SqlEtls)
                {
                    task.Disabled = true;
                }
            }

            if (databaseRecord.OlapEtls != null)
            {
                foreach (var task in databaseRecord.OlapEtls)
                {
                    task.Disabled = true;
                }
            }

            if (databaseRecord.ElasticSearchEtls != null)
            {
                foreach (var task in databaseRecord.ElasticSearchEtls)
                {
                    task.Disabled = true;
                }
            }

            if (databaseRecord.QueueEtls != null)
            {
                foreach (var task in databaseRecord.QueueEtls)
                {
                    task.Disabled = true;
                }
            }

            if (databaseRecord.PeriodicBackups != null)
            {
                foreach (var task in databaseRecord.PeriodicBackups)
                {
                    task.Disabled = true;
                }
            }

            if (databaseRecord.ExternalReplications != null)
            {
                foreach (var task in databaseRecord.ExternalReplications)
                {
                    task.Disabled = true;
                }
            }

            if (databaseRecord.HubPullReplications != null)
            {
                foreach (var task in databaseRecord.HubPullReplications)
                {
                    task.Disabled = true;
                }
            }

            if (databaseRecord.SinkPullReplications != null)
            {
                foreach (var task in databaseRecord.SinkPullReplications)
                {
                    task.Disabled = true;
                }
            }
        }

        private async Task ImportSingleBackupFile(DocumentDatabase database,
            Action<IOperationProgress> onProgress, RestoreResult restoreResult,
            string filePath, DocumentsOperationContext context,
            DatabaseDestination destination, DatabaseSmugglerOptionsServerSide options, bool isLastFile,
            Action<IndexDefinitionAndType> onIndexAction = null,
            Action<DatabaseRecord> onDatabaseRecordAction = null)
        {
            await using (var fileStream = await GetStream(filePath))
            await using (var inputStream = await GetInputStreamAsync(fileStream, database.MasterKey))
            await using (var gzipStream = await RavenServerBackupUtils.GetDecompressionStreamAsync(inputStream))
            using (var source = new StreamSource(gzipStream, context, database))
            {
                var smuggler = new Smuggler.Documents.DatabaseSmuggler(database, source, destination,
                    database.Time, options, result: restoreResult, onProgress: onProgress, token: _operationCancelToken.Token)
                {
                    OnIndexAction = onIndexAction,
                    OnDatabaseRecordAction = onDatabaseRecordAction,
                    BackupKind = BackupUtils.IsFullBackup(Path.GetExtension(filePath)) ? BackupKind.Full : BackupKind.Incremental
                };
                await smuggler.ExecuteAsync(ensureStepsProcessed: false, isLastFile);
            }
        }

        /// <summary>
        /// Restore CompareExchange, Identities and Subscriptions from smuggler file when restoring snapshot.
        /// </summary>
        /// <param name="onProgress"></param>
        /// <param name="database"></param>
        /// <param name="smugglerFile"></param>
        /// <param name="context"></param>
        /// <param name="result"></param>
        protected async Task RestoreFromSmugglerFile(Action<IOperationProgress> onProgress, DocumentDatabase database, string smugglerFile,
            DocumentsOperationContext context, RestoreResult result)
        {
            var destination = new DatabaseDestination(database);

            var smugglerOptions = new DatabaseSmugglerOptionsServerSide
            {
                AuthorizationStatus = AuthorizationStatus.DatabaseAdmin,
                OperateOnTypes = DatabaseItemType.CompareExchange | DatabaseItemType.Identities | DatabaseItemType.Subscriptions,
                SkipRevisionCreation = true
            };

            result.Files.CurrentFileName = smugglerFile; 
            result.Files.CurrentFile++;

            onProgress.Invoke(result.Progress);

            if (_zipArchiveForSnapshot == null)
                throw new InvalidOperationException($"Restoring of smuggler values failed because {nameof(_zipArchiveForSnapshot)} is null");

            var entry = _zipArchiveForSnapshot.GetEntry(RestoreSettings.SmugglerValuesFileName);
            if (entry != null)
            {
                await using (var input = entry.Open())
                await using (var inputStream = await GetSnapshotInputStreamAsync(input, database.Name))
                await using (var uncompressed = await RavenServerBackupUtils.GetDecompressionStreamAsync(inputStream))
                {
                    var source = new StreamSource(uncompressed, context, database);
                    var smuggler = new Smuggler.Documents.DatabaseSmuggler(database, source, destination,
                        database.Time, smugglerOptions, onProgress: onProgress, token: _operationCancelToken.Token)
                    {
                        BackupKind = BackupKind.Incremental
                    };

                    await smuggler.ExecuteAsync(ensureStepsProcessed: true, isLastFile: true);
                }
            }
        }

        private async Task<Stream> GetInputStreamAsync(Stream stream, byte[] databaseEncryptionKey)
        {
            if (RestoreFromConfiguration.BackupEncryptionSettings == null ||
                RestoreFromConfiguration.BackupEncryptionSettings.EncryptionMode == EncryptionMode.None)
                return stream;

            byte[] encryptionKey;

            if (RestoreFromConfiguration.BackupEncryptionSettings.EncryptionMode == EncryptionMode.UseDatabaseKey)
            {
                if (databaseEncryptionKey == null)
                    throw new ArgumentException("Stream is encrypted but the encryption key is missing!");

                encryptionKey = databaseEncryptionKey;
            }
            else
            {
                encryptionKey = Convert.FromBase64String(RestoreFromConfiguration.BackupEncryptionSettings.Key);
            }

            var decryptingStream = new DecryptingXChaCha20Oly1305Stream(stream, encryptionKey);

            await decryptingStream.InitializeAsync();

            return decryptingStream;
        }

        private async Task<Stream> GetSnapshotInputStreamAsync(Stream fileStream, string database)
        {
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var key = _serverStore.GetSecretKey(ctx, database);
                if (key != null)
                {
                    var decryptingStream = new DecryptingXChaCha20Oly1305Stream(fileStream, key);

                    await decryptingStream.InitializeAsync();

                    return decryptingStream;
                }
            }

            return fileStream;
        }

        protected bool HasFilesOrDirectories(string location)
        {
            if (Directory.Exists(location) == false)
                return false;

            return Directory.GetFiles(location).Length > 0 ||
                   Directory.GetDirectories(location).Length > 0;
        }

        protected virtual void Dispose()
        {
            using (_zipArchiveForSnapshot)
                _operationCancelToken.Dispose();
        }
    }
}
