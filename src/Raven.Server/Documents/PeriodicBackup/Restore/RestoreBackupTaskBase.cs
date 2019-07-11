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
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Utils;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Platform;
using Voron.Impl.Backup;
using Voron.Util.Settings;
using DatabaseSmuggler = Raven.Client.Documents.Smuggler.DatabaseSmuggler;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public abstract class RestoreBackupTaskBase
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<RestoreBackupTaskBase>("Server");

        private readonly ServerStore _serverStore;
        protected readonly RestoreBackupConfigurationBase RestoreFromConfiguration;//protect for using in GetFilesForRestore()
        private readonly string _nodeTag;
        private readonly OperationCancelToken _operationCancelToken;
        private bool _hasEncryptionKey;
        private readonly bool _restoringToDefaultDataDirectory;

        protected RestoreBackupTaskBase(ServerStore serverStore,
            RestoreBackupConfigurationBase restoreFromConfiguration,
            string nodeTag,
            OperationCancelToken operationCancelToken)
        {
            _serverStore = serverStore;
            RestoreFromConfiguration = restoreFromConfiguration;
            _nodeTag = nodeTag;
            _operationCancelToken = operationCancelToken;

            if (string.IsNullOrWhiteSpace(RestoreFromConfiguration.DatabaseName))
                throw new ArgumentException("Database name can't be null or empty");

            if (ResourceNameValidator.IsValidResourceName(RestoreFromConfiguration.DatabaseName, _serverStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new InvalidOperationException(errorMessage);

            _serverStore.EnsureNotPassive();

            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                if (_serverStore.Cluster.ReadDatabase(context, RestoreFromConfiguration.DatabaseName) != null)
                    throw new ArgumentException($"Cannot restore data to an existing database named {RestoreFromConfiguration.DatabaseName}");

                var clusterTopology = _serverStore.GetClusterTopology(context);

                _hasEncryptionKey = string.IsNullOrWhiteSpace(RestoreFromConfiguration.EncryptionKey) == false;
                if (_hasEncryptionKey)
                {
                    var key = Convert.FromBase64String(RestoreFromConfiguration.EncryptionKey);
                    if (key.Length != 256 / 8)
                        throw new InvalidOperationException($"The size of the key must be 256 bits, but was {key.Length * 8} bits.");

                    if (AdminDatabasesHandler.NotUsingHttps(clusterTopology.GetUrlFromTag(_serverStore.NodeTag)))
                        throw new InvalidOperationException("Cannot restore an encrypted database to a node which doesn't support SSL!");
                }
            }

            var hasRestoreDataDirectory = string.IsNullOrWhiteSpace(RestoreFromConfiguration.DataDirectory) == false;
            if (hasRestoreDataDirectory &&
                HasFilesOrDirectories(RestoreFromConfiguration.DataDirectory))
                throw new ArgumentException("New data directory must be empty of any files or folders, " +
                                            $"path: {RestoreFromConfiguration.DataDirectory}");

            if (hasRestoreDataDirectory == false)
                RestoreFromConfiguration.DataDirectory = GetDataDirectory();

            _restoringToDefaultDataDirectory = IsDefaultDataDirectory(RestoreFromConfiguration.DataDirectory, RestoreFromConfiguration.DatabaseName);
        }

        protected abstract Task<Stream> GetStream(string path);

        protected abstract Task<ZipArchive> GetZipArchiveForSnapshot(string path);
        protected abstract Task<ZipArchive> GetZipArchiveForSnapshotCalc(string path);

        protected abstract Task<List<string>> GetFilesForRestore();

        protected abstract string GetBackupPath(string smugglerFile);
        protected abstract string GetSmugglerBackupPath(string smugglerFile);

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
                    if (databaseRecord.Settings == null)
                        databaseRecord.Settings = new Dictionary<string, string>();

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

                    var addToInitLog = new Action<string>(txt => // init log is not save in mem during RestoreBackup
                    {
                        var msg = $"[RestoreBackup] {DateTime.UtcNow} :: Database '{databaseName}' : {txt}";
                        if (Logger.IsInfoEnabled)
                            Logger.Info(msg);
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

                        var (index, _) = await _serverStore.WriteDatabaseRecordAsync(databaseName, databaseRecord, null, RaftIdGenerator.NewId(), restoreSettings.DatabaseValues, isRestore: true);
                        await _serverStore.Cluster.WaitForIndexNotification(index);

                        DisableOngoingTasksIfNeeded(databaseRecord);

                        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        {
                            if (snapshotRestore)
                            {
                                await RestoreFromSmugglerFile(onProgress, database, firstFile, context);
                                await SmugglerRestore(database, filesToRestore, context, databaseRecord, onProgress, result);

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

                                result.AddInfo($"Successfully restored {result.SnapshotRestore.ReadCount} files during snapshot restore, took: {sw.ElapsedMilliseconds:#,#;;0}ms");
                                onProgress.Invoke(result.Progress);
                            }
                            else
                            {
                                await SmugglerRestore(database, filesToRestore, context, databaseRecord, onProgress, result);
                            }

                            result.DatabaseRecord.Processed = true;
                            result.Documents.Processed = true;
                            result.RevisionDocuments.Processed = true;
                            result.Conflicts.Processed = true;
                            result.Indexes.Processed = true;
                            result.Counters.Processed = true;
                            result.Identities.Processed = true;
                            result.CompareExchange.Processed = true;
                            result.Subscriptions.Processed = true;
                            onProgress.Invoke(result.Progress);
                        }
                    }

                    // after the db for restore is done, we can safely set the db state to normal and write the DatabaseRecord
                    databaseRecord.DatabaseState = DatabaseStateStatus.Normal;
                    var (updateIndex, _) = await _serverStore.WriteDatabaseRecordAsync(databaseName, databaseRecord, null, RaftIdGenerator.DontCareId, isRestore: true);
                    await _serverStore.Cluster.WaitForIndexNotification(updateIndex);

                    if (databaseRecord.Topology.RelevantFor(_serverStore.NodeTag))
                    {
                        // we need to wait for the database record change to be propagated properly
                        var db = await _serverStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                        await db.RachisLogIndexNotifications.WaitForIndexNotification(updateIndex, _operationCancelToken.Token);
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

                var databaseRecord = _serverStore.LoadDatabaseRecord(RestoreFromConfiguration.DatabaseName, out _);

                if (databaseRecord == null)
                {
                    // delete any files that we already created during the restore
                    IOExtensions.DeleteDirectory(RestoreFromConfiguration.DataDirectory);
                }
                else
                {
                    databaseRecord.Disabled = false;
                    databaseRecord.DatabaseState = DatabaseStateStatus.Normal;

                    var deleteResult = await _serverStore.DeleteDatabaseAsync(RestoreFromConfiguration.DatabaseName, true, new[] { _serverStore.NodeTag }, RaftIdGenerator.DontCareId);
                    await _serverStore.Cluster.WaitForIndexNotification(deleteResult.Index);
                }

                result.AddError($"Error occurred during restore of database {databaseName}. Exception: {e.Message}");
                onProgress.Invoke(result.Progress);
                throw;
            }
            finally
            {
                Dispose();
            }
        }

        private async Task<List<string>> GetOrderedFilesToRestore()
        {
            var files = await GetFilesForRestore();

            var orderedFiles = files
                .Where(RestorePointsBase.IsBackupOrSnapshot)
                .OrderBackups();

            if (orderedFiles == null)
                throw new ArgumentException("No files to restore from the backup location, " +
                                            $"path: {GetBackupLocation()}");

            if (string.IsNullOrWhiteSpace(RestoreFromConfiguration.LastFileNameToRestore))
                return orderedFiles.ToList();

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
            using (var zip = await GetZipArchiveForSnapshot(fullBackupPath))
            {
                foreach (var zipEntries in zip.Entries.GroupBy(x => x.FullName.Substring(0, x.FullName.Length - x.Name.Length)))
                {
                    var directory = zipEntries.Key;

                    if (string.IsNullOrWhiteSpace(directory))
                    {
                        foreach (var zipEntry in zipEntries)
                        {
                            if (string.Equals(zipEntry.Name, RestoreSettings.SettingsFileName, StringComparison.OrdinalIgnoreCase))
                            {
                                using (var entryStream = zipEntry.Open())
                                {
                                    var stream = RestoreFromConfiguration.BackupEncryptionSettings?.EncryptionMode == EncryptionMode.UseDatabaseKey ?
                                        new DecryptingXChaCha20Oly1305Stream(entryStream, Convert.FromBase64String(RestoreFromConfiguration.EncryptionKey))
                                        : entryStream;

                                    var json = context.Read(stream, "read database settings for restore");
                                    json.BlittableValidation();

                                    restoreSettings = JsonDeserializationServer.RestoreSettings(json);

                                    restoreSettings.DatabaseRecord.DatabaseName = RestoreFromConfiguration.DatabaseName;
                                    DatabaseHelper.Validate(RestoreFromConfiguration.DatabaseName, restoreSettings.DatabaseRecord, _serverStore.Configuration);

                                    if (restoreSettings.DatabaseRecord.Encrypted && _hasEncryptionKey == false)
                                        throw new ArgumentException("Database snapshot is encrypted but the encryption key is missing!");

                                    if (restoreSettings.DatabaseRecord.Encrypted == false && _hasEncryptionKey)
                                        throw new ArgumentException("Cannot encrypt a non encrypted snapshot backup during restore!");
                                }
                            }
                        }

                        continue;
                    }

                    var voronDataDirectory = new VoronPathSetting(RestoreFromConfiguration.DataDirectory);
                    var restoreDirectory = directory.StartsWith(Constants.Documents.PeriodicBackup.Folders.Documents, StringComparison.OrdinalIgnoreCase)
                        ? voronDataDirectory
                        : voronDataDirectory.Combine(directory);

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
            }

            if (restoreSettings == null)
                throw new InvalidDataException("Cannot restore the snapshot without the settings file!");

            return restoreSettings;
        }

        protected async Task SmugglerRestore(DocumentDatabase database, List<string> filesToRestore, DocumentsOperationContext context,
            DatabaseRecord databaseRecord, Action<IOperationProgress> onProgress, RestoreResult result)
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

            var oldOperateOnTypes = DatabaseSmuggler.ConfigureOptionsForIncrementalImport(options);
            var destination = new DatabaseDestination(database);

            for (var i = 0; i < filesToRestore.Count - 1; i++)
            {
                result.AddInfo($"Restoring file {(i + 1):#,#;;0}/{filesToRestore.Count:#,#;;0}");
                onProgress.Invoke(result.Progress);

                var filePath = GetBackupPath(filesToRestore[i]);
                await ImportSingleBackupFile(database, onProgress, result, filePath, context, destination, options, isLastFile: false,
                    onDatabaseRecordAction: smugglerDatabaseRecord =>
                    {
                        // need to enable revisions before import
                        database.DocumentsStorage.RevisionsStorage.InitializeFromDatabaseRecord(smugglerDatabaseRecord);
                    });
            }

            options.OperateOnTypes = oldOperateOnTypes;
            var lastFilePath = GetBackupPath(filesToRestore.Last());

            result.AddInfo($"Restoring file {filesToRestore.Count:#,#;;0}/{filesToRestore.Count:#,#;;0}");

            onProgress.Invoke(result.Progress);

            await ImportSingleBackupFile(database, onProgress, result, lastFilePath, context, destination, options, isLastFile: true,
                onIndexAction: indexAndType =>
                {
                    if (this.RestoreFromConfiguration.SkipIndexes)
                        return;

                    switch (indexAndType.Type)
                    {
                        case IndexType.AutoMap:
                        case IndexType.AutoMapReduce:
                            var autoIndexDefinition = (AutoIndexDefinitionBase)indexAndType.IndexDefinition;
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
                    databaseRecord.SinkPullReplications = smugglerDatabaseRecord.SinkPullReplications;
                    databaseRecord.HubPullReplications = smugglerDatabaseRecord.HubPullReplications;
                    databaseRecord.Revisions = smugglerDatabaseRecord.Revisions;
                    databaseRecord.Expiration = smugglerDatabaseRecord.Expiration;
                    databaseRecord.RavenConnectionStrings = smugglerDatabaseRecord.RavenConnectionStrings;
                    databaseRecord.SqlConnectionStrings = smugglerDatabaseRecord.SqlConnectionStrings;
                    databaseRecord.Client = smugglerDatabaseRecord.Client;

                    // need to enable revisions before import
                    database.DocumentsStorage.RevisionsStorage.InitializeFromDatabaseRecord(smugglerDatabaseRecord);
                });
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

            if (databaseRecord.ExternalReplications != null)
            {
                foreach (var task in databaseRecord.ExternalReplications)
                {
                    task.Disabled = true;
                }
            }

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

            if (databaseRecord.PeriodicBackups != null)
            {
                foreach (var task in databaseRecord.PeriodicBackups)
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
            using (var fileStream = await GetStream(filePath))
            using (var inputStream = GetInputStream(fileStream))
            using (var gzipStream = new GZipStream(inputStream, CompressionMode.Decompress))
            using (var source = new StreamSource(gzipStream, context, database))
            {
                var smuggler = new Smuggler.Documents.DatabaseSmuggler(database, source, destination,
                    database.Time, options, result: restoreResult, onProgress: onProgress, token: _operationCancelToken.Token)
                {
                    OnIndexAction = onIndexAction,
                    OnDatabaseRecordAction = onDatabaseRecordAction
                };
                smuggler.Execute(ensureStepsProcessed: false, isLastFile);
            }
        }

        /// <summary>
        /// Restore CompareExchange, Identities and Subscriptions from smuggler file when restoring snapshot.
        /// </summary>
        /// <param name="onProgress"></param>
        /// <param name="database"></param>
        /// <param name="smugglerFile"></param>
        /// <param name="context"></param>
        protected async Task RestoreFromSmugglerFile(Action<IOperationProgress> onProgress, DocumentDatabase database, string smugglerFile, DocumentsOperationContext context)
        {
            var destination = new DatabaseDestination(database);

            var smugglerOptions = new DatabaseSmugglerOptionsServerSide
            {
                AuthorizationStatus = AuthorizationStatus.DatabaseAdmin,
                OperateOnTypes = DatabaseItemType.CompareExchange | DatabaseItemType.Identities | DatabaseItemType.Subscriptions,
                SkipRevisionCreation = true
            };

            var lastPath = GetSmugglerBackupPath(smugglerFile);

            using (var zip = await GetZipArchiveForSnapshot(lastPath))
            {
                foreach (var entry in zip.Entries)
                {
                    if (entry.Name == RestoreSettings.SmugglerValuesFileName)
                    {
                        using (var input = entry.Open())
                        using (var inputStream = GetSnapshotInputStream(input, database.Name))
                        using (var uncompressed = new GZipStream(inputStream, CompressionMode.Decompress))
                        {
                            var source = new StreamSource(uncompressed, context, database);
                            var smuggler = new Smuggler.Documents.DatabaseSmuggler(database, source, destination,
                                database.Time, smugglerOptions, onProgress: onProgress, token: _operationCancelToken.Token);

                            smuggler.Execute(ensureStepsProcessed: true, isLastFile: true);
                        }
                        break;
                    }
                }
            }
        }

        private Stream GetInputStream(Stream fileStream)
        {
            return RestoreFromConfiguration.BackupEncryptionSettings?.Key != null ? new DecryptingXChaCha20Oly1305Stream(fileStream, Convert.FromBase64String(RestoreFromConfiguration.BackupEncryptionSettings.Key)) : fileStream;
        }

        private Stream GetSnapshotInputStream(Stream fileStream, string database)
        {
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var key = _serverStore.GetSecretKey(ctx, database);
                if (key != null)
                {
                    return new DecryptingXChaCha20Oly1305Stream(fileStream, key);
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
            _operationCancelToken.Dispose();
        }

        public async Task<long> CalculateBackupSizeInBytes()
        {
            var zipPath = GetBackupLocation();
             zipPath = Path.Combine(zipPath, RestoreFromConfiguration.LastFileNameToRestore);
            using (var zip = await GetZipArchiveForSnapshotCalc(zipPath))
                return zip.Entries.Sum(entry => entry.Length);
        }
    }
}
