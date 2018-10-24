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
using Raven.Client.ServerWide;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
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
using Sparrow.Logging;
using Sparrow.Platform;
using Voron.Impl.Backup;
using Voron.Util.Settings;
using DatabaseSmuggler = Raven.Client.Documents.Smuggler.DatabaseSmuggler;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public class RestoreBackupTask
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<RestoreBackupTask>("Server");

        private readonly ServerStore _serverStore;
        private readonly RestoreBackupConfiguration _restoreConfiguration;
        private readonly string _nodeTag;
        private readonly OperationCancelToken _operationCancelToken;
        private List<string> _filesToRestore;
        private bool _hasEncryptionKey;
        private readonly bool _restoringToDefaultDataDirectory;

        public RestoreBackupTask(ServerStore serverStore,
            RestoreBackupConfiguration restoreConfiguration,
            string nodeTag,
            OperationCancelToken operationCancelToken)
        {
            _serverStore = serverStore;
            _restoreConfiguration = restoreConfiguration;
            _nodeTag = nodeTag;
            _operationCancelToken = operationCancelToken;

            ValidateArguments(out _restoringToDefaultDataDirectory);
        }

        public async Task<IOperationResult> Execute(Action<IOperationProgress> onProgress)
        {
            var databaseName = _restoreConfiguration.DatabaseName;
            var result = new RestoreResult
            {
                DataDirectory = _restoreConfiguration.DataDirectory
            };

            try
            {
                if (onProgress == null)
                    onProgress = _ => { };

                Stopwatch sw = null;
                RestoreSettings restoreSettings = null;
                var firstFile = _filesToRestore[0];
                var lastFile = _filesToRestore.Last();

                var extension = Path.GetExtension(firstFile);
                var snapshotRestore = false;
                if (extension == Constants.Documents.PeriodicBackup.SnapshotExtension)
                {
                    onProgress.Invoke(result.Progress);

                    snapshotRestore = true;
                    sw = Stopwatch.StartNew();
                    // restore the snapshot
                    restoreSettings = SnapshotRestore(firstFile,
                        _restoreConfiguration.DataDirectory,
                        onProgress,
                        result);

                    if (restoreSettings != null && _restoreConfiguration.SkipIndexes)
                    {
                        // remove all indexes from the database record
                        restoreSettings.DatabaseRecord.AutoIndexes = null;
                        restoreSettings.DatabaseRecord.Indexes = null;
                    }

                    // removing the snapshot from the list of files
                    _filesToRestore.RemoveAt(0);
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
                    databaseRecord.Settings[dataDirectoryConfigurationKey] = _restoreConfiguration.DataDirectory;

                if (_hasEncryptionKey)
                {
                    // save the encryption key so we'll be able to access the database
                    _serverStore.PutSecretKey(_restoreConfiguration.EncryptionKey,
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

                    if (snapshotRestore)
                    {
                        result.SnapshotRestore.Processed = true;

                        var summary = database.GetDatabaseSummary();
                        result.Documents.ReadCount += summary.DocumentsCount;
                        result.Documents.Attachments.ReadCount += summary.AttachmentsCount;
                        result.Counters.ReadCount += summary.CountersCount;
                        result.RevisionDocuments.ReadCount += summary.RevisionsCount;
                        result.Conflicts.ReadCount += summary.ConflictsCount;
                        result.Indexes.ReadCount += databaseRecord.GetIndexesCount();
                        result.AddInfo($"Successfully restored {result.SnapshotRestore.ReadCount} " +
                                       $"files during snapshot restore, took: {sw.ElapsedMilliseconds:#,#;;0}ms");
                        onProgress.Invoke(result.Progress);
                    }

                    using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        SmugglerRestore(_restoreConfiguration.BackupLocation, database, context, databaseRecord, onProgress, result);

                        result.DatabaseRecord.Processed = true;
                        result.Documents.Processed = true;
                        result.RevisionDocuments.Processed = true;
                        result.Conflicts.Processed = true;
                        result.Indexes.Processed = true;
                        result.Counters.Processed = true;
                        onProgress.Invoke(result.Progress);

                        databaseRecord.Topology = new DatabaseTopology();
                        // restoring to the current node only
                        databaseRecord.Topology.Members.Add(_nodeTag);
                        databaseRecord.Disabled = true; // we are currently restoring, shouldn't try to access it
                        _serverStore.EnsureNotPassive();

                        DisableOngoingTasksIfNeeded(databaseRecord);

                        var (index, _) = await _serverStore.WriteDatabaseRecordAsync(databaseName, databaseRecord, null, restoreSettings.DatabaseValues, isRestore: true);
                        await _serverStore.Cluster.WaitForIndexNotification(index);

                        // restore identities & cmpXchg values
                        RestoreFromLastFile(onProgress, database, lastFile, context, result);
                    }
                }

                // after the db for restore is done, we can safely set the db status to active
                databaseRecord = _serverStore.LoadDatabaseRecord(databaseName, out _);
                databaseRecord.Disabled = false;

                var (updateIndex, _) = await _serverStore.WriteDatabaseRecordAsync(databaseName, databaseRecord, null);
                await _serverStore.Cluster.WaitForIndexNotification(updateIndex);

                return result;
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations("Failed to restore database", e);

                var alert = AlertRaised.Create(
                    _restoreConfiguration.DatabaseName,
                    "Failed to restore database",
                    $"Could not restore database named {_restoreConfiguration.DatabaseName}",
                    AlertType.RestoreError,
                    NotificationSeverity.Error,
                    details: new ExceptionDetails(e));
                _serverStore.NotificationCenter.Add(alert);

                if (_serverStore.LoadDatabaseRecord(_restoreConfiguration.DatabaseName, out var _) == null)
                {
                    // delete any files that we already created during the restore
                    IOExtensions.DeleteDirectory(_restoreConfiguration.DataDirectory);
                }
                else
                {
                    var deleteResult = await _serverStore.DeleteDatabaseAsync(_restoreConfiguration.DatabaseName, true, new[] { _serverStore.NodeTag });
                    await _serverStore.Cluster.WaitForIndexNotification(deleteResult.Index);
                }

                result.AddError($"Error occurred during restore of database {databaseName}. Exception: {e.Message}");
                onProgress.Invoke(result.Progress);
                throw;
            }
            finally
            {
                _operationCancelToken.Dispose();
            }
        }

        private void DisableOngoingTasksIfNeeded(DatabaseRecord databaseRecord)
        {
            if (_restoreConfiguration.DisableOngoingTasks == false)
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

        private void RestoreFromLastFile(Action<IOperationProgress> onProgress, DocumentDatabase database, string lastFile, DocumentsOperationContext context, RestoreResult result)
        {
            var destination = new DatabaseDestination(database);
            var smugglerOptions = new DatabaseSmugglerOptionsServerSide
            {
                AuthorizationStatus = AuthorizationStatus.DatabaseAdmin,
                OperateOnTypes = DatabaseItemType.CompareExchange | DatabaseItemType.Identities
            };
            var lastPath = Path.Combine(_restoreConfiguration.BackupLocation, lastFile);

            if (Path.GetExtension(lastPath) == Constants.Documents.PeriodicBackup.SnapshotExtension)
            {
                using (var zip = ZipFile.Open(lastPath, ZipArchiveMode.Read, System.Text.Encoding.UTF8))
                {
                    foreach (var entry in zip.Entries)
                    {
                        if (entry.Name == RestoreSettings.SmugglerValuesFileName)
                        {
                            using (var input = entry.Open())
                            using (var uncompressed = new GZipStream(input, CompressionMode.Decompress))
                            {
                                var source = new StreamSource(uncompressed, context, database);
                                var smuggler = new Smuggler.Documents.DatabaseSmuggler(database, source, destination,
                                    database.Time, smugglerOptions, onProgress: onProgress, token: _operationCancelToken.Token);

                                smuggler.Execute();
                            }
                            break;
                        }
                    }
                }
            }
            else
            {
                ImportSingleBackupFile(database, onProgress, null, lastPath, context, destination, smugglerOptions);
            }

            result.Identities.Processed = true;
            result.CompareExchange.Processed = true;
            onProgress.Invoke(result.Progress);
        }

        private void ValidateArguments(out bool restoringToDefaultDataDirectory)
        {
            if (string.IsNullOrWhiteSpace(_restoreConfiguration.BackupLocation))
                throw new ArgumentException("Backup location can't be null or empty");

            if (Directory.Exists(_restoreConfiguration.BackupLocation) == false)
                throw new ArgumentException($"Backup location doesn't exist, path: {_restoreConfiguration.BackupLocation}");

            var hasRestoreDataDirectory = string.IsNullOrWhiteSpace(_restoreConfiguration.DataDirectory) == false;
            if (hasRestoreDataDirectory &&
                HasFilesOrDirectories(_restoreConfiguration.DataDirectory))
                throw new ArgumentException("New data directory must be empty of any files or folders, " +
                                            $"path: {_restoreConfiguration.DataDirectory}");

            if (hasRestoreDataDirectory == false)
                _restoreConfiguration.DataDirectory = GetDataDirectory();

            restoringToDefaultDataDirectory = IsDefaultDataDirectory(_restoreConfiguration.DataDirectory, _restoreConfiguration.DatabaseName);

            _filesToRestore = GetFilesForRestore(_restoreConfiguration.BackupLocation);
            if (_filesToRestore.Count == 0)
                throw new ArgumentException("No files to restore from the backup location, " +
                                            $"path: {_restoreConfiguration.BackupLocation}");

            _hasEncryptionKey = string.IsNullOrWhiteSpace(_restoreConfiguration.EncryptionKey) == false;
            if (_hasEncryptionKey)
            {
                var key = Convert.FromBase64String(_restoreConfiguration.EncryptionKey);
                if (key.Length != 256 / 8)
                    throw new InvalidOperationException($"The size of the encryption key must be 256 bits, but was {key.Length * 8} bits.");
            }
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
                    _restoreConfiguration.DatabaseName,
                    ResourceType.Database);

            var i = 0;
            while (HasFilesOrDirectories(dataDirectory))
                dataDirectory += $"-{++i}";

            return dataDirectory;
        }

        private List<string> GetFilesForRestore(string backupLocation)
        {
            var orderedFiles = Directory.GetFiles(backupLocation)
                .Where(RestoreUtils.IsBackupOrSnapshot)
                .OrderBackups();

            if (string.IsNullOrWhiteSpace(_restoreConfiguration.LastFileNameToRestore))
                return orderedFiles.ToList();

            var filesToRestore = new List<string>();
            foreach (var file in orderedFiles)
            {
                filesToRestore.Add(file);
                var fileName = Path.GetFileName(file);
                if (fileName.Equals(_restoreConfiguration.LastFileNameToRestore, StringComparison.OrdinalIgnoreCase))
                    break;
            }

            return filesToRestore;
        }

        private void SmugglerRestore(
            string backupDirectory,
            DocumentDatabase database,
            DocumentsOperationContext context,
            DatabaseRecord databaseRecord,
            Action<IOperationProgress> onProgress,
            RestoreResult result)
        {
            Debug.Assert(onProgress != null);

            // the files are already ordered by name
            // take only the files that are relevant for smuggler restore
            _filesToRestore = _filesToRestore
                .Where(BackupUtils.IsBackupFile)
                .OrderBackups()
                .ToList();

            if (_filesToRestore.Count == 0)
                return;

            // we do have at least one smuggler backup, we'll take the indexes from the last file
            databaseRecord.AutoIndexes = new Dictionary<string, AutoIndexDefinition>();
            databaseRecord.Indexes = new Dictionary<string, IndexDefinition>();

            // restore the smuggler backup
            var options = new DatabaseSmugglerOptionsServerSide
            {
                AuthorizationStatus = AuthorizationStatus.DatabaseAdmin,
                OperateOnTypes = ~(DatabaseItemType.CompareExchange | DatabaseItemType.Identities)
            };

            options.OperateOnTypes |= DatabaseItemType.LegacyDocumentDeletions;
            options.OperateOnTypes |= DatabaseItemType.LegacyAttachments;
            options.OperateOnTypes |= DatabaseItemType.LegacyAttachmentDeletions;

            var oldOperateOnTypes = DatabaseSmuggler.ConfigureOptionsForIncrementalImport(options);
            var destination = new DatabaseDestination(database);
            for (var i = 0; i < _filesToRestore.Count - 1; i++)
            {
                result.AddInfo($"Restoring file {(i + 1):#,#;;0}/{_filesToRestore.Count:#,#;;0}");
                onProgress.Invoke(result.Progress);

                var filePath = Path.Combine(backupDirectory, _filesToRestore[i]);
                ImportSingleBackupFile(database, onProgress, result, filePath, context, destination, options,
                    onDatabaseRecordAction: smugglerDatabaseRecord =>
                    {
                        // need to enable revisions before import
                        database.DocumentsStorage.RevisionsStorage.InitializeFromDatabaseRecord(smugglerDatabaseRecord);
                    });
            }

            options.OperateOnTypes = oldOperateOnTypes;
            var lastFilePath = Path.Combine(backupDirectory, _filesToRestore.Last());

            result.AddInfo($"Restoring file {_filesToRestore.Count:#,#;;0}/{_filesToRestore.Count:#,#;;0}");

            onProgress.Invoke(result.Progress);

            ImportSingleBackupFile(database, onProgress, result, lastFilePath, context, destination, options,
                onIndexAction: indexAndType =>
                {
                    if (_restoreConfiguration.SkipIndexes)
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
                    // need to enable revisions before import
                    database.DocumentsStorage.RevisionsStorage.InitializeFromDatabaseRecord(smugglerDatabaseRecord);

                    databaseRecord.Revisions = smugglerDatabaseRecord.Revisions;
                    databaseRecord.Expiration = smugglerDatabaseRecord.Expiration;
                    databaseRecord.RavenConnectionStrings = smugglerDatabaseRecord.RavenConnectionStrings;
                    databaseRecord.SqlConnectionStrings = smugglerDatabaseRecord.SqlConnectionStrings;
                    databaseRecord.Client = smugglerDatabaseRecord.Client;
                });
        }

        private void ImportSingleBackupFile(DocumentDatabase database,
            Action<IOperationProgress> onProgress, RestoreResult restoreResult,
            string filePath, DocumentsOperationContext context,
            DatabaseDestination destination, DatabaseSmugglerOptionsServerSide options,
            Action<IndexDefinitionAndType> onIndexAction = null,
            Action<DatabaseRecord> onDatabaseRecordAction = null)
        {
            using (var fileStream = File.Open(filePath, FileMode.Open))
            using (var stream = new GZipStream(new BufferedStream(fileStream, 128 * Voron.Global.Constants.Size.Kilobyte), CompressionMode.Decompress))
            using (var source = new StreamSource(stream, context, database))
            {
                var smuggler = new Smuggler.Documents.DatabaseSmuggler(database, source, destination,
                    database.Time, options, result: restoreResult, onProgress: onProgress, token: _operationCancelToken.Token)
                {
                    OnIndexAction = onIndexAction,
                    OnDatabaseRecordAction = onDatabaseRecordAction
                };

                smuggler.Execute(ensureStepsProcessed: false);
            }
        }

        private RestoreSettings SnapshotRestore(
            string backupPath,
            string dataDirectory,
            Action<IOperationProgress> onProgress,
            RestoreResult restoreResult)
        {
            Debug.Assert(onProgress != null);

            RestoreSettings restoreSettings = null;

            var voronBackupPath = new VoronPathSetting(backupPath);
            var voronDataDirectory = new VoronPathSetting(dataDirectory);

            using (var zip = ZipFile.Open(voronBackupPath.FullPath, ZipArchiveMode.Read, System.Text.Encoding.UTF8))
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
                                using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                                {
                                    var json = context.Read(entryStream, "read database settings for restore");
                                    json.BlittableValidation();

                                    restoreSettings = JsonDeserializationServer.RestoreSettings(json);

                                    restoreSettings.DatabaseRecord.DatabaseName = _restoreConfiguration.DatabaseName;
                                    DatabaseHelper.Validate(_restoreConfiguration.DatabaseName, restoreSettings.DatabaseRecord, _serverStore.Configuration);

                                    if (restoreSettings.DatabaseRecord.Encrypted && _hasEncryptionKey == false)
                                        throw new ArgumentException("Database snapshot is encrypted but the encryption key is missing!");

                                    if (restoreSettings.DatabaseRecord.Encrypted == false && _hasEncryptionKey)
                                        throw new ArgumentException("Cannot encrypt a non encrypted snapshot backup during restore!");
                                }
                            }
                        }

                        continue;
                    }

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

        private static bool HasFilesOrDirectories(string location)
        {
            if (Directory.Exists(location) == false)
                return false;

            return Directory.GetFiles(location).Length > 0 ||
                   Directory.GetDirectories(location).Length > 0;
        }
    }
}
