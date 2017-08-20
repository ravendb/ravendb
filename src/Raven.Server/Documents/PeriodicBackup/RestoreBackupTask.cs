using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.PeriodicBackup;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Indexes;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Utils;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Logging;
using Voron.Impl.Backup;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class RestoreBackupTask
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<RestoreBackupTask>("RestoreBackupTask");

        private readonly ServerStore _serverStore;
        private readonly RestoreBackupConfiguration _restoreConfiguration;
        private readonly JsonOperationContext _context;
        private readonly string _nodeTag;
        private readonly CancellationToken _cancellationToken;
        private List<string> _filesToRestore;
        private bool _hasEncryptionKey;

        public RestoreBackupTask(ServerStore serverStore, 
            RestoreBackupConfiguration restoreConfiguration, 
            JsonOperationContext context, 
            string nodeTag, 
            CancellationToken cancellationToken)
        {
            _serverStore = serverStore;
            _restoreConfiguration = restoreConfiguration;
            _context = context;
            _nodeTag = nodeTag;
            _cancellationToken = cancellationToken;

            ValidateArguments();
        }

        public async Task<IOperationResult> Execute(Action<IOperationProgress> onProgress)
        {
            try
            {
                if (onProgress == null)
                    onProgress = _ => { };

                var restoreResult = new RestoreResult
                {
                    DataDirectory = _restoreConfiguration.DataDirectory,
                    JournalStoragePath = _restoreConfiguration.JournalsStoragePath
                };

                RestoreSettings restoreSettings = null;
                var firstFile = _filesToRestore[0];
                var extension = Path.GetExtension(firstFile);
                var snapshotRestore = false;
                if (extension == Constants.Documents.PeriodicBackup.SnapshotExtension)
                {
                    // restore the snapshot
                    restoreSettings = SnapshotRestore(firstFile, 
                        _restoreConfiguration.DataDirectory, 
                        _restoreConfiguration.JournalsStoragePath, 
                        onProgress, 
                        restoreResult);
                    snapshotRestore = true;
                    // removing the snapshot from the list of files
                    _filesToRestore.RemoveAt(0);
                }

                var databaseName = _restoreConfiguration.DatabaseName;
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

                    DatabaseHelper.Validate(databaseName, restoreSettings.DatabaseRecord);
                }

                var databaseRecord = restoreSettings.DatabaseRecord;

                if (databaseRecord.Settings == null)
                    databaseRecord.Settings = new Dictionary<string, string>();

                databaseRecord.Settings[RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false";
                databaseRecord.Settings[RavenConfiguration.GetKey(x => x.Core.DataDirectory)] = _restoreConfiguration.DataDirectory;
                databaseRecord.Settings[RavenConfiguration.GetKey(x => x.Storage.JournalsStoragePath)] = _restoreConfiguration.JournalsStoragePath;

                if (_hasEncryptionKey)
                {
                    // save the encryption key so we'll be able to access the database
                    _serverStore.PutSecretKey(_restoreConfiguration.EncryptionKey, 
                        databaseName, overwrite: false);
                }

                using (var database = new DocumentDatabase(databaseName,
                    new RavenConfiguration(databaseName, ResourceType.Database)
                    {
                        Core =
                        {
                            DataDirectory = new PathSetting(_restoreConfiguration.DataDirectory),
                            RunInMemory = false
                        },
                        Storage =
                        {
                            JournalsStoragePath = new PathSetting(_restoreConfiguration.JournalsStoragePath)
                        }
                    }, _serverStore))
                {
                    // smuggler needs an existing document database to operate
                    var options = InitializeOptions.SkipLoadingDatabaseRecord;
                    if (snapshotRestore)
                        options |= InitializeOptions.GenerateNewDatabaseId;
                    database.Initialize(options);
                    SmugglerRestore(_restoreConfiguration.BackupLocation, database, databaseRecord, onProgress, restoreResult);
                }

                databaseRecord.Topology = new DatabaseTopology();
                // restoring to the current node
                databaseRecord.Topology.Members.Add(_nodeTag);

                // TODO: disribute key in cluster?
                // TODO: _restoreConfiguration.ReplicationFactor ? 
                // TODO: _restoreConfiguration.TopologyMembers ? 

                var (newEtag, _) = await _serverStore.WriteDatabaseRecordAsync(
                    databaseName, databaseRecord, null, restoreSettings.DatabaseValues, isRestore:  true);
                await _serverStore.Cluster.WaitForIndexNotification(newEtag);

                return restoreResult;
            }
            catch (OperationCanceledException)
            {
                // database shutdown
                throw;
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations("Failed to restore database", e);

                var alert = AlertRaised.Create(
                    "Failed to restore database",
                    $"Could not restore database named {_restoreConfiguration.DatabaseName}",
                    AlertType.RestoreError,
                    NotificationSeverity.Error,
                    details: new ExceptionDetails(e));
                _serverStore.NotificationCenter.Add(alert);

                // delete any files that we already created during the restore
                IOExtensions.DeleteDirectory(_restoreConfiguration.DataDirectory);
                throw;
            }
        }

        private void ValidateArguments()
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

            var hasJournalStoragePath = string.IsNullOrWhiteSpace(_restoreConfiguration.JournalsStoragePath) == false;
            if (hasJournalStoragePath &&
                HasFilesOrDirectories(_restoreConfiguration.JournalsStoragePath))
                throw new ArgumentException("Journals directory must be empty of any files or folders, " +
                                            $"path: {_restoreConfiguration.JournalsStoragePath}");

            if (hasJournalStoragePath == false)
                _restoreConfiguration.JournalsStoragePath = _restoreConfiguration.DataDirectory;

            if (string.IsNullOrWhiteSpace(_restoreConfiguration.IndexingStoragePath) == false &&
                HasFilesOrDirectories(_restoreConfiguration.IndexingStoragePath))
                throw new ArgumentException("Indexes directory must be empty of any files or folders, " +
                                            $"path: {_restoreConfiguration.IndexingStoragePath}");

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
                .Where(file =>
                {
                    var extension = Path.GetExtension(file);
                    return
                        Constants.Documents.PeriodicBackup.IncrementalBackupExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                        Constants.Documents.PeriodicBackup.FullBackupExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                        Constants.Documents.PeriodicBackup.SnapshotExtension.Equals(extension, StringComparison.OrdinalIgnoreCase);
                })
                .OrderBy(x => x);

            if (string.IsNullOrWhiteSpace(_restoreConfiguration.LastFileNameToRestore))
                return orderedFiles.ToList();

            var filesToRestore = new List<string>();
            foreach (var file in orderedFiles)
            {
                filesToRestore.Add(file);
                if (file.Equals(_restoreConfiguration.LastFileNameToRestore, StringComparison.OrdinalIgnoreCase))
                    break;
            }

            return filesToRestore;
        }

        private void SmugglerRestore(
            string backupDirectory, 
            DocumentDatabase database, 
            DatabaseRecord databaseRecord,
            Action<IOperationProgress> onProgress,
            RestoreResult restoreResult)
        {
            Debug.Assert(onProgress != null);

            // the files are already ordered by name
            // take only the files that are relevant for smuggler restore
            _filesToRestore = _filesToRestore
                .Where(file =>
                {
                    var extension = Path.GetExtension(file);
                    return
                        Constants.Documents.PeriodicBackup.IncrementalBackupExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                        Constants.Documents.PeriodicBackup.FullBackupExtension.Equals(extension, StringComparison.OrdinalIgnoreCase);
                })
                .OrderBy(x => x)
                .ToList();

            if (_filesToRestore.Count == 0)
                return;

            // we do have at least one smuggler backup
            databaseRecord.AutoIndexes = new Dictionary<string, AutoIndexDefinition>();
            databaseRecord.Indexes = new Dictionary<string, IndexDefinition>();
            databaseRecord.Identities = new Dictionary<string, long>();

            // restore the smuggler backup
            var options = new DatabaseSmugglerOptions();

            // we import the indexes, transformers and identities from the last file only, 
            // as the previous files can hold indexes, transformers and identities which were deleted and shouldn't be imported
            var oldOperateOnTypes = options.OperateOnTypes;
            options.OperateOnTypes = options.OperateOnTypes &
                                     ~(DatabaseItemType.Indexes |
                                       DatabaseItemType.Identities);

            var destination = new DatabaseDestination(database);
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                for (var i = 0; i < _filesToRestore.Count - 1; i++)
                {
                    var filePath = Path.Combine(backupDirectory, _filesToRestore[i]);
                    ImportSingleBackupFile(database, onProgress, restoreResult, filePath, context, destination, options);
                }

                options.OperateOnTypes = oldOperateOnTypes;
                var lastFilePath = Path.Combine(backupDirectory, _filesToRestore.Last());

                ImportSingleBackupFile(database, onProgress, restoreResult, lastFilePath, context, destination, options,
                    onIndexAction: indexAndType =>
                    {
                        switch (indexAndType.Type)
                        {
                            case IndexType.AutoMap:
                            case IndexType.AutoMapReduce:
                                var autoIndexDefinition = (IndexDefinitionBase)indexAndType.IndexDefinition;
                                databaseRecord.AutoIndexes[autoIndexDefinition.Name] =
                                    PutAutoIndexCommand.GetAutoIndexDefinition(autoIndexDefinition, indexAndType.Type);
                                break;
                            case IndexType.Map:
                            case IndexType.MapReduce:
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
                    onIdentityAction: keyValuePair => databaseRecord.Identities[keyValuePair.Key] = keyValuePair.Value);
            }
        }

        private void ImportSingleBackupFile(DocumentDatabase database, 
            Action<IOperationProgress> onProgress, RestoreResult restoreResult, 
            string filePath, DocumentsOperationContext context,
            DatabaseDestination destination, DatabaseSmugglerOptions options,
            Action<IndexDefinitionAndType> onIndexAction = null,
            Action<KeyValuePair<string, long>> onIdentityAction = null)
        {
            using (var fileStream = File.Open(filePath, FileMode.Open))
            using (var stream = new GZipStream(new BufferedStream(fileStream, 128 * Voron.Global.Constants.Size.Kilobyte), CompressionMode.Decompress))
            {
                var source = new StreamSource(stream, context);
                var smuggler = new Smuggler.Documents.DatabaseSmuggler(source, destination,
                    database.Time, options, result: restoreResult, onProgress: onProgress, token: _cancellationToken)
                {
                    OnIndexAction = onIndexAction,
                    OnIdentityAction = onIdentityAction
                };

                smuggler.Execute();
            }
        }

        private RestoreSettings SnapshotRestore(
            string backupPath,
            string dataDirectory,
            string journalStoragePath,
            Action<IOperationProgress> onProgress,
            RestoreResult restoreResult)
        {
            Debug.Assert(onProgress != null);

            RestoreSettings restoreSettings = null;

            BackupMethods.Full.Restore(
                backupPath,
                dataDirectory,
                journalStoragePath,
                settingsKey: RestoreSettings.FileName,
                onSettings: settingsStream =>
                {
                    //TODO: decrypt this file using the _restoreConfiguration.EncryptionKey
                    //http://issues.hibernatingrhinos.com/issue/RavenDB-7546

                    var json = _context.Read(settingsStream, "read database settings for restore");
                    restoreSettings = JsonDeserializationServer.RestoreSettings(json);

                    restoreSettings.DatabaseRecord.DatabaseName = _restoreConfiguration.DatabaseName;
                    DatabaseHelper.Validate(_restoreConfiguration.DatabaseName, restoreSettings.DatabaseRecord);

                    if (restoreSettings.DatabaseRecord.Encrypted && _hasEncryptionKey == false)
                        throw new ArgumentException("Database snapshot is encrypted but the encryption key is missing!");

                    if (restoreSettings.DatabaseRecord.Encrypted == false && _hasEncryptionKey)
                        throw new ArgumentException("Cannot encrypt a non encrypted snapshot backup during restore!");
                },
                onProgress: message =>
                {
                    restoreResult.AddInfo(message);
                    restoreResult.RestoredFilesInSnapshotCount++;
                    onProgress.Invoke(restoreResult.Progress);
                },
                cancellationToken: _cancellationToken);

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
