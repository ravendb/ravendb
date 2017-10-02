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
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.PeriodicBackup;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Utils;
using Raven.Server.Web.System;
using Sparrow.Logging;
using Voron.Impl.Backup;
using DatabaseSmuggler = Raven.Client.Documents.Smuggler.DatabaseSmuggler;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class RestoreBackupTask
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<RestoreBackupTask>("RestoreBackupTask");

        private readonly ServerStore _serverStore;
        private readonly RestoreBackupConfiguration _restoreConfiguration;
        private readonly string _nodeTag;
        private readonly OperationCancelToken _operationCancelToken;
        private List<string> _filesToRestore;
        private bool _hasEncryptionKey;

        public RestoreBackupTask(ServerStore serverStore,
            RestoreBackupConfiguration restoreConfiguration,
            string nodeTag,
            OperationCancelToken operationCancelToken)
        {
            _serverStore = serverStore;
            _restoreConfiguration = restoreConfiguration;
            _nodeTag = nodeTag;
            _operationCancelToken = operationCancelToken;

            ValidateArguments();
        }

        public async Task<IOperationResult> Execute(Action<IOperationProgress> onProgress)
        {
            try
            {
                if (onProgress == null)
                    onProgress = _ => { };

                var result = new RestoreResult
                {
                    DataDirectory = _restoreConfiguration.DataDirectory
                };

                onProgress.Invoke(result.Progress);

                Stopwatch sw = null;
                RestoreSettings restoreSettings = null;
                var firstFile = _filesToRestore[0];
                var extension = Path.GetExtension(firstFile);
                var snapshotRestore = false;
                if (extension == Constants.Documents.PeriodicBackup.SnapshotExtension)
                {
                    snapshotRestore = true;
                    sw = Stopwatch.StartNew();
                    // restore the snapshot
                    restoreSettings = SnapshotRestore(firstFile,
                        _restoreConfiguration.DataDirectory,
                        onProgress,
                        result);
                    
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
                        },
                        Identities = new Dictionary<string, long>()
                    };

                    DatabaseHelper.Validate(databaseName, restoreSettings.DatabaseRecord);
                }

                var databaseRecord = restoreSettings.DatabaseRecord;
                if (databaseRecord.Settings == null)
                    databaseRecord.Settings = new Dictionary<string, string>();

                databaseRecord.Settings[RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false";
                databaseRecord.Settings[RavenConfiguration.GetKey(x => x.Core.DataDirectory)] = _restoreConfiguration.DataDirectory;

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
                        }
                    }, _serverStore))
                {
                    // smuggler needs an existing document database to operate
                    var options = InitializeOptions.SkipLoadingDatabaseRecord;
                    if (snapshotRestore)
                        options |= InitializeOptions.GenerateNewDatabaseId;

                    database.Initialize(options);

                    if (snapshotRestore)
                    {
                        var summary = database.GetDatabaseSummary();
                        result.Documents.ReadCount += summary.DocumentsCount;
                        result.Documents.Attachments.ReadCount += summary.AttachmentsCount;
                        result.RevisionDocuments.ReadCount += summary.RevisionsCount;
                        result.Indexes.ReadCount += summary.IndexesCount;
                        result.Identities.ReadCount += summary.IdentitiesCount;
                        result.AddInfo($"Successfully restored {result.RestoredFilesInSnapshotCount} " +
                                       $"files during snapshot restore, took: {sw.ElapsedMilliseconds:#,#;;0}ms");
                        onProgress.Invoke(result.Progress);
                    }

                    SmugglerRestore(_restoreConfiguration.BackupLocation, database, databaseRecord, restoreSettings, onProgress, result);
                }

                result.Documents.Processed = true;
                result.RevisionDocuments.Processed = true;
                result.Indexes.Processed = true;
                result.Identities.Processed = true;
                onProgress.Invoke(result.Progress);

                databaseRecord.Topology = new DatabaseTopology();
                // restoring to the current node only
                databaseRecord.Topology.Members.Add(_nodeTag);

                await _serverStore.WriteDatabaseRecordAsync(databaseName, databaseRecord, null, restoreSettings.DatabaseValues, isRestore: true);
                var index = await WriteIdentitiesAsync(databaseName, restoreSettings.Identities);
                await _serverStore.Cluster.WaitForIndexNotification(index);

                return result;
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
            finally
            {
                _operationCancelToken.Dispose();
            }
        }

        private async Task<long> WriteIdentitiesAsync(string databaseName, Dictionary<string, long> identities)
        {
            const int batchSize = 1024;

            if (identities.Count <= batchSize)
                return await SendIdentities(identities);

            long index = 0;
            var identitiesToSend = new Dictionary<string, long>();
            foreach (var identity in identities)
            {
                identitiesToSend[identity.Key] = identity.Value;

                if (identitiesToSend.Count < batchSize)
                    continue;

                index = await SendIdentities(identitiesToSend);
            }

            if (identitiesToSend.Count > 0)
                index = await SendIdentities(identitiesToSend);

            return index;

            async Task<long> SendIdentities(Dictionary<string, long> toSend)
            {
                var result = await _serverStore.SendToLeaderAsync(new UpdateClusterIdentityCommand(databaseName, toSend));

                toSend.Clear();

                return result.Index;
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
                var fileName = Path.GetFileName(file);
                if (fileName.Equals(_restoreConfiguration.LastFileNameToRestore, StringComparison.OrdinalIgnoreCase))
                    break;
            }

            return filesToRestore;
        }

        private void SmugglerRestore(
            string backupDirectory,
            DocumentDatabase database,
            DatabaseRecord databaseRecord,
            RestoreSettings restoreSettings,
            Action<IOperationProgress> onProgress,
            RestoreResult result)
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
            restoreSettings.Identities = new Dictionary<string, long>();

            // restore the smuggler backup
            var options = new DatabaseSmugglerOptionsServerSide();
            var oldOperateOnTypes = DatabaseSmuggler.ConfigureOptionsForIncrementalImport(options);

            var destination = new DatabaseDestination(database);
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                for (var i = 0; i < _filesToRestore.Count - 1; i++)
                {
                    var filePath = Path.Combine(backupDirectory, _filesToRestore[i]);
                    ImportSingleBackupFile(database, onProgress, result, filePath, context, destination, options);
                }

                options.OperateOnTypes = oldOperateOnTypes;
                var lastFilePath = Path.Combine(backupDirectory, _filesToRestore.Last());

                ImportSingleBackupFile(database, onProgress, result, lastFilePath, context, destination, options,
                    onIndexAction: indexAndType =>
                    {
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
                    onIdentityAction: identity => restoreSettings.Identities[identity.Prefix] = identity.Value);
            }
        }

        private void ImportSingleBackupFile(DocumentDatabase database,
            Action<IOperationProgress> onProgress, RestoreResult restoreResult,
            string filePath, DocumentsOperationContext context,
            DatabaseDestination destination, DatabaseSmugglerOptionsServerSide options,
            Action<IndexDefinitionAndType> onIndexAction = null,
            Action<(string Prefix, long Value)> onIdentityAction = null)
        {
            using (var fileStream = File.Open(filePath, FileMode.Open))
            using (var stream = new GZipStream(new BufferedStream(fileStream, 128 * Voron.Global.Constants.Size.Kilobyte), CompressionMode.Decompress))
            using (var source = new StreamSource(stream, context))
            {
                var smuggler = new Smuggler.Documents.DatabaseSmuggler(database, source, destination,
                    database.Time, options, result: restoreResult, onProgress: onProgress, token: _operationCancelToken.Token)
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
            Action<IOperationProgress> onProgress,
            RestoreResult restoreResult)
        {
            Debug.Assert(onProgress != null);

            RestoreSettings restoreSettings = null;

            BackupMethods.Full.Restore(
                backupPath,
                dataDirectory,
                journalDir: null,
                settingsKey: RestoreSettings.FileName,
                onSettings: settingsStream =>
                {
                    //TODO: decrypt this file using the _restoreConfiguration.EncryptionKey
                    //http://issues.hibernatingrhinos.com/issue/RavenDB-7546

                    using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    {
                        var json = context.Read(settingsStream, "read database settings for restore");
                        json.BlittableValidation();

                        restoreSettings = JsonDeserializationServer.RestoreSettings(json);

                        restoreSettings.DatabaseRecord.DatabaseName = _restoreConfiguration.DatabaseName;
                        DatabaseHelper.Validate(_restoreConfiguration.DatabaseName, restoreSettings.DatabaseRecord);

                        if (restoreSettings.DatabaseRecord.Encrypted && _hasEncryptionKey == false)
                            throw new ArgumentException("Database snapshot is encrypted but the encryption key is missing!");

                        if (restoreSettings.DatabaseRecord.Encrypted == false && _hasEncryptionKey)
                            throw new ArgumentException("Cannot encrypt a non encrypted snapshot backup during restore!");
                    }
                },
                onProgress: message =>
                {
                    restoreResult.AddInfo(message);
                    restoreResult.RestoredFilesInSnapshotCount++;
                    onProgress.Invoke(restoreResult.Progress);
                },
                cancellationToken: _operationCancelToken.Token);

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
