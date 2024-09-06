using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Indexes.Auto;
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
using BackupUtils = Raven.Client.Documents.Smuggler.BackupUtils;
using RavenServerBackupUtils = Raven.Server.Utils.BackupUtils;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public abstract class AbstractRestoreBackupTask : IDisposable
    {
        protected RestoreBackupConfigurationBase RestoreConfiguration { get; set; }
        protected ServerStore ServerStore;
        protected RestoreResult Result;
        protected Action<IOperationProgress> Progress;
        protected List<string> FilesToRestore;
        protected RestoreSettings RestoreSettings;
        protected readonly OperationCancelToken OperationCancelToken;
        protected readonly IRestoreSource RestoreSource;
        protected bool HasEncryptionKey;
        protected DocumentDatabase Database;
        protected readonly JsonOperationContext Context;

        protected string DatabaseName => RestoreConfiguration.DatabaseName;
        protected static readonly Logger Logger = LoggingSource.Instance.GetLogger<AbstractRestoreBackupTask>("Server");
        protected bool DatabaseValidation = true;
        protected bool DeleteDatabaseOnFailure = true;
        protected InitializeOptions Options = InitializeOptions.SkipLoadingDatabaseRecord;

        private bool _restoringToDefaultDataDirectory;
        private readonly IDisposable _disposeContext;

        protected AbstractRestoreBackupTask(ServerStore serverStore,
            RestoreBackupConfigurationBase restoreConfiguration,
            IRestoreSource restoreSource,
            List<string> filesToRestore,
            OperationCancelToken operationCancelToken)
        {
            ServerStore = serverStore;
            RestoreConfiguration = restoreConfiguration;
            RestoreSource = restoreSource;
            FilesToRestore = filesToRestore;
            OperationCancelToken = operationCancelToken;
            _disposeContext = ServerStore.ContextPool.AllocateOperationContext(out Context);
        }

        public async Task<IOperationResult> ExecuteAsync(Action<IOperationProgress> onProgress)
        {
            Result = new RestoreResult
            {
                DataDirectory = RestoreConfiguration.DataDirectory
            };

            Progress = onProgress;

            try
            {
                using (this)
                {
                    await InitializeAsync();

                    if (HasEncryptionKey)
                    {
                        // save the encryption key so we'll be able to access the database
                        ServerStore.PutSecretKey(RestoreConfiguration.EncryptionKey, RestoreSettings.DatabaseRecord.DatabaseName, overwrite: false);
                    }

                    using (await OnBeforeRestoreAsync())
                    {
                        await RestoreAsync();
                        await OnAfterRestoreAsync();
                    }

                    await OnAfterRestoreBeforeReturnAsync();

                    return OperationResult();
                }
            }
            catch (Exception e)
            {
                await OnErrorAsync(Progress, e);
                throw;
            }
        }

        protected abstract Task RestoreAsync();

        protected virtual async Task InitializeAsync()
        {
            var dataDirectoryThatWillBeUsed = string.IsNullOrWhiteSpace(RestoreConfiguration.DataDirectory) ?
                           ServerStore.Configuration.Core.DataDirectory.FullPath :
                           new PathSetting(RestoreConfiguration.DataDirectory, ServerStore.Configuration.Core.DataDirectory.FullPath).FullPath;

            if (DatabaseValidation && ResourceNameValidator.IsValidResourceName(RestoreConfiguration.DatabaseName, dataDirectoryThatWillBeUsed, out string errorMessage) == false)
                throw new InvalidOperationException(errorMessage);

            await ServerStore.EnsureNotPassiveAsync();

            ClusterTopology clusterTopology = GetClusterTopology();

            if (string.IsNullOrWhiteSpace(RestoreConfiguration.EncryptionKey) == false)
            {
                HasEncryptionKey = true;
                var key = Convert.FromBase64String(RestoreConfiguration.EncryptionKey);
                if (key.Length != 256 / 8)
                    throw new InvalidOperationException($"The size of the key must be 256 bits, but was {key.Length * 8} bits.");

                if (AdminDatabasesHandler.NotUsingHttps(clusterTopology.GetUrlFromTag(ServerStore.NodeTag)))
                    throw new InvalidOperationException("Cannot restore an encrypted database to a node which doesn't support SSL!");
            }

            var backupEncryptionSettings = RestoreConfiguration.BackupEncryptionSettings;
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

            var hasRestoreDataDirectory = string.IsNullOrWhiteSpace(RestoreConfiguration.DataDirectory) == false;
            if (hasRestoreDataDirectory &&
                HasFilesOrDirectories(dataDirectoryThatWillBeUsed))
                throw new ArgumentException("New data directory must be empty of any files or folders, " +
                                            $"path: {dataDirectoryThatWillBeUsed}");

            if (hasRestoreDataDirectory == false)
                RestoreConfiguration.DataDirectory = GetDataDirectory();

            _restoringToDefaultDataDirectory = IsDefaultDataDirectory(RestoreConfiguration.DataDirectory, RestoreConfiguration.DatabaseName);
        }

        protected virtual DatabaseRecord GetDatabaseRecord()
        {
            return new DatabaseRecord(DatabaseName)
            {
                // we only have a smuggler restore
                // use the encryption key to encrypt the database
                Encrypted = HasEncryptionKey
            };
        }

        protected virtual IOperationResult OperationResult() => Result;

        protected virtual RavenConfiguration CreateDatabaseConfiguration()
        {
            return ServerStore
                .DatabasesLandlord
                .CreateDatabaseConfiguration(DatabaseName, ignoreDisabledDatabase: true, ignoreBeenDeleted: true, ignoreNotRelevant: true, RestoreSettings.DatabaseRecord);
        }

        protected virtual async Task<IDisposable> OnBeforeRestoreAsync()
        {
            ModifyDatabaseRecordSettings();

            CreateDocumentDatabase();

            var databaseRecord = RestoreSettings.DatabaseRecord;
            databaseRecord.Topology = new DatabaseTopology();

            // restoring to the current node only
            databaseRecord.Topology.Members.Add(ServerStore.NodeTag);

            // we are currently restoring, shouldn't try to access it
            databaseRecord.DatabaseState = DatabaseStateStatus.RestoreInProgress;

            await SaveDatabaseRecordAsync(DatabaseName, databaseRecord, RestoreSettings.DatabaseValues, Result, Progress);

            ServerStore.ForTestingPurposes?.RestoreDatabaseAfterSavingDatabaseRecord?.Invoke();

            Database.SetIds(RestoreSettings.DatabaseRecord);

            return Database;
        }

        protected virtual Task OnAfterRestoreBeforeReturnAsync()
        {
            return Task.CompletedTask;
        }

        protected virtual async Task OnAfterRestoreAsync()
        {
            Result.Files.CurrentFileName = null;

            DisableOngoingTasksIfNeeded(RestoreSettings.DatabaseRecord);
            SmugglerBase.EnsureProcessed(Result, skipped: false, indexesSkipped: Result.Indexes.Skipped);
            Progress.Invoke(Result.Progress);

            // after the db for restore is done, we can safely set the db state to normal and write the DatabaseRecord
            RestoreSettings.DatabaseRecord.DatabaseState = DatabaseStateStatus.Normal;

            await SaveDatabaseRecordAsync(DatabaseName, RestoreSettings.DatabaseRecord, null, Result, Progress);
        }

        protected void CreateDocumentDatabase()
        {
            var configuration = CreateDatabaseConfiguration();
            var addToInitLog = new Action<LogMode, string>((logMode, txt) => // init log is not save in mem during RestoreBackup
            {
                var msg = $"[RestoreBackup] {DateTime.UtcNow} :: Database '{DatabaseName}' : {txt}";

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

            Database = DatabasesLandlord.CreateDocumentDatabase(DatabaseName, configuration, ServerStore, addToInitLog);
            Database.Initialize(Options);
        }

        protected async Task<long> SaveDatabaseRecordAsync(string databaseName, DatabaseRecord databaseRecord, Dictionary<string,
                    BlittableJsonReaderObject> databaseValues, SmugglerResult restoreResult, Action<IOperationProgress> onProgress)
        {
            // at this point we restored a large portion of the database or all of it	
            // we'll retry saving the database record since a failure here will cause us to abort the entire restore operation	

            var index = await BackupHelper.RunWithRetriesAsync(maxRetries: 10, async () =>
                {
                    var result = await ServerStore.WriteDatabaseRecordAsync(
                        databaseName, databaseRecord, null, RaftIdGenerator.NewId(), databaseValues, isRestore: true);
                    return result.Index;
                },
                "Saving the database record",
                "Failed to save the database record, the restore is aborted",
                restoreResult, onProgress, OperationCancelToken);

            return await BackupHelper.RunWithRetriesAsync(maxRetries: 10, async () =>
                {
                    await ServerStore.Cluster.WaitForIndexNotification(index, TimeSpan.FromSeconds(30));
                    return index;
                },
                $"Verifying that the change to the database record propagated to node {ServerStore.NodeTag}",
                $"Failed to verify that the change to the database record was propagated to node {ServerStore.NodeTag}, the restore is aborted",
                restoreResult, onProgress, OperationCancelToken);
        }

        protected void CreateRestoreSettings()
        {
            RestoreSettings = new RestoreSettings
            {
                DatabaseRecord = GetDatabaseRecord()
            };

            if (DatabaseValidation)
                DatabaseHelper.Validate(DatabaseName, RestoreSettings.DatabaseRecord, ServerStore.Configuration);
        }

        protected void ModifyDatabaseRecordSettings()
        {
            var databaseRecord = RestoreSettings.DatabaseRecord;
            databaseRecord.Settings ??= new Dictionary<string, string>();

            var runInMemoryConfigurationKey = RavenConfiguration.GetKey(x => x.Core.RunInMemory);
            databaseRecord.Settings.Remove(runInMemoryConfigurationKey);
            if (ServerStore.Configuration.Core.RunInMemory)
                databaseRecord.Settings[runInMemoryConfigurationKey] = "false";

            var dataDirectoryConfigurationKey = RavenConfiguration.GetKey(x => x.Core.DataDirectory);
            databaseRecord.Settings.Remove(dataDirectoryConfigurationKey); // removing because we want to restore to given location, not to serialized in backup one
            if (_restoringToDefaultDataDirectory == false)
                databaseRecord.Settings[dataDirectoryConfigurationKey] = RestoreConfiguration.DataDirectory;
        }

        protected async Task SmugglerRestoreAsync(DocumentDatabase database, JsonOperationContext context, DatabaseDestination lastFileDestination)
        {
            Debug.Assert(Progress != null);

            // the files are already ordered by name
            // take only the files that are relevant for smuggler restore

            if (FilesToRestore.Count == 0)
                return;

            // restore the smuggler backup
            var options = new DatabaseSmugglerOptionsServerSide(AuthorizationStatus.DatabaseAdmin)
            {
                SkipRevisionCreation = true,
                IncludeArchived = true
            };

            options.OperateOnTypes |= DatabaseItemType.LegacyDocumentDeletions;
            options.OperateOnTypes |= DatabaseItemType.LegacyAttachments;
            options.OperateOnTypes |= DatabaseItemType.LegacyAttachmentDeletions;

            var oldOperateOnTypes = Client.Documents.Smuggler.DatabaseSmuggler.ConfigureOptionsForIncrementalImport(options);
            var destination = database.Smuggler.CreateDestination(OperationCancelToken.Token);

            for (var i = 0; i < FilesToRestore.Count - 1; i++)
            {
                Result.AddInfo($"Restoring file {(i + 1):#,#;;0}/{FilesToRestore.Count:#,#;;0}");

                var fileName = FilesToRestore[i];
                Result.Files.CurrentFileName = fileName;
                Result.Files.CurrentFile++;

                Progress.Invoke(Result.Progress);

                var filePath = RestoreSource.GetBackupPath(fileName);
                await ImportSingleBackupFileAsync(database, Progress, Result, filePath, context, destination, options, isLastFile: false);
            }

            options.OperateOnTypes = oldOperateOnTypes;

            Result.AddInfo($"Restoring file {FilesToRestore.Count:#,#;;0}/{FilesToRestore.Count:#,#;;0}");

            var lastFileName = FilesToRestore.Last();
            Result.Files.CurrentFileName = lastFileName;
            Result.Files.CurrentFile++;

            Result.Indexes.Skipped = RestoreConfiguration.SkipIndexes;
            Progress.Invoke(Result.Progress);

            var lastFilePath = RestoreSource.GetBackupPath(lastFileName);

            await ImportSingleBackupFileAsync(database, Progress, Result, lastFilePath, context, lastFileDestination, options, isLastFile: true);
        }

        protected async Task<Stream> GetInputStreamAsync(Stream stream, byte[] databaseEncryptionKey)
        {
            if (RestoreConfiguration.BackupEncryptionSettings == null ||
                RestoreConfiguration.BackupEncryptionSettings.EncryptionMode == EncryptionMode.None)
                return stream;

            byte[] encryptionKey;

            if (RestoreConfiguration.BackupEncryptionSettings.EncryptionMode == EncryptionMode.UseDatabaseKey)
            {
                if (databaseEncryptionKey == null)
                    throw new ArgumentException("Stream is encrypted but the encryption key is missing!");

                encryptionKey = databaseEncryptionKey;
            }
            else
            {
                encryptionKey = Convert.FromBase64String(RestoreConfiguration.BackupEncryptionSettings.Key);
            }

            var decryptingStream = new DecryptingXChaCha20Oly1305Stream(stream, encryptionKey);

            await decryptingStream.InitializeAsync();

            return decryptingStream;
        }

        protected void DisableOngoingTasksIfNeeded(DatabaseRecord databaseRecord)
        {
            if (RestoreConfiguration.DisableOngoingTasks == false)
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

            if (databaseRecord.ElasticSearchEtls != null)
            {
                foreach (var task in databaseRecord.ElasticSearchEtls)
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

            if (databaseRecord.QueueSinks != null)
            {
                foreach (var task in databaseRecord.QueueSinks)
                {
                    task.Disabled = true;
                }
            }
        }

        private ClusterTopology GetClusterTopology()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                if (DatabaseValidation && ServerStore.Cluster.DatabaseExists(context, RestoreConfiguration.DatabaseName))
                    throw new ArgumentException($"Cannot restore data to an existing database named {RestoreConfiguration.DatabaseName}");

                return ServerStore.GetClusterTopology(context);
            }
        }

        protected async Task ImportSingleBackupFileAsync(DocumentDatabase database,
            Action<IOperationProgress> onProgress,
            RestoreResult restoreResult,
            string filePath,
            JsonOperationContext context,
            DatabaseDestination destination,
            DatabaseSmugglerOptionsServerSide options,
            bool isLastFile)
        {
            await using (var fileStream = await RestoreSource.GetStream(filePath))
            await using (var inputStream = await GetInputStreamAsync(fileStream, database.MasterKey))
            await using (var gzipStream = await RavenServerBackupUtils.GetDecompressionStreamAsync(inputStream))
            using (var source = new StreamSource(gzipStream, context, database.Name, options))
            {
                var smuggler = database.Smuggler.CreateForRestore(RestoreSettings.DatabaseRecord, source, destination, context, options, restoreResult, onProgress, OperationCancelToken.Token);

                ConfigureSettingsForSmugglerRestore(database, smuggler, filePath, isLastFile);

                await smuggler.ExecuteAsync(ensureStepsProcessed: false, isLastFile);
            }
        }

        protected virtual void ConfigureSettingsForSmugglerRestore(DocumentDatabase database, SmugglerBase smuggler, string filePath, bool isLastFile)
        {
            smuggler.BackupKind = BackupUtils.IsFullBackup(Path.GetExtension(filePath)) ? BackupKind.Full : BackupKind.Incremental;

            if (isLastFile)
            {
                var databaseRecord = RestoreSettings.DatabaseRecord;

                smuggler.OnIndexAction = indexAndType =>
                {
                    if (RestoreConfiguration.SkipIndexes)
                        return;

                    databaseRecord.AutoIndexes ??= new Dictionary<string, AutoIndexDefinition>();
                    databaseRecord.Indexes ??= new Dictionary<string, IndexDefinition>();

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
                };

                smuggler.OnDatabaseRecordAction = smugglerDatabaseRecord =>
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
                    databaseRecord.DataArchival = smugglerDatabaseRecord.DataArchival;
                    databaseRecord.QueueSinks = smugglerDatabaseRecord.QueueSinks;
                    databaseRecord.SupportedFeatures = smugglerDatabaseRecord.SupportedFeatures;
                };
            }

            smuggler.OnDatabaseRecordAction += smugglerDatabaseRecord =>
            {
                // need to enable revisions before import
                database.DocumentsStorage.RevisionsStorage.InitializeFromDatabaseRecord(smugglerDatabaseRecord);
                
                database.SupportedFeatures = new SupportedFeature(smugglerDatabaseRecord);
            };
        }

        private async Task OnErrorAsync(Action<IOperationProgress> onProgress, Exception e)
        {
            if (Logger.IsOperationsEnabled)
                Logger.Operations("Failed to restore database", e);

            var alert = AlertRaised.Create(
                RestoreConfiguration.DatabaseName,
                "Failed to restore database",
                $"Could not restore database named {RestoreConfiguration.DatabaseName}",
                AlertType.RestoreError,
                NotificationSeverity.Error,
                details: new ExceptionDetails(e));
            ServerStore.NotificationCenter.Add(alert);

            if (DeleteDatabaseOnFailure)
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    bool databaseExists;
                    using (context.OpenReadTransaction())
                    {
                        databaseExists = ServerStore.Cluster.DatabaseExists(context, RestoreConfiguration.DatabaseName);
                    }

                    if (databaseExists == false)
                    {
                        // delete any files that we already created during the restore
                        IOExtensions.DeleteDirectory(RestoreConfiguration.DataDirectory);
                    }
                    else
                    {
                        try
                        {
                            // delete entire sharded database from all nodes
                            var deleteResult = await ServerStore.DeleteDatabaseAsync(RestoreConfiguration.DatabaseName, true, fromNodes: null,
                                RaftIdGenerator.DontCareId);
                            await ServerStore.Cluster.WaitForIndexNotification(deleteResult.Index, TimeSpan.FromSeconds(60));
                        }
                        catch (TimeoutException te)
                        {
                            Result.AddError($"Failed to delete the database {DatabaseName} after a failed restore. " +
                                            $"In order to restart the restore process this database needs to be deleted manually. Exception: {te}.");
                            onProgress.Invoke(Result.Progress);
                        }
                    }
                }
            }

            Result.AddError($"Error occurred during restore of database {DatabaseName}. Exception: {e}");
            onProgress.Invoke(Result.Progress);
        }

        private bool IsDefaultDataDirectory(string dataDirectory, string databaseName)
        {
            var defaultDataDirectory = RavenConfiguration.GetDataDirectoryPath(
                ServerStore.Configuration.Core,
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
                    ServerStore.Configuration.Core,
                    RestoreConfiguration.DatabaseName,
                    ResourceType.Database);

            var i = 0;
            while (HasFilesOrDirectories(dataDirectory))
                dataDirectory += $"-{++i}";

            return dataDirectory;
        }

        private static bool HasFilesOrDirectories(string location)
        {
            if (Directory.Exists(location) == false)
                return false;

            return Directory.GetFiles(location).Length > 0 ||
                   Directory.GetDirectories(location).Length > 0;
        }

        public virtual void Dispose()
        {
            RestoreSource?.Dispose();
            OperationCancelToken?.Dispose();
            _disposeContext?.Dispose();
        }
    }
}
